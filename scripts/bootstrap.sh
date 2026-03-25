#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_DIR="$(cd -- "$SCRIPT_DIR/.." >/dev/null 2>&1 && pwd)"

VDDK_DIR=""
VDDK_TAR=""
CONFIG_PATH="$REPO_DIR/config.yaml"
BIN_PATH="/usr/local/bin/v2c-engine"
LISTEN_ADDR=":8000"
UI_LISTEN_ADDR="0.0.0.0:5173"
INSTALL_SERVICE=0
WITH_UI=0
SKIP_BUILD=0
START_SERVICES=0
SERVICE_CONFIG_PATH=""
UI_CONFIG_PATH="/etc/v2c-ui/.env.local"

usage() {
  cat <<'EOF'
Usage: scripts/bootstrap.sh [options]

Options:
  --vddk-dir <path>        Path to VMware VDDK root (contains include/ and lib64/)
  --vddk-tar <path>        Path to VMware VDDK tarball to extract under /opt/vmware-vddk
  --config <path>          Config file path for service (default: ./config.yaml)
  --bin-path <path>        Installed binary path for service (default: /usr/local/bin/v2c-engine)
  --listen <addr>          API listen address for service (default: :8000)
  --ui-listen <addr>       UI listen address for v2c-ui service (default: 0.0.0.0:5173)
  --install-service        Create systemd service(s): v2c-engine (+ v2c-ui if --with-ui)
  --start-services         Enable and start installed service(s) after setup
  --with-ui                Install frontend npm dependencies
  --skip-build             Skip Go build
  -h, --help               Show this help
EOF
}

log() { printf '[bootstrap] %s\n' "$*"; }
warn() { printf '[bootstrap] warning: %s\n' "$*" >&2; }
die() { printf '[bootstrap] error: %s\n' "$*" >&2; exit 1; }

run_root() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1; then
    sudo "$@"
  else
    die "Need root privileges for: $* (install sudo or run as root)"
  fi
}

command_exists() { command -v "$1" >/dev/null 2>&1; }

install_dnf_packages() {
  local base=(gcc make git tar curl ca-certificates golang)
  local tools=(qemu-img qemu-kvm-core virt-v2v libguestfs-tools-c)
  local ui=(nodejs npm)
  run_root dnf -y makecache
  run_root dnf -y install "${base[@]}"
  if ! run_root dnf -y install "${tools[@]}"; then
    warn "Some virtualization packages failed in bulk; retrying individually."
    for p in "${tools[@]}"; do
      run_root dnf -y install "$p" || warn "Package not installed: $p"
    done
  fi
  if [[ "$WITH_UI" -eq 1 ]]; then
    run_root dnf -y install "${ui[@]}" || warn "UI packages nodejs/npm not fully installed."
  fi
}

install_apt_packages() {
  local base=(build-essential git tar curl ca-certificates golang-go)
  local tools=(qemu-utils qemu-system-x86 virt-v2v libguestfs-tools)
  local ui=(nodejs npm)
  run_root apt-get update
  run_root apt-get install -y "${base[@]}"
  if ! run_root apt-get install -y "${tools[@]}"; then
    warn "Some virtualization packages failed in bulk; retrying individually."
    for p in "${tools[@]}"; do
      run_root apt-get install -y "$p" || warn "Package not installed: $p"
    done
  fi
  if [[ "$WITH_UI" -eq 1 ]]; then
    run_root apt-get install -y "${ui[@]}" || warn "UI packages nodejs/npm not fully installed."
  fi
}

detect_and_install_packages() {
  [[ -f /etc/os-release ]] || die "/etc/os-release not found"
  # shellcheck disable=SC1091
  source /etc/os-release
  local os_id="${ID:-}"
  local os_like="${ID_LIKE:-}"
  log "Detected OS: ${os_id:-unknown} (${os_like:-n/a})"

  if command_exists dnf && ([[ "$os_id" == "rhel" ]] || [[ "$os_id" == "rocky" ]] || [[ "$os_id" == "almalinux" ]] || [[ "$os_like" == *"rhel"* ]] || [[ "$os_like" == *"fedora"* ]]); then
    install_dnf_packages
    return
  fi
  if command_exists apt-get && ([[ "$os_id" == "ubuntu" ]] || [[ "$os_id" == "debian" ]] || [[ "$os_like" == *"debian"* ]]); then
    install_apt_packages
    return
  fi
  die "Unsupported distro. Install prerequisites manually (Go + qemu-img + qemu-nbd + virt-v2v + libguestfs + build tools)."
}

extract_vddk_tar() {
  [[ -f "$VDDK_TAR" ]] || die "VDDK tar not found: $VDDK_TAR"
  local target_root="/opt/vmware-vddk"
  log "Extracting VDDK tarball to $target_root"
  run_root mkdir -p "$target_root"
  run_root tar -xf "$VDDK_TAR" -C "$target_root"
}

resolve_vddk_dir() {
  if [[ -n "$VDDK_TAR" ]]; then
    extract_vddk_tar
  fi

  if [[ -n "$VDDK_DIR" ]]; then
    :
  elif [[ -d /opt/vmware-vddk/vmware-vix-disklib-distrib ]]; then
    VDDK_DIR="/opt/vmware-vddk/vmware-vix-disklib-distrib"
  else
    die "VDDK not found. Pass --vddk-dir or --vddk-tar."
  fi

  [[ -f "$VDDK_DIR/include/vixDiskLib.h" ]] || die "Missing header: $VDDK_DIR/include/vixDiskLib.h"
  if ! ls "$VDDK_DIR"/lib64/libvixDiskLib.so* >/dev/null 2>&1; then
    die "Missing library: $VDDK_DIR/lib64/libvixDiskLib.so*"
  fi
}

write_env_profile() {
  local profile_path="/etc/profile.d/v2c-engine.sh"
  local tmp
  tmp="$(mktemp)"
  cat >"$tmp" <<EOF
export VDDK="$VDDK_DIR"
export PATH="/usr/libexec:\$PATH"
export LD_LIBRARY_PATH="$VDDK_DIR/lib64:\${LD_LIBRARY_PATH:-}"
export CGO_ENABLED=1
export CGO_CFLAGS="-I$VDDK_DIR/include"
export CGO_LDFLAGS="-L$VDDK_DIR/lib64 -lvixDiskLib -ldl -lpthread"
EOF
  run_root install -m 0644 "$tmp" "$profile_path"
  rm -f "$tmp"
  log "Wrote environment profile: $profile_path"
}

ensure_config_exists() {
  if [[ -f "$CONFIG_PATH" ]]; then
    return
  fi
  [[ -f "$REPO_DIR/config.example.yaml" ]] || die "config.example.yaml not found"
  cp "$REPO_DIR/config.example.yaml" "$CONFIG_PATH"
  log "Created $CONFIG_PATH from config.example.yaml (edit credentials/IDs before migration)"
}

build_engine() {
  [[ "$SKIP_BUILD" -eq 1 ]] && return
  command_exists go || die "go not found after package installation"
  (
    cd "$REPO_DIR"
    export CGO_ENABLED=1
    export CGO_CFLAGS="-I$VDDK_DIR/include"
    export CGO_LDFLAGS="-L$VDDK_DIR/lib64 -lvixDiskLib -ldl -lpthread"
    export LD_LIBRARY_PATH="$VDDK_DIR/lib64:${LD_LIBRARY_PATH:-}"
    export PATH="/usr/libexec:$PATH"
    go mod tidy
    go build -o v2c-engine ./cmd/v2c-engine
  )
  run_root install -m 0755 "$REPO_DIR/v2c-engine" "$BIN_PATH"
  log "Built binary: $REPO_DIR/v2c-engine"
  log "Installed binary: $BIN_PATH"
}

install_ui_deps() {
  [[ "$WITH_UI" -eq 1 ]] || return
  command_exists npm || die "npm not available; rerun with package install support for nodejs"
  (
    cd "$REPO_DIR/frontend"
    [[ -f .env.local ]] || cp .env.example .env.local
    npm install
  )
  log "Installed frontend dependencies."
}

ensure_ui_config() {
  [[ "$WITH_UI" -eq 1 ]] || return
  run_root mkdir -p /etc/v2c-ui
  local tmp
  tmp="$(mktemp)"
  cat >"$tmp" <<EOF
# UI API endpoint (edit before starting v2c-ui service)
# Example:
# VITE_API_BASE=http://<engine-host>:8000
VITE_API_BASE=http://127.0.0.1:8000
EOF
  if [[ ! -f "$UI_CONFIG_PATH" ]]; then
    run_root install -m 0644 "$tmp" "$UI_CONFIG_PATH"
    log "Created UI env file: $UI_CONFIG_PATH"
  fi
  rm -f "$tmp"
}

install_systemd_service() {
  [[ "$INSTALL_SERVICE" -eq 1 ]] || return
  local engine_unit="/etc/systemd/system/v2c-engine.service"
  local ui_unit="/etc/systemd/system/v2c-ui.service"
  local service_config="/etc/v2c-engine/config.yaml"
  run_root mkdir -p /etc/v2c-engine
  run_root install -m 0640 "$CONFIG_PATH" "$service_config"
  SERVICE_CONFIG_PATH="$service_config"
  log "Copied service config: $CONFIG_PATH -> $service_config"
  run_root mkdir -p /var/lib/vm-migrator
  local tmp
  tmp="$(mktemp)"
  cat >"$tmp" <<EOF
[Unit]
Description=VMware to CloudStack Migrator API (Go)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/var/lib/vm-migrator
Environment=PATH=/usr/libexec:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin
Environment=LD_LIBRARY_PATH=$VDDK_DIR/lib64
EnvironmentFile=-/etc/default/v2c-engine
ExecStart=$BIN_PATH serve --config $service_config --listen $LISTEN_ADDR
Restart=on-failure
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF
  run_root install -m 0644 "$tmp" "$engine_unit"
  rm -f "$tmp"
  if [[ ! -f /etc/default/v2c-engine ]]; then
    local envtmp
    envtmp="$(mktemp)"
    cat >"$envtmp" <<'EOF'
# Optional runtime secret (recommended over plain-text in config.yaml)
# VC_PASSWORD=change-me
EOF
    run_root install -m 0644 "$envtmp" /etc/default/v2c-engine
    rm -f "$envtmp"
  fi

  if [[ "$WITH_UI" -eq 1 ]]; then
    ensure_ui_config
    local ui_host ui_port
    if [[ "$UI_LISTEN_ADDR" == *:* ]]; then
      ui_host="${UI_LISTEN_ADDR%:*}"
      ui_port="${UI_LISTEN_ADDR##*:}"
      [[ -n "$ui_host" ]] || ui_host="0.0.0.0"
    else
      ui_host="0.0.0.0"
      ui_port="$UI_LISTEN_ADDR"
    fi
    tmp="$(mktemp)"
    cat >"$tmp" <<EOF
[Unit]
Description=VMware to CloudStack Migrator UI (Vite Preview)
After=network-online.target v2c-engine.service
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=$REPO_DIR/frontend
Environment=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin
ExecStart=/bin/bash -lc 'set -e; cp -f "$UI_CONFIG_PATH" "$REPO_DIR/frontend/.env.local"; npm run build; npm run preview -- --host $ui_host --port $ui_port'
Restart=on-failure
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF
    run_root install -m 0644 "$tmp" "$ui_unit"
    rm -f "$tmp"
  fi

  run_root systemctl daemon-reload
  if [[ "$START_SERVICES" -eq 1 ]]; then
    run_root systemctl enable --now v2c-engine
    if [[ "$WITH_UI" -eq 1 ]]; then
      run_root systemctl enable --now v2c-ui
      log "Systemd services installed and started: v2c-engine, v2c-ui"
    else
      log "Systemd service installed and started: v2c-engine"
    fi
  else
    run_root systemctl disable --now v2c-engine 2>/dev/null || true
    if [[ "$WITH_UI" -eq 1 ]]; then
      run_root systemctl disable --now v2c-ui 2>/dev/null || true
      log "Systemd services installed (not started): v2c-engine, v2c-ui"
    else
      log "Systemd service installed (not started): v2c-engine"
    fi
  fi
}

print_summary() {
  local display_config="$CONFIG_PATH"
  if [[ "$INSTALL_SERVICE" -eq 1 ]] && [[ -n "$SERVICE_CONFIG_PATH" ]]; then
    display_config="$SERVICE_CONFIG_PATH"
  fi
  cat <<EOF

Bootstrap complete.

Repo:         $REPO_DIR
Config:       $display_config
VDDK:         $VDDK_DIR
Binary:       $BIN_PATH
Service:      $([[ "$INSTALL_SERVICE" -eq 1 ]] && echo "installed (v2c-engine)" || echo "not installed")
Frontend deps:$([[ "$WITH_UI" -eq 1 ]] && echo "installed" || echo "not installed")
UI env:       $([[ "$WITH_UI" -eq 1 ]] && echo "$UI_CONFIG_PATH" || echo "n/a")

Next:
1) Edit config (required before start): $display_config
2) If UI installed, edit UI API endpoint: $UI_CONFIG_PATH
3) If service installed:
   sudo systemctl enable --now v2c-engine$([[ "$WITH_UI" -eq 1 ]] && echo " v2c-ui")
   systemctl status v2c-engine$([[ "$WITH_UI" -eq 1 ]] && echo " v2c-ui")
   journalctl -u v2c-engine -f
4) If service not installed:
   export VC_PASSWORD='your-vcenter-password'
   $BIN_PATH serve --config $CONFIG_PATH --listen $LISTEN_ADDR

EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --vddk-dir)
      VDDK_DIR="${2:-}"; shift 2 ;;
    --vddk-tar)
      VDDK_TAR="${2:-}"; shift 2 ;;
    --config)
      CONFIG_PATH="${2:-}"; shift 2 ;;
    --bin-path)
      BIN_PATH="${2:-}"; shift 2 ;;
    --listen)
      LISTEN_ADDR="${2:-}"; shift 2 ;;
    --ui-listen)
      UI_LISTEN_ADDR="${2:-}"; shift 2 ;;
    --install-service)
      INSTALL_SERVICE=1; shift ;;
    --start-services)
      START_SERVICES=1; shift ;;
    --with-ui)
      WITH_UI=1; shift ;;
    --skip-build)
      SKIP_BUILD=1; shift ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      die "Unknown argument: $1" ;;
  esac
done

[[ -d "$REPO_DIR/cmd/v2c-engine" ]] || die "Run this from inside the vmware-to-cloudstack repo"
if [[ "$CONFIG_PATH" != /* ]]; then
  CONFIG_PATH="$REPO_DIR/$CONFIG_PATH"
fi
if [[ "$START_SERVICES" -eq 1 && "$INSTALL_SERVICE" -eq 0 ]]; then
  die "--start-services requires --install-service"
fi

detect_and_install_packages
resolve_vddk_dir
write_env_profile
ensure_config_exists
build_engine
install_ui_deps
install_systemd_service
print_summary
