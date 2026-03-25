#!/usr/bin/env bash
set -euo pipefail

REMOVE_PACKAGES=0
PURGE_STATE=0
YES=0

usage() {
  cat <<'EOF'
Usage: scripts/uninstall.sh [options]

Removes v2c-engine bootstrap artifacts so you can reinstall cleanly.

Options:
  --purge-state        Remove /var/lib/vm-migrator runtime state/logs/specs
  --remove-packages    Also remove packages installed by bootstrap (aggressive)
  --yes                Skip confirmation prompt
  -h, --help           Show this help
EOF
}

log() { printf '[uninstall] %s\n' "$*"; }
warn() { printf '[uninstall] warning: %s\n' "$*" >&2; }
die() { printf '[uninstall] error: %s\n' "$*" >&2; exit 1; }

run_root() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1; then
    sudo "$@"
  else
    die "Need root privileges for: $*"
  fi
}

confirm() {
  [[ "$YES" -eq 1 ]] && return 0
  cat <<'EOF'
This will remove:
- systemd unit: /etc/systemd/system/v2c-engine.service
- installed binary: /usr/local/bin/v2c-engine
- service config: /etc/v2c-engine/config.yaml (and directory)
- env files: /etc/default/v2c-engine and /etc/profile.d/v2c-engine.sh

Optional flags also remove state and packages.
EOF
  read -r -p "Continue? [y/N]: " ans
  case "${ans,,}" in
    y|yes) ;;
    *) die "Aborted by user." ;;
  esac
}

remove_service() {
  if run_root systemctl list-unit-files | grep -q '^v2c-engine\.service'; then
    run_root systemctl disable --now v2c-engine || true
  else
    warn "v2c-engine.service not registered; skipping stop/disable."
  fi

  run_root rm -f /etc/systemd/system/v2c-engine.service
  run_root systemctl daemon-reload
  run_root systemctl reset-failed || true
  log "Removed systemd unit."
}

remove_files() {
  run_root rm -f /usr/local/bin/v2c-engine
  run_root rm -f /etc/default/v2c-engine
  run_root rm -f /etc/profile.d/v2c-engine.sh
  run_root rm -f /etc/v2c-engine/config.yaml
  run_root rmdir /etc/v2c-engine 2>/dev/null || true
  log "Removed installed binary and configuration files."
}

remove_state() {
  if [[ "$PURGE_STATE" -eq 1 ]]; then
    run_root rm -rf /var/lib/vm-migrator
    log "Removed runtime state: /var/lib/vm-migrator"
  fi
}

remove_packages() {
  [[ "$REMOVE_PACKAGES" -eq 1 ]] || return 0
  if command -v dnf >/dev/null 2>&1; then
    # Keep this list aligned with bootstrap package installation.
    run_root dnf -y remove golang qemu-img qemu-kvm-core virt-v2v libguestfs-tools-c nodejs npm || true
    run_root dnf -y autoremove || true
    log "Attempted package removal via dnf."
    return 0
  fi
  if command -v apt-get >/dev/null 2>&1; then
    run_root apt-get -y remove golang-go qemu-utils qemu-system-x86 virt-v2v libguestfs-tools nodejs npm || true
    run_root apt-get -y autoremove || true
    log "Attempted package removal via apt-get."
    return 0
  fi
  warn "No supported package manager detected for --remove-packages."
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --purge-state)
      PURGE_STATE=1; shift ;;
    --remove-packages)
      REMOVE_PACKAGES=1; shift ;;
    --yes)
      YES=1; shift ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      die "Unknown argument: $1" ;;
  esac
done

confirm
remove_service
remove_files
remove_state
remove_packages

cat <<'EOF'

Uninstall complete.

Fresh reinstall (example):
  sudo ./scripts/bootstrap.sh --vddk-dir /opt/vmware-vddk/vmware-vix-disklib-distrib --install-service --with-ui

EOF

