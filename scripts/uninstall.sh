#!/usr/bin/env bash
set -euo pipefail

PURGE_STATE=0
YES=0

usage() {
  cat <<'EOF'
Usage: scripts/uninstall.sh [options]

Removes v2c-engine bootstrap artifacts so you can reinstall cleanly.

Options:
  --purge-state        Remove /var/lib/vm-migrator runtime state/logs/specs
  --list-packages      Print bootstrap package list and exit
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
- systemd unit: /etc/systemd/system/v2c-ui.service (if present)
- installed binary: /usr/local/bin/v2c-engine
- service config: /etc/v2c-engine/config.yaml (and directory)
- UI env config: /etc/v2c-ui/.env.local (and directory)
- env files: /etc/default/v2c-engine, /etc/v2c-engine/build.env and /etc/profile.d/v2c-engine.sh (legacy)

Optional flag can also remove runtime state.
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
  if run_root systemctl list-unit-files | grep -q '^v2c-ui\.service'; then
    run_root systemctl disable --now v2c-ui || true
  fi

  run_root rm -f /etc/systemd/system/v2c-engine.service
  run_root rm -f /etc/systemd/system/v2c-ui.service
  run_root systemctl daemon-reload
  run_root systemctl reset-failed || true
  log "Removed systemd unit(s)."
}

remove_files() {
  run_root rm -f /usr/local/bin/v2c-engine
  run_root rm -f /etc/default/v2c-engine
  run_root rm -f /etc/v2c-engine/build.env
  run_root rm -f /etc/profile.d/v2c-engine.sh
  run_root rm -f /etc/v2c-engine/config.yaml
  run_root rmdir /etc/v2c-engine 2>/dev/null || true
  run_root rm -f /etc/v2c-ui/.env.local
  run_root rmdir /etc/v2c-ui 2>/dev/null || true
  log "Removed installed binary and configuration files."
}

remove_state() {
  if [[ "$PURGE_STATE" -eq 1 ]]; then
    run_root rm -rf /var/lib/vm-migrator
    log "Removed runtime state: /var/lib/vm-migrator"
  fi
}

print_bootstrap_package_list() {
  cat <<'EOF'
Bootstrap package list (reference only, not auto-removed):

RHEL/Rocky/Alma/Fedora (dnf):
  gcc make git tar curl ca-certificates golang
  qemu-img qemu-kvm-core virt-v2v libguestfs-tools-c
  nodejs npm (only when bootstrap used with --with-ui)

Debian/Ubuntu (apt):
  build-essential git tar curl ca-certificates golang-go
  qemu-utils qemu-system-x86 virt-v2v libguestfs-tools
  nodejs npm (only when bootstrap used with --with-ui)

Suggested manual review commands:
  dnf list --installed | egrep 'golang|qemu|virt-v2v|guestfs|nodejs|npm'
  apt list --installed 2>/dev/null | egrep 'golang|qemu|virt-v2v|guestfs|nodejs|npm'
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --purge-state)
      PURGE_STATE=1; shift ;;
    --list-packages)
      print_bootstrap_package_list
      exit 0 ;;
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

cat <<'EOF'

Uninstall complete.

Note: OS packages are intentionally NOT removed automatically.
Review/remove packages manually if needed:
  ./scripts/uninstall.sh --list-packages

Fresh reinstall (example):
  sudo ./scripts/bootstrap.sh --vddk-dir /opt/vmware-vddk/vmware-vix-disklib-distrib --install-service --with-ui

EOF
