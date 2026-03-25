# Go Engine: VMware -> CloudStack

This repository is Go-first and provides a production migration engine + API/UI services for VMware to CloudStack migrations.

## Prerequisites (Top Priority)

- Linux host
- VMware VDDK installed (must include `include/vixDiskLib.h` and `lib64/libvixDiskLib.so*`)
- Root/sudo access (required for service install and NFS mount operations)
- CloudStack API access
- vCenter credentials

The bootstrap script installs required OS packages (Go, qemu tools, virt-v2v, guestfs, and optional node/npm for UI).

## Bootstrap Script Options (Top Priority)

Use `scripts/bootstrap.sh` to install dependencies, build the engine, and install services.

```bash
chmod +x ./scripts/bootstrap.sh
sudo ./scripts/bootstrap.sh --vddk-dir /opt/vmware-vddk/vmware-vix-disklib-distrib --install-service --with-ui
```

If you have a VDDK tarball:

```bash
sudo ./scripts/bootstrap.sh --vddk-tar /tmp/VMware-vix-disklib-*.tar.gz --install-service --with-ui
```

Supported bootstrap options:

- `--vddk-dir <path>`
- `--vddk-tar <path>`
- `--config <path>`
- `--bin-path <path>`
- `--listen <addr>` (API service listen, default `:8000`)
- `--ui-listen <addr>` (UI service listen, default `0.0.0.0:5173`)
- `--install-service` (installs `v2c-engine` and, with `--with-ui`, `v2c-ui`)
- `--with-ui` (installs frontend dependencies and UI service unit)
- `--start-services` (optional immediate start after setup)
- `--skip-build`

## Enable API and UI Services (Top Priority)

Bootstrap installs service units by default without auto-start (unless `--start-services` is passed).  
Configure first, then enable/start:

```bash
sudo vi /etc/v2c-engine/config.yaml
sudo vi /etc/v2c-ui/.env.local
sudo systemctl enable --now v2c-engine v2c-ui
systemctl status v2c-engine v2c-ui
journalctl -u v2c-engine -f
```

Installed paths:

- Engine binary: `/usr/local/bin/v2c-engine`
- Engine config: `/etc/v2c-engine/config.yaml`
- UI env config: `/etc/v2c-ui/.env.local`
- Runtime state/log root: `/var/lib/vm-migrator`

Config notes:

- `run`/`serve` use vCenter credentials from `vcenter` block in config (`VC_PASSWORD` env fallback).
- You do not need a second vCenter credential block under `vddk`.
- CloudStack endpoint input is flexible:
  - `10.0.35.146`
  - `10.0.35.146:8080`
  - `http://10.0.35.146:8080/client/api`
  - `https://cloudstack.example.com`

## What The Engine Does

- `run` is the primary user command.
- Internal base copy and delta sync are handled automatically inside `run`.
- Base copy and delta write directly into QCOW2 (no RAW intermediate).
- Delta sync uses VMware CBT (`QueryChangedDiskAreas` path).
- Conversion (`virt-v2v-in-place`) runs in `converting` stage after final sync (when enabled).
- Stateful/resumable workflow persists state and logs per VM under `/var/lib/vm-migrator/<vm>_<moref>/`.
- Finalize is supported via:
  - marker file (`FINALIZE`) internally
  - CLI command `v2c-engine finalize` for operators
  - API/UI finalize action

Storage behavior:

- Destination path pattern: `/mnt/<storageid>/<vm>_<vmMoref>_disk<unit>.qcow2`
- Selected CloudStack primary storage is validated as NFS.
- Engine ensures `/mnt/<storageid>` exists and is mounted before copy.
- If not mounted, engine attempts NFS mount using CloudStack storage pool details (`listStoragePools`).

## Primary Commands

```bash
# Run one or more VM migrations
./v2c-engine run --spec ./spec.run.example.yaml --config ./config.yaml
./v2c-engine run --spec ./spec.run.example.yaml --spec ./another-vm.yaml --config ./config.yaml
./v2c-engine run --spec ./spec.run.multi.example.yaml --parallel-vms 3 --config ./config.yaml

# Check status (includes current stage, next stage, finalize_requested)
./v2c-engine status --spec ./spec.run.multi.example.yaml --config ./config.yaml
./v2c-engine status --spec ./spec.run.multi.example.yaml --vm Centos7 --json --config ./config.yaml

# Request finalize for selected VM(s) from a batch spec
./v2c-engine finalize --spec ./spec.run.multi.example.yaml --vm Centos7 --config ./config.yaml
./v2c-engine finalize --spec ./spec.run.multi.example.yaml --vm Centos7 --vm-id vm-3312 --config ./config.yaml

# API service
./v2c-engine serve --config ./config.yaml --listen :8000
```

`finalize` is idempotent:

- If finalize already requested, command returns success and reports it.
- If VM is already done, command returns success and reports completion.

## Run Workflow Stages

Stage order:

- `init`
- `base_copy`
- `delta` / `final_sync`
- `converting`
- `import_root_disk`
- `import_data_disk`
- `done`

Highlights:

- Snapshot quiesce policy: `auto` tries quiesced snapshots when VMware Tools are healthy, else fallback non-quiesced.
- CBT auto-enable if not already enabled.
- Parallel VM and parallel disk support.
- CloudStack import of root + data disks; data disk attach handled in workflow.
- Additional NIC mappings are attached after import VM creation.

## UI/API

UI runs as a service (`v2c-ui`) and talks to `v2c-engine serve`.

API endpoints:

- `GET /vmware/vms`
- `GET /cloudstack/{zones|clusters|storage|networks|diskofferings|serviceofferings}`
- `POST /migration/spec`
- `POST /migration/start`
- `GET /migration/status/{vm}`
- `GET /migration/jobs`
- `POST /migration/finalize/{vm}`
- `GET /migration/logs/{vm}`
- `GET /health`

Status payload includes:

- `stage`
- `next_stage`
- `finalize_requested`
- `overall_progress`
- `transfer_speed_mbps`
- `disk_progress`

## Example Files

Use [examples/README.md](./examples/README.md).

Common templates:

- [examples/config.full.example.yaml](./examples/config.full.example.yaml)
- [examples/spec.run.single-vm.single-disk.single-nic.yaml](./examples/spec.run.single-vm.single-disk.single-nic.yaml)
- [examples/spec.run.single-vm.multi-disk.multi-nic.yaml](./examples/spec.run.single-vm.multi-disk.multi-nic.yaml)
- [examples/spec.run.single-vm.defaults-only.yaml](./examples/spec.run.single-vm.defaults-only.yaml)
- [examples/spec.run.multi-vm.single-disk.single-nic.yaml](./examples/spec.run.multi-vm.single-disk.single-nic.yaml)
- [examples/spec.run.multi-vm.multi-disk.multi-nic.yaml](./examples/spec.run.multi-vm.multi-disk.multi-nic.yaml)
- [examples/spec.run.multi-vm.defaults-only.yaml](./examples/spec.run.multi-vm.defaults-only.yaml)

## Build (Manual)

```bash
go build -o v2c-engine ./cmd/v2c-engine
```

If VDDK is in non-default path:

```bash
export CGO_CFLAGS="-I/opt/vmware-vddk/include"
export CGO_LDFLAGS="-L/opt/vmware-vddk/lib64 -lvixDiskLib -ldl -lpthread"
```

## Uninstall / Reset

```bash
chmod +x ./scripts/uninstall.sh
sudo ./scripts/uninstall.sh --purge-state
```

`uninstall.sh` removes service/files/config artifacts created by bootstrap.  
It does not auto-remove OS packages.

To print the bootstrap package list for manual review/removal:

```bash
./scripts/uninstall.sh --list-packages
```

## Expert-Only Commands

`base-copy` and `delta-sync` are hidden by default.

To enable direct expert usage:

```bash
export V2C_ENABLE_EXPERT_COMMANDS=1
```
