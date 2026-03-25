# Go Engine: VMware -> CloudStack

This repository is Go-first and provides a production migration engine + API/UI services for VMware to CloudStack migrations.

## Prerequisites

- Linux host
- VMware VDDK installed (must include `include/vixDiskLib.h` and `lib64/libvixDiskLib.so*`)
- Root/sudo access (required for service install and NFS mount operations)
- CloudStack API access
- vCenter credentials

The bootstrap script installs required OS packages (Go, qemu tools, virt-v2v, guestfs, and optional node/npm for UI).

## Bootstrap Script Options

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

## Enable API and UI Services

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

## How It Works

At a high level, each VM migration follows this model:

1. The engine connects to vCenter, finds the VM, verifies disks/NICs, and ensures CBT is enabled.
2. It creates a base snapshot and copies each VMware disk directly into QCOW2 on the selected CloudStack primary storage mount.
3. It enters delta mode and repeatedly uses VMware CBT to fetch only changed blocks since the previous snapshot.
4. When finalize is requested, or when `finalize_at` time is reached, the engine shuts down the source VM according to policy and performs one final delta sync.
5. If enabled, it runs `virt-v2v-in-place` on the boot QCOW2.
6. It imports the root disk into CloudStack, then imports and attaches data disks, and finally attaches additional NICs.

Important behavior:

- Base and delta both write directly into QCOW2.
- Delta sync preserves QCOW2 metadata by writing through the qemu block path.
- The workflow is resumable through `state.json`.
- `status` reports current stage, next stage, and whether finalize has already been requested.

## Migration Methods

The main migration strategies are controlled by the `migration:` block in the VM spec.

### Continuous Delta Loop

This is the default behavior when `delta_interval` is set.

Parameters:

- `delta_interval`
  - Required for normal continuous sync behavior.
  - Unit: seconds.
  - Controls how often the engine performs a delta round during the pre-cutover phase.

Behavior:

- Base copy completes first.
- The engine waits `delta_interval` seconds before the first delta round.
- It then keeps running delta rounds every `delta_interval` seconds until finalize is triggered.
- Finalize can be triggered manually from CLI/API/UI.

Example:

```yaml
migration:
  delta_interval: 300
```

### Scheduled Finalize

This is used when you want the tool to keep syncing until a planned cutover time.

Parameters:

- `finalize_at`
  - Optional.
  - Accepts ISO-like timestamps such as:
    - `2026-03-12T23:30:00+00:00`
    - `2026-03-12T23:30:00`
    - `2026-03-12T23:30`
- `finalize_delta_interval`
  - Optional.
  - Unit: seconds.
  - Default: `30`
  - Used when the engine is inside the finalize window and wants tighter sync frequency before cutover.
- `finalize_window`
  - Optional.
  - Unit: seconds.
  - Default: `600`
  - If current time is within `finalize_window` seconds of `finalize_at`, the engine reduces the sleep interval to `finalize_delta_interval`.

Behavior:

- The engine still does normal delta rounds after base copy.
- Before the `finalize_at` time, it uses:
  - `delta_interval` normally
  - `finalize_delta_interval` once the engine is inside the finalize window
- Once the current time passes `finalize_at`, the engine treats that as a finalize request.
- It then powers off the source VM according to `shutdown_mode`, performs `final_sync`, and continues import.

Example:

```yaml
migration:
  delta_interval: 300
  finalize_at: "2026-03-12T23:30:00+00:00"
  finalize_delta_interval: 30
  finalize_window: 600
```

### Manual Finalize

You can trigger finalize explicitly even if `finalize_at` is not set.

Supported methods:

- CLI:
  - `./v2c-engine finalize --spec ./spec.run.multi.example.yaml --vm Centos7 --config ./config.yaml`
- API:
  - `POST /migration/finalize/{vm}`
- UI:
  - `Finalize` action from Progress tab

Behavior:

- Finalize request is idempotent.
- If finalize is already requested, the engine reports that state and does not duplicate work.
- If the VM is already complete, finalize returns success with completion status.

## Workflow Diagram

```text
                  +----------------------+
                  |   v2c-engine run     |
                  +----------+-----------+
                             |
                             v
                  +----------------------+
                  | init                 |
                  | - find VM            |
                  | - ensure CBT         |
                  | - create base snap   |
                  +----------+-----------+
                             |
                             v
                  +----------------------+
                  | base_copy            |
                  | - VDDK reads         |
                  | - write QCOW2        |
                  | - per-disk parallel  |
                  +----------+-----------+
                             |
                             v
                  +----------------------+
                  | delta loop           |
                  | - wait delta_interval|
                  | - create delta snap  |
                  | - QueryChanged...    |
                  | - apply CBT blocks   |
                  +----------+-----------+
                             |
              +--------------+---------------+
              |                              |
              | finalize requested?          | no
              | finalize_at reached?         +------> back to delta loop
              |
              v
   +---------------------------+
   | final_sync                |
   | - shutdown source VM      |
   | - create final snapshot   |
   | - apply last CBT changes  |
   +-------------+-------------+
                 |
                 v
   +---------------------------+
   | converting                |
   | - virt-v2v-in-place       |
   |   (if enabled)            |
   +-------------+-------------+
                 |
                 v
   +---------------------------+
   | import_root_disk          |
   | - importVm                |
   | - attach extra NICs       |
   +-------------+-------------+
                 |
                 v
   +---------------------------+
   | import_data_disk          |
   | - importVolume            |
   | - attachVolume            |
   +-------------+-------------+
                 |
                 v
   +---------------------------+
   | done                      |
   +---------------------------+
```

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
