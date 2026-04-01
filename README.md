# Go Engine: VMware -> CloudStack

This repository is Go-first and provides a production migration engine + API/UI services for VMware to CloudStack migrations.

## Project Purpose

The goal of this project is to provide **near-live / warm migration** from VMware to CloudStack with minimal cutover downtime.

How warm migration is achieved:

- A base snapshot is taken and copied to QCOW2 on CloudStack primary storage.
- Incremental delta rounds are continuously synced using VMware CBT (`QueryChangedDiskAreas`).
- At cutover (`Finalize`/`Finalize Now`/`finalize_at`), the source VM is shut down, one final delta sync is applied, then import is completed.

This design keeps source VM downtime mostly to the final sync + import boundary, not the full disk copy duration.

## Engine Internals (Summary)

- Disk copy path: VMware VDDK -> direct QCOW2 writes (no RAW intermediate).
- Delta path: CBT ranges -> direct QCOW2 updates.
- Conversion path: optional `virt-v2v-in-place` after final sync, passing the boot disk first and all remaining VM disks so multi-disk guests convert correctly.
- State machine + resume: per-VM runtime state under `/var/lib/vm-migrator/<vm>_<moref>/`.
- Control actions: `Finalize` and `Finalize Now` markers, plus CLI/API/UI triggers.

## Prerequisites

- Linux host
- VMware VDDK installed (must include `include/vixDiskLib.h` and `lib64/libvixDiskLib.so*`)
  - Official download: [Broadcom VDDK](https://developer.broadcom.com/sdks/vmware-virtual-disk-development-kit-vddk/latest/)
- Root/sudo access (required for service install and NFS mount operations)
- CloudStack API access
- vCenter credentials
- CloudStack primary storage support in this release: **NFS only**
- The migration host/appliance must have network and mount-level access to the NFS primary storage backends selected in CloudStack

The bootstrap script installs required OS packages (Go, qemu tools, virt-v2v, guestfs, and optional node/npm for UI).
This project does not redistribute VDDK. Users must obtain VDDK directly from Broadcom and accept Broadcom licensing terms separately.

## Quick Start (Clone -> Bootstrap -> UI -> CLI)

1. Clone and enter the repository:

```bash
git clone https://github.com/prashanthr2/vmware-to-cloudstack.git
cd vmware-to-cloudstack
```

2. Bootstrap dependencies, build, and install services:

Before running bootstrap, make sure one of these is already present on the host:

- Extracted VDDK directory (for `--vddk-dir`), for example `/opt/vmware-vddk/vmware-vix-disklib-distrib`
- VDDK tarball file (for `--vddk-tar`)

```bash
chmod +x ./scripts/bootstrap.sh
sudo ./scripts/bootstrap.sh --vddk-dir /opt/vmware-vddk/vmware-vix-disklib-distrib --install-service --with-ui
```

If you have a VDDK tarball instead of an extracted directory:

```bash
chmod +x ./scripts/bootstrap.sh
sudo ./scripts/bootstrap.sh --vddk-tar /tmp/VMware-vix-disklib-8.0.2-xxxxxxx.x86_64.tar.gz --install-service --with-ui
```

3. Configure engine and UI endpoint:

```bash
sudo vi /etc/v2c-engine/config.yaml
sudo vi /etc/v2c-ui/.env.local
```

In `/etc/v2c-ui/.env.local`, set:

- `VITE_API_BASE=http://<migration-host-ip>:8000`

Use the IP/hostname of the same host where `v2c-engine serve` is running (not `127.0.0.1` unless browser is on that same host).

4. Start services:

```bash
sudo systemctl enable --now v2c-engine v2c-ui
systemctl status v2c-engine v2c-ui
```

5. Access the UI:

- URL: `http://<migration-host-ip>:5173`
- API health check: `curl -s http://<migration-host-ip>:8000/health`

6. Use CLI (optional/advanced):

```bash
# check migration status for one or more specs
/usr/local/bin/v2c-engine status --spec ./examples/spec.run.single-vm.single-disk.single-nic.yaml --config /etc/v2c-engine/config.yaml

# request finalize (normal)
/usr/local/bin/v2c-engine finalize --spec ./examples/spec.run.single-vm.single-disk.single-nic.yaml --vm Centos7 --config /etc/v2c-engine/config.yaml

# request finalize-now (immediate delta wait interrupt)
/usr/local/bin/v2c-engine finalize --spec ./examples/spec.run.single-vm.single-disk.single-nic.yaml --vm Centos7 --now --config /etc/v2c-engine/config.yaml
```

Note: use `--start-services` in bootstrap only when `/etc/v2c-engine/config.yaml` and `/etc/v2c-ui/.env.local` are already valid.

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
- `--start-services` (optional immediate start after setup; only use when config files are already valid)
- `--skip-build`

Recommended bootstrap flow:

1. Install/build/services without auto-start.
2. Edit config files.
3. Enable/start services.

```bash
sudo ./scripts/bootstrap.sh --vddk-dir /opt/vmware-vddk/vmware-vix-disklib-distrib --install-service --with-ui
sudo vi /etc/v2c-engine/config.yaml
sudo vi /etc/v2c-ui/.env.local
sudo systemctl enable --now v2c-engine v2c-ui
```

Use `--start-services` only if `/etc/v2c-engine/config.yaml` and `/etc/v2c-ui/.env.local` are already populated with real values (not placeholders).

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
- Optional manual build env helper: `/etc/v2c-engine/build.env` (not auto-sourced)
- UI env config: `/etc/v2c-ui/.env.local`
- Runtime state/log root: `/var/lib/vm-migrator`

Environment note:

- Bootstrap does not set global `LD_LIBRARY_PATH` in `/etc/profile.d`.
- This avoids breaking host tools like `journalctl` / `dnf` with VDDK libraries.

Config notes:

- `run`/`serve` use vCenter credentials from `vcenter` block in config (`VC_PASSWORD` env fallback).
- `migration.vddk_path` is required for `run` (path to extracted VDDK root, for example `/opt/vmware-vddk/vmware-vix-disklib-distrib`).
- You do not need a second vCenter credential block under `vddk`.
- CloudStack endpoint input is flexible:
  - `cloudstack-mgmt.example.com`
  - `cloudstack-mgmt.example.com:8080`
  - `http://cloudstack-mgmt.example.com:8080/client/api`
  - `https://cloudstack.example.com`

Sample references:

- Engine config template with all fields: [examples/config.full.example.yaml](./examples/config.full.example.yaml)
- UI env template: [frontend/.env.example](./frontend/.env.example)

## What The Engine Does

- `run` is the primary user command.
- Internal base copy and delta sync are handled automatically inside `run`.
- Base copy and delta write directly into QCOW2 (no RAW intermediate).
- Delta sync uses VMware CBT (`QueryChangedDiskAreas` path).
- Conversion (`virt-v2v-in-place`) runs in `converting` stage after final sync (when enabled).
- Stateful/resumable workflow persists state and logs per VM under `/var/lib/vm-migrator/<vm>_<moref>/`.
- Finalize is supported via:
  - marker file (`FINALIZE`) internally
  - immediate marker file (`FINALIZE_NOW`) internally
  - CLI command `v2c-engine finalize` for operators
  - API/UI finalize action

Storage behavior:

- Destination path pattern: `/mnt/<storageid>/<vm>_<vmMoref>_disk<unit>.qcow2`
- Selected CloudStack primary storage is validated as NFS.
- Engine ensures `/mnt/<storageid>` exists and is mounted before copy.
- If not mounted, engine attempts NFS mount using CloudStack storage pool details (`listStoragePools`).
- On Ubuntu hosts, engine-managed NFS mounts use explicit `vers=3` options to avoid QCOW2 flush I/O issues seen with some NFSv4 environments.
  - Optional override: `V2C_NFS_MOUNT_OPTS="<mount-options>"`

## How It Works

At a high level, each VM migration follows this model:

1. The engine connects to vCenter, finds the VM, verifies disks/NICs, and ensures CBT is enabled.
2. It creates a base snapshot and copies each VMware disk directly into QCOW2 on the selected CloudStack primary storage mount.
3. It enters delta mode and repeatedly uses VMware CBT to fetch only changed blocks since the previous snapshot.
4. When finalize is requested, or when `finalize_at` time is reached, the engine shuts down the source VM according to policy and performs one final delta sync.
5. If enabled, it runs `virt-v2v-in-place` on the boot QCOW2 plus any additional VM disks.
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

### Manual Finalize and Finalize Now

You can trigger finalize explicitly even if `finalize_at` is not set.

Supported methods:

- CLI:
  - `./v2c-engine finalize --spec ./spec.run.multi.example.yaml --vm Centos7 --config ./config.yaml`
  - `./v2c-engine finalize --spec ./spec.run.multi.example.yaml --vm Centos7 --now --config ./config.yaml`
- API:
  - `POST /migration/finalize/{vm}`
  - `POST /migration/finalize/{vm}?now=true`
- UI:
  - `Finalize` and `Finalize Now` actions from Progress tab

Behavior:

- `Finalize` creates a finalize request and the workflow picks it up in the delta loop.
- `Finalize Now` requests immediate cutover from the delta loop wait:
  - it interrupts delta sleep and moves to `final_sync` as soon as possible.
  - if currently in `base_copy`, base copy still completes first, then workflow moves directly into finalization path.
- Both requests are idempotent.
- If VM is already complete, finalize calls return success with completion status.

### Retry Failed Migration

If a VM migration job fails, you can retry it from UI or API without regenerating all settings.

Supported methods:

- API:
  - `POST /migration/retry/{vm}`
  - Optional query: `?spec_file=/absolute/path/to/spec.yaml` to force a specific spec.
  - If `spec_file` is not provided, server retries using the latest resolved spec for that VM.
- UI:
  - `Retry` action from Progress tab (enabled only for failed jobs).

Behavior:

- Retry creates a new job ID and keeps previous failed job history.
- Retry is blocked (`409`) if a job for that VM is already `queued` or `running`.

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

# Check status (includes current stage, next stage, finalize_requested, finalize_now_requested)
./v2c-engine status --spec ./spec.run.multi.example.yaml --config ./config.yaml
./v2c-engine status --spec ./spec.run.multi.example.yaml --vm Centos7 --json --config ./config.yaml

# Request finalize for selected VM(s) from a batch spec
./v2c-engine finalize --spec ./spec.run.multi.example.yaml --vm Centos7 --config ./config.yaml
./v2c-engine finalize --spec ./spec.run.multi.example.yaml --vm Centos7 --now --config ./config.yaml
./v2c-engine finalize --spec ./spec.run.multi.example.yaml --vm Centos7 --vm-id vm-3312 --config ./config.yaml

# API service
./v2c-engine serve --config ./config.yaml --listen :8000
```

`finalize` is idempotent:

- If finalize already requested, command returns success and reports it.
- If finalize-now already requested (`--now`), command returns success and reports it.
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
- `POST /migration/retry/{vm}`
  - Optional query: `?spec_file=...`
- `GET /migration/status/{vm}`
- `GET /migration/jobs`
- `POST /migration/finalize/{vm}`
  - Optional query: `?now=true` for immediate finalize request
- `GET /migration/logs/{vm}`
- `GET /health`

Status payload includes:

- `stage`
- `next_stage`
- `finalize_requested`
- `finalize_now_requested`
- `overall_progress`
- `transfer_speed_mbps`
- `disk_progress`

## Example Files

Use [examples/README.md](./examples/README.md).

Known limitations and platform-specific workarounds are tracked in
[docs/KNOWN_ISSUES.md](./docs/KNOWN_ISSUES.md).

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
