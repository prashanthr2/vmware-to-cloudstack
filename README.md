# Go Engine: VMware -> CloudStack

This repository is now Go-first and contains the migration copy/sync engine with direct VDDK reads and direct QCOW2 writes.

## What it does

- `run` is the primary migration command for end users.
- Internally, `run` performs base copy and delta sync:
  - Opens multiple independent VDDK handles (`VixDiskLib_Open`) in parallel readers.
  - Uses a shared dynamic work queue.
  - Uses adaptive chunk sizing (1 MB to 4 MB) based on read performance.
  - Detects all-zero blocks and skips writes to preserve sparse QCOW2 behavior.
  - Auto-detects source disk capacity from VDDK.
  - Writes directly to QCOW2 via `qemu-nbd` (no RAW intermediary).
  - Uses CBT changed ranges for delta rounds.
  - Runs `virt-v2v-in-place` in the `converting` stage (after final sync), when enabled.
- `base-copy` and `delta-sync` remain available as expert/internal commands only.

## Build prerequisites

- Linux host with:
  - VMware VDDK headers and shared libraries (`vixDiskLib.h`, `libvixDiskLib.so`)
  - `qemu-img`
  - `qemu-nbd`
  - `virt-v2v-in-place` (or adjust command in code)
- Go toolchain 1.22+
- `CGO_ENABLED=1`

CloudStack endpoint input is flexible:
- `10.0.35.146`
- `10.0.35.146:8080`
- `http://10.0.35.146:8080/client/api`
- `https://cloudstack.example.com`

If a port is required in your environment, specify it explicitly (for example `host:8080`).

## One-command bootstrap (recommended)

For operators, use the bootstrap script to install packages, setup VDDK env, build binary, and optionally create a systemd service:

```bash
chmod +x ./scripts/bootstrap.sh
sudo ./scripts/bootstrap.sh --vddk-dir /opt/vmware-vddk/vmware-vix-disklib-distrib --install-service --with-ui
```

If you have a VDDK tarball instead of extracted directory:

```bash
./scripts/bootstrap.sh --vddk-tar /tmp/VMware-vix-disklib-*.tar.gz --install-service --with-ui
```

The bootstrap service install places:
- executable at `/usr/local/bin/v2c-engine`
- active service config at `/etc/v2c-engine/config.yaml`

This keeps systemd runtime independent from where the repo is cloned (`/root`, `/home`, etc.).

After bootstrap:
- edit `/etc/v2c-engine/config.yaml` with vCenter + CloudStack details
- `systemctl status v2c-engine`
- `journalctl -u v2c-engine -f`

Config note:
- `run`/`serve` use vCenter credentials only from `vcenter` in config (plus `VC_PASSWORD` fallback).
- You do not need a second `vddk` credential block in `config.yaml`.
- For expert `base-copy` / `delta-sync` commands, `thumbprint` is optional; engine auto-detects it from `--server` when not provided.

## Clean uninstall / reset

To remove bootstrap-installed service/binary/config and reinstall from scratch:

```bash
chmod +x ./scripts/uninstall.sh
sudo ./scripts/uninstall.sh --purge-state
sudo ./scripts/bootstrap.sh --vddk-dir /opt/vmware-vddk/vmware-vix-disklib-distrib --install-service --with-ui
```

`uninstall.sh` does not remove OS packages automatically.  
To view the bootstrap package list for manual review/removal:

```bash
./scripts/uninstall.sh --list-packages
```

## Example files

Use the example pack in [examples/README.md](./examples/README.md).

Recommended starting points:

- Full operator config with comments:
  - [examples/config.full.example.yaml](./examples/config.full.example.yaml)
- Single VM, single disk, single NIC:
  - [examples/spec.run.single-vm.single-disk.single-nic.yaml](./examples/spec.run.single-vm.single-disk.single-nic.yaml)
- Single VM, multiple disks, multiple NICs:
  - [examples/spec.run.single-vm.multi-disk.multi-nic.yaml](./examples/spec.run.single-vm.multi-disk.multi-nic.yaml)
- Single VM using config defaults:
  - [examples/spec.run.single-vm.defaults-only.yaml](./examples/spec.run.single-vm.defaults-only.yaml)
- Multiple VMs, single disk, single NIC:
  - [examples/spec.run.multi-vm.single-disk.single-nic.yaml](./examples/spec.run.multi-vm.single-disk.single-nic.yaml)
- Multiple VMs, multiple disks, multiple NICs:
  - [examples/spec.run.multi-vm.multi-disk.multi-nic.yaml](./examples/spec.run.multi-vm.multi-disk.multi-nic.yaml)
- Multiple VMs using config defaults:
  - [examples/spec.run.multi-vm.defaults-only.yaml](./examples/spec.run.multi-vm.defaults-only.yaml)

## Build

```bash
go build -o v2c-engine ./cmd/v2c-engine
```

If headers/libs are in non-default locations, set:

```bash
export CGO_CFLAGS="-I/opt/vmware-vddk/include"
export CGO_LDFLAGS="-L/opt/vmware-vddk/lib64 -lvixDiskLib -ldl -lpthread"
```

## Spec-file mode (UI-friendly)

`v2c-engine` can read a YAML spec directly:

```bash
./v2c-engine run --spec ./spec.run.example.yaml --config ./config.yaml
./v2c-engine run --spec ./spec.run.example.yaml --spec ./another-vm.yaml --config ./config.yaml
./v2c-engine run --spec ./spec.run.multi.example.yaml --parallel-vms 3 --config ./config.yaml
./v2c-engine serve --config ./config.yaml --listen :8000
```

`run` mode follows the established migration workflow for base copy and delta loop scheduling:
- Uses a resumable JSON state machine per VM at `/var/lib/vm-migrator/<vm>_<moref>/state.json`.
- Legacy `state.engine.json` is auto-read for resume, then new updates continue in `state.json`.
- Writes migration logs with timestamps to `/var/lib/vm-migrator/<vm>_<moref>/migration.log` (also mirrored to stderr).
- Stage order is: `init -> base_copy -> delta/final_sync -> converting -> import_root_disk -> import_data_disk -> done`.
- The first snapshot is always created as `Migrate_Base_<vm>` during `init`; delta snapshots are only created in delta stages.
- Snapshot policy: `snapshot_quiesce=auto` tries quiesced snapshots only when VMware Tools is healthy, then falls back to non-quiesced.
- CBT policy: if CBT is not enabled, the engine enables CBT before base/delta work.
- Persists per-disk progress fields (progress %, bytes read/written, speed, ETA) and overall VM progress.
- `FINALIZE` file behavior is supported: if `/var/lib/vm-migrator/<vm>_<moref>/FINALIZE` exists, the engine runs one final delta round (`final_sync`) and proceeds to import.
- Before `final_sync`, source VM shutdown is enforced using `shutdown_mode` (`auto|force|manual`) from spec/config.
- Reads VM and migration strategy from spec (`delta_interval`, `finalize_at`, etc.).
- Resolves destination disk path from CloudStack storage selection:
  - boot disk -> `target.cloudstack.storageid`
  - data disk -> `disks.<unit>.storageid`
  - output path format -> `/mnt/<storageid>/<vm>_<vmMoref>_disk<unit>.qcow2`
- Imports root VM and data disks into CloudStack and attaches imported data volumes.
- Uses CloudStack target fields from spec:
  - `target.cloudstack.zoneid`
  - `target.cloudstack.clusterid`
  - `target.cloudstack.networkid`
  - `target.cloudstack.serviceofferingid`
  - `target.cloudstack.storageid`
- Merges defaults from `config.yaml -> cloudstack_defaults` when these fields are omitted in spec.
- For data disks, `diskofferingid` resolves with fallback order:
  - `disks.<unit>.diskofferingid`
  - `target.cloudstack.diskofferingid`
  - `config.yaml cloudstack_defaults.diskofferingid`
- Uses `migration.readers` and `migration.run_virt_v2v` from spec.
- Supports parallel disk migration within the same VM via `migration.parallel_disks` (or `--parallel-disks`).
- Supports parallel VM migrations by passing multiple `--spec` values and setting `--parallel-vms` (or config `migration.parallel_vms`).
- Also accepts spec files with top-level `vms:` list.
- `run_virt_v2v` default comes from `config.yaml -> virt.run_virt_v2v`, and can be overridden per-VM via `spec.migration.run_virt_v2v`.
- If `virt-v2v-in-place` does not support `--inject-virtio-win`, the engine retries conversion without that flag for compatibility.

You can still override any value from spec with CLI flags:

```bash
./v2c-engine run --spec ./spec.run.example.yaml --readers 8 --override-run-virt-v2v --run-virt-v2v=true
./v2c-engine run --spec ./specs.yaml --parallel-vms 3 --parallel-disks 4
```

Use [spec.run.example.yaml](./spec.run.example.yaml) as the template for full run-mode specs.
Use [spec.run.multi.example.yaml](./spec.run.multi.example.yaml) for top-level `vms:` batch format.

Expert mode note:
- `base-copy` and `delta-sync` are hidden from normal CLI usage.
- To enable them for troubleshooting, set `V2C_ENABLE_EXPERT_COMMANDS=1`.

## GUI integration (pure Go)

The React UI can run directly against `v2c-engine serve` (no Python backend required):

```bash
./v2c-engine serve --config ./config.yaml --listen :8000
```

`serve` implements the same API shape used by the UI:
- `GET /vmware/vms`
- `GET /cloudstack/{zones|clusters|storage|networks|diskofferings|serviceofferings}`
- `POST /migration/spec`
- `POST /migration/start`
- `GET /migration/status/{vm}`
- `GET /migration/jobs`
- `POST /migration/finalize/{vm}`
- `GET /migration/logs/{vm}`
- `GET /health`

Optional `serve` flags:

```bash
./v2c-engine serve \
  --config ./config.yaml \
  --listen :8000 \
  --control-dir /var/lib/vm-migrator \
  --specs-dir /var/lib/vm-migrator \
  --workdir /var/lib/vm-migrator \
  --max-workers 3
```

Troubleshooting:
- If API job start fails with `chdir ... no such file or directory`, set a valid `migration.workdir` in config or start serve with `--workdir /var/lib/vm-migrator`.
