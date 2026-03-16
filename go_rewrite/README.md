# Go Rewrite: VMware -> QCOW2 Engine

This folder contains a Go rewrite of the migration copy/sync engine with direct VDDK reads and direct QCOW2 writes.

## What it does

- `base-copy`:
  - Opens multiple independent VDDK handles (`VixDiskLib_Open`) in parallel readers.
  - Uses a shared dynamic work queue.
  - Starts with 1 MB reads, adaptively increases up to 4 MB, and shrinks on high latency.
  - Detects all-zero blocks and skips writes (sparse QCOW2 preserved).
  - Auto-detects source disk capacity from VDDK (ignores mismatched `-disk-size-bytes`).
  - Writes non-zero blocks directly into QCOW2 through `qemu-nbd`.
  - Runs `virt-v2v-in-place` immediately after base copy (optional flag).

- `delta-sync`:
  - Accepts CBT changed ranges (JSON file).
  - Reads changed blocks via VDDK.
  - Writes changed blocks directly into existing QCOW2 (no RAW intermediary).
  - Preserves QCOW2 metadata by writing via qemu block layer (`qemu-nbd` + NBD protocol).

## Build prerequisites

- Linux host with:
  - VMware VDDK headers and shared libraries (`vixDiskLib.h`, `libvixDiskLib.so`)
  - `qemu-img`
  - `qemu-nbd`
  - `virt-v2v-in-place` (or adjust command in code)
- Go toolchain 1.22+
- `CGO_ENABLED=1`

## Build

```bash
cd go_rewrite
go build -o v2c-engine ./cmd/v2c-engine
```

If headers/libs are in non-default locations, set:

```bash
export CGO_CFLAGS="-I/opt/vmware-vddk/include"
export CGO_LDFLAGS="-L/opt/vmware-vddk/lib64 -lvixDiskLib -ldl -lpthread"
```

## Example: base copy

```bash
./v2c-engine base-copy \
  -vddk-libdir /opt/vmware-vddk/vmware-vix-disklib-distrib \
  -server 10.0.35.3 \
  -user administrator@vsphere.local \
  -password '***' \
  -thumbprint 'AA:BB:CC:...' \
  -vm-moref vm-123 \
  -snapshot-moref snapshot-456 \
  -disk-path '[datastore1] vm/vm_1-000001.vmdk' \
  -target-qcow2 /mnt/storage/vm_disk1.qcow2 \
  -disk-size-bytes 21474836480 \
  -readers 4 \
  -min-chunk-mb 1 \
  -max-chunk-mb 4 \
  -run-virt-v2v=true
```

## Example: delta sync

`ranges.json` format:

```json
[
  { "start": 0, "length": 1048576 },
  { "start": 8388608, "length": 4194304 }
]
```

```bash
./v2c-engine delta-sync \
  -vddk-libdir /opt/vmware-vddk/vmware-vix-disklib-distrib \
  -server 10.0.35.3 \
  -user administrator@vsphere.local \
  -password '***' \
  -thumbprint 'AA:BB:CC:...' \
  -vm-moref vm-123 \
  -snapshot-moref snapshot-789 \
  -disk-path '[datastore1] vm/vm_1-000002.vmdk' \
  -target-qcow2 /mnt/storage/vm_disk1.qcow2 \
  -ranges-file /tmp/ranges.json \
  -readers 4 \
  -chunk-mb 4
```

## Spec-file mode (UI-friendly)

`v2c-engine` can read a YAML spec directly:

```bash
./v2c-engine run --spec ./spec.run.example.yaml --config ../config.yaml
./v2c-engine run --spec ./spec.run.example.yaml --spec ./another-vm.yaml --config ../config.yaml
./v2c-engine run --spec ./spec.run.multi.example.yaml --parallel-vms 3 --config ../config.yaml
./v2c-engine base-copy --spec ./spec.engine.example.yaml
./v2c-engine delta-sync --spec ./spec.engine.example.yaml
```

`run` mode follows the Python-style workflow for base copy and delta loop scheduling:
- Uses a resumable JSON state machine per VM at `/var/lib/vm-migrator/<vm>_<moref>/state.json`.
- Legacy `state.engine.json` is auto-read for resume, then new updates continue in `state.json`.
- Stage order is: `init -> base_copy -> delta/final_sync -> import_root_disk -> import_data_disk -> done`.
- The first snapshot is always created as `Migrate_Base_<vm>` during `init`; delta snapshots are only created in delta stages.
- Persists per-disk progress fields (progress %, bytes read/written, speed, ETA) and overall VM progress.
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
- Also accepts spec files with top-level `vms:` list, same as Python behavior.

You can still override any value from spec with CLI flags:

```bash
./v2c-engine base-copy --spec ./spec.engine.example.yaml -readers 8
./v2c-engine run --spec ./spec.run.example.yaml --readers 8 --override-run-virt-v2v --run-virt-v2v=true
./v2c-engine run --spec ./specs.yaml --parallel-vms 3 --parallel-disks 4
```

Use [spec.engine.example.yaml](./spec.engine.example.yaml) as the template for UI-generated specs.
Use [spec.run.example.yaml](./spec.run.example.yaml) as the template for full run-mode specs.
Use [spec.run.multi.example.yaml](./spec.run.multi.example.yaml) for top-level `vms:` batch format.

## Mapping from current Python flow

- Replace Python `copy_disk_base(...)` with `v2c-engine base-copy`.
- Replace Python `delta(...)` write loop with `v2c-engine delta-sync`.
- Keep existing snapshot creation/CBT range discovery/state/import phases unchanged, but execute them from Go orchestration if you are fully migrating runtime to Go.
