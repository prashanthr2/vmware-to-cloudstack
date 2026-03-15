# Go Rewrite: VMware -> QCOW2 Engine

This folder contains a Go rewrite of the migration copy/sync engine with direct VDDK reads and direct QCOW2 writes.

## What it does

- `base-copy`:
  - Opens multiple independent VDDK handles (`VixDiskLib_Open`) in parallel readers.
  - Uses a shared dynamic work queue.
  - Starts with 1 MB reads, adaptively increases up to 4 MB, and shrinks on high latency.
  - Detects all-zero blocks and skips writes (sparse QCOW2 preserved).
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

## Mapping from current Python flow

- Replace Python `copy_disk_base(...)` with `v2c-engine base-copy`.
- Replace Python `delta(...)` write loop with `v2c-engine delta-sync`.
- Keep existing snapshot creation/CBT range discovery/state/import phases unchanged, but execute them from Go orchestration if you are fully migrating runtime to Go.

