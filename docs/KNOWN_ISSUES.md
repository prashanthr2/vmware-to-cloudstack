# Known Issues

## Ubuntu hosts: NFSv4 can cause QCOW2 flush I/O errors on some environments

### Symptom

Base copy fails with messages like:

- `qemu-nbd: Failed to flush the L2 table cache: Input/output error`
- `qemu-nbd: Failed to flush the refcount block cache: Input/output error`
- `qcow2 write failed ... nbd server returned error=5`

Kernel logs may show:

- `NFS: <server-ip>: lost <n> locks`

### Why this happens

In some Ubuntu + NFS server combinations, the default NFSv4 mount behavior can be unstable for heavy QCOW2 metadata flush workloads.
This is an environment/mount-level issue (reproducible with `qemu-io`), not a VMware/VDDK read-path issue.

### Workaround

Use NFSv3-style mount options for CloudStack primary storage mounts.

The engine now does this automatically when it performs the mount on Ubuntu:

- `rw,relatime,vers=3,rsize=1048576,wsize=1048576,hard,proto=tcp,timeo=600,retrans=2`

If the storage path is already mounted externally, the engine will reuse it as-is and will not remount automatically.
In that case, remount with appropriate options manually.

Advanced override:

- Set `V2C_NFS_MOUNT_OPTS` to force explicit mount options for engine-managed mounts.

## `virt-v2v-in-place` fails for CentOS 7 XFS v4 guests on AlmaLinux/RHEL 10

### Symptom

During conversion stage, migration fails with messages like:

- `XFS (sda2): Deprecated V4 format (crc=0) not supported by kernel`
- `virt-v2v-in-place: error: libguestfs error: mount_ro ... wrong fs type`

### Why this happens

On AlmaLinux/RHEL 10 hosts, the guestfs appliance kernel no longer supports deprecated XFS v4.
Some older Linux guests (for example CentOS 7) still use XFS v4, so libguestfs cannot mount the guest filesystem.

### Impact

- Base copy and delta sync can still complete.
- Conversion (`virt-v2v-in-place`) fails at `converting` stage for affected guests.

### Workaround (Dual-Host Conversion)

Use two hosts:

- **Host A**: main migration engine host (can be Alma/RHEL 10)
- **Host B**: conversion host with EL8/EL9-style guestfs stack that can handle XFS v4

Workflow:

1. Run migration on Host A as usual until it reaches `converting` and fails.
2. Keep QCOW2 on shared NFS path (for example `/mnt/<storageid>/<vm>_disk0.qcow2`).
3. On Host B, mount the same NFS path and run:
   - `virt-v2v-in-place -i disk /mnt/<storageid>/<vm>_disk0.qcow2`
4. After conversion succeeds on Host B, return to Host A.
5. Ensure retry on Host A does not re-run conversion:
   - set VM spec `migration.run_virt_v2v: false` (or equivalent CLI override).
6. Retry migration from UI/API/CLI on Host A; it resumes and continues import.

Important:

- Do not run delta/final sync writes while Host B is running `virt-v2v-in-place` on the same QCOW2 file.
