# Known Issues

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

