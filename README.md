# vmware-to-cloudstack

VMware to CloudStack Near-Zero Downtime Migration

This project provides a near-zero downtime VM migration pipeline from VMware vSphere to KVM on Apache CloudStack using:

VMware CBT (Changed Block Tracking) for incremental disk sync

RAW disks for block-accurate replication

virt-v2v for final guest OS conversion (drivers + bootloader)

CloudMonkey (cmk) for CloudStack API integration

Downtime is limited to the final sync and VM boot.

High-Level Architecture
┌──────────────────────────┐
│ VMware vCenter / ESXi    │
│                          │
│  VM (Running)            │
│  ├─ Disk(s)              │
│  └─ CBT + Snapshots      │
└───────────┬──────────────┘
            │
            │  Base sync + CBT deltas
            ▼
┌──────────────────────────┐
│ Migration Engine (Python)│
│  (Runs on KVM Host)      │
│                          │
│  ├─ Auto-discover disks  │
│  ├─ Base RAW export      │
│  ├─ Incremental CBT sync │
│  └─ Final CBT sync       │
└───────────┬──────────────┘
            │
            │  RAW disks (bit-accurate)
            ▼
┌──────────────────────────┐
│ virt-v2v (Finalizer)     │
│                          │
│  ├─ RAW → QCOW2          │
│  ├─ Inject virtio drivers│
│  ├─ Fix bootloader       │
│  └─ Remove VMware tools  │
└───────────┬──────────────┘
            │
            │  QCOW2 disks
            ▼
┌──────────────────────────┐
│ CloudStack (via cmk)     │
│                          │
│  ├─ Import volumes       │
│  ├─ Deploy VM            │
│  └─ Attach networks      │
└──────────────────────────┘
Key Design Principles

No user-provided VMDK paths

Disks are auto-discovered from vCenter using VM name

CBT is applied per disk

Disk offsets are preserved exactly

Guest OS conversion happens only once, at the end

CloudStack lifecycle is respected (no internal hacks)

Prerequisites
VMware

vCenter / ESXi with CBT support

Snapshot quiescing supported

VM must not change disk layout during migration

KVM / Migration Host

Linux (RHEL / Rocky / Alma recommended)

Python 3.8+

VMware VDDK installed:

/opt/vmware/vddk/bin/vixDiskLibSample

Packages:

dnf install -y libguestfs-tools virt-v2v virtio-win qemu-img
CloudStack

Working CloudStack environment

CloudMonkey (cmk) installed and configured

API key and secret configured

Disk offerings, service offerings, networks pre-created

Install Python Dependencies
pip install -r requirements.txt
Repository Layout
vmware-to-cloudstack/
├── migrate.py                 # Base / delta / finalize sync
├── finalize_os.py             # virt-v2v guest OS conversion
├── import_to_cloudstack.py    # CloudStack import via cmk
├── cloudstack_mapping.yaml    # User-defined CloudStack mapping
├── requirements.txt
└── lib/
    ├── vcenter.py
    ├── snapshot.py
    ├── cbt.py
    ├── vm_disks.py            # Disk auto-discovery
    ├── vddk_reader.py
    ├── raw_writer.py
    ├── virt_v2v.py
    ├── cloudstack.py
    ├── cloudstack_volume.py
    ├── cloudstack_vm_import.py
    └── metadata.py
Migration Workflow
Phase 1: Base Migration (No Downtime)
python migrate.py \
  --vcenter vc.example.com \
  --username administrator@vsphere.local \
  --password 'VMWARE_PASSWORD' \
  --vm-name MyVM \
  --target-path /data/migrations/MyVM \
  --mode base

What happens:

VM is located by name

All disks are auto-discovered

CBT is enabled

Snapshot is taken

Full disk(s) exported to RAW

Metadata is written

Result:

/data/migrations/MyVM/
├── disk0.raw
├── disk1.raw
└── migration.json
Phase 2: Incremental Delta Sync (Repeatable)
python migrate.py \
  --vcenter vc.example.com \
  --username administrator@vsphere.local \
  --password 'VMWARE_PASSWORD' \
  --vm-name MyVM \
  --target-path /data/migrations/MyVM \
  --mode delta

What happens:

New snapshot created

CBT queried per disk

Only changed blocks are synced

No downtime

Run this as many times as needed.

Phase 3: Final Sync (Short Downtime)
python migrate.py \
  --vcenter vc.example.com \
  --username administrator@vsphere.local \
  --password 'VMWARE_PASSWORD' \
  --vm-name MyVM \
  --target-path /data/migrations/MyVM \
  --mode finalize

What happens:

VM is powered off

Final CBT delta applied

RAW disks are now consistent

Phase 4: Guest OS Conversion (virt-v2v)
python finalize_os.py \
  --raw-disk /data/migrations/MyVM/disk0.raw \
  --output-dir /data/migrations/MyVM/final

What happens:

RAW → QCOW2 conversion

virtio drivers injected

Bootloader fixed (BIOS or UEFI)

VMware tools removed

Mandatory for Windows, strongly recommended for Linux.

Phase 5: CloudStack Mapping

Edit cloudstack_mapping.yaml:

vm:
  name: myvm-cloudstack
  zone: ZONE_ID
  service_offering: SERVICE_OFFERING_ID
  os_type: OS_TYPE_ID

networks:
  - Prod-Network

disks:
  - name: root
    path: /data/migrations/MyVM/final/disk0.qcow2
    disk_offering: DISK_OFFERING_ID

Users map:

Disk offerings

OS type

Networks

They do not deal with disk paths from VMware.

Phase 6: Import into CloudStack
python import_to_cloudstack.py

What happens:

QCOW2 disks imported as CloudStack volumes

VM deployed

Volumes attached

VM started

Downtime Expectations
Phase	Downtime
Base sync	None
Delta sync	None
Final sync	Required
virt-v2v	Offline (already powered off)
CloudStack boot	Normal boot

Typical downtime: 1–5 minutes

Supported Guest OS
Linux

RHEL / CentOS / Rocky / Alma

Ubuntu / Debian

SUSE

Windows

Windows Server 2012 R2+

Windows Server 2016 / 2019 / 2022

Windows 10 / 11 (UEFI)

Best Practices

Do not resize disks during migration

Match BIOS vs UEFI in CloudStack

Use RAW for CBT sync, QCOW2 only after final sync

Expect first Windows boot to take longer

Always test with a snapshot or clone first

What This Is (and Isn’t)

✅ Near-zero downtime
✅ Block-level correctness
✅ CloudStack-native
✅ Product-ready architecture

❌ Live cross-hypervisor migration
❌ Filesystem-level replication

Summary

This project combines VMware CBT, virt-v2v, and CloudStack APIs to deliver a safe, scalable, near-zero-downtime migration solution suitable for enterprise environments and product integration.
