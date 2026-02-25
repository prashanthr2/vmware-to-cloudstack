# VMware to CloudStack Near-Zero Downtime Migration

This project provides a comprehensive solution for migrating virtual machine (VM) workloads from **VMware vSphere** to **Apache CloudStack** with minimal service interruption.

---

## Overview
The migration process is designed to achieve near-zero downtime by:
* Initial data sync while the VM is running.
* Incremental delta syncs to capture changes during the migration window.
* A brief final sync during a controlled cutover.
* Automated Guest OS conversion (virt-v2v).
* Seamless import into the CloudStack environment.
```text
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
```
---

## Key Features
* **Near-Zero Downtime:** Minimize business impact.
* **Volume-Level Sync:** Efficient data transfer via Changed Block Tracking (CBT).
* **Automation:** Reduces human error and speeds up migration time.
* **Multi-Platform Support:** Supports both Linux and Windows Guest OS.
* **Customizable Mapping:** Map VMware networks/storage to CloudStack equivalents.
* **Scalability:** Move single VMs or batches of workloads.

---

## Design Principles
* **Agentless:** No agents required inside the VM guest.
* **Non-disruptive:** Primary VM remains online during initial stages.
* **Repeatable:** Delta syncs can be run multiple times before cutover.
* **Safe:** Original source VM is preserved until migration is verified.
* **Secure:** Data transfers occur over encrypted channels.

---

## Prerequisites

### VMware Environment
* VMware vCenter 6.7, 7.0, or 8.0.
* VMware ESXi hosts must be accessible.
* Changed Block Tracking (CBT) must be enabled on source VMs.
* A service account with administrative privileges for the API.

### Migration Host (RHEL/CentOS)
* A dedicated Linux host to orchestrate the migration.
* Minimum 8GB RAM and 4 vCPUs.
* Sufficient disk space to hold the VM disk images during conversion.
* Installed packages: `virt-v2v`, `libguestfs`, `qemu-img`, `python3`, `ansible`.

### Apache CloudStack
* Running CloudStack environment (v4.15+).
* API credentials (API Key and Secret Key).
* Configured Zone, Pod, Cluster, and Primary Storage.

---

## Python Dependencies
Install required Python libraries via pip:
```bash
pip install -r requirements.txt
```

## Repository Structure

The migration tool is structured as follows:
```text
vmware-to-cloudstack/
├── migrate.py              # Main script for base / delta / final sync
├── finalize_os.py          # Performs virt-v2v guest OS conversion
├── import_to_cloudstack.py # Handles CloudStack import via API
├── cloudstack_mapping.yaml # Configuration for mapping CloudStack resources
├── requirements.txt        # Python dependency list
└── lib/                    # Core library modules
    ├── vcenter.py          # VMware vCenter API integration
    ├── snapshot.py         # Snapshot management logic
    ├── cbt.py              # Changed Block Tracking (CBT) utilities
    ├── vm_disks.py         # Disk management utilities
    ├── vddk_reader.py      # VMware VDDK integration
    ├── nbd_writer.py       # Network Block Device writing
    ├── virt_v2v.py         # virt-v2v wrapper
    ├── cloudstack.py       # CloudStack API client
    ├── cloudstack_volume.py# Volume management for CloudStack
    ├── cloudstack_vm_import.py # VM registration logic
    └── metadata.py         # Migration state and metadata handling
```

## Migration Workflow
### Phase 1: Base Migration (No Downtime)
Copies the full disk(s) while the VM is still running.
```text
python migrate.py \
  --vcenter vc.example.com \
  --username administrator@vsphere.local \
  --password VMWARE_PASSWORD \
  --vm-name MyVM \
  --target-path /data/migrations/MyVM \
  --mode base
```
Actions performed:
* VM located by name
* All attached disks auto-discovered
* CBT enabled if not already enabled
* Snapshot created
* Full disk export to RAW format
* Migration metadata written
Resulting files:
```text
/data/migrations/MyVM/
├── disk0.raw
├── disk1.raw
└── migration.json
```


### Phase 2: Incremental Delta Sync (Repeatable)
Synchronizes only changed blocks using CBT.
```text
python migrate.py \
  --vcenter vc.example.com \
  --username administrator@vsphere.local \
  --password VMWARE_PASSWORD \
  --vm-name MyVM \
  --target-path /data/migrations/MyVM \
  --mode delta
```
* Uses CBT per disk
* Applies block-level deltas
* No downtime
* Can be run multiple times


### Phase 3: Final Sync (Short Downtime)
Performs the final synchronization during a planned downtime window.
```text
python migrate.py \
  --vcenter vc.example.com \
  --username administrator@vsphere.local \
  --password VMWARE_PASSWORD \
  --vm-name MyVM \
  --target-path /data/migrations/MyVM \
  --mode finalize
	• VM is powered off
	• Final CBT delta applied
	• RAW disks are now consistent
```


## Guest OS Conversion (virt-v2v)
After final sync, convert RAW disks to QCOW2 and prepare the guest OS.
```text
python finalize_os.py \
  --raw-disk /data/migrations/MyVM/disk0.raw \
  --output-dir /data/migrations/MyVM/final
```
This step:
* Converts RAW to QCOW2
* Injects virtio storage and network drivers
* Fixes bootloader (BIOS or UEFI)
* Removes VMware tools
Mandatory for Windows, strongly recommended for Linux.


## CloudStack Mapping Configuration
Edit cloudstack_mapping.yaml to map disks, OS type, and networks.
Example:
```text
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
```
Users map CloudStack constructs only — not VMware internals.


## Import VM into CloudStack
Deploy the VM using CloudMonkey:
```bash
python import_to_cloudstack.py
```
This will:
* Import QCOW2 disks as CloudStack volumes
* Deploy the VM
* Attach volumes and networks
* Start the VM

## Downtime Expectations
* Base sync: none
* Delta sync: none
* Final sync: required
* virt-v2v: offline (already powered off)
* VM boot: normal
Typical downtime: 1–5 minutes

## Supported Guest Operating Systems
### Linux
* RHEL / CentOS / Rocky / Alma
* Ubuntu / Debian
* SUSE
### Windows
* Windows Server 2012 R2 and later
* Windows Server 2016 / 2019 / 2022
* Windows 10 / 11 (UEFI)

## Best Practices
* Do not resize disks during migration
* Match BIOS vs UEFI firmware in CloudStack
* Use RAW disks during CBT synchronization
* Convert to QCOW2 only after final sync
* Expect first Windows boot to take longer
* Always test using snapshots or cloned VMs

## What This Project Is (and Is Not)
This project is:
* A near-zero downtime migration solution
* Block-level correct and safe
* CloudStack-native
* Suitable for product integration
This project is not:
* Live cross-hypervisor migration
* Filesystem-level replication

## Summary
This project combines VMware CBT, virt-v2v, and Apache CloudStack APIs to deliver a safe, scalable, near-zero-downtime VM migration workflow suitable for enterprise environments and future productization.

