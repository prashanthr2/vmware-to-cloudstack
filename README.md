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
pip install -r requirements.txt```

---

# Repository Structure

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
