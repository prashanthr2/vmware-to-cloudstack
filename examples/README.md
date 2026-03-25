# Example Pack

This folder contains operator-facing examples for the Go migration engine.

## Config files

- `config.full.example.yaml`
  - Fully commented config with all supported fields.
  - Comments mark each field as `MANDATORY` or `OPTIONAL`.
  - Comments also describe runtime/default behavior.

## Run spec files

- `spec.run.single-vm.single-disk.single-nic.yaml`
  - One VM.
  - One boot disk only.
  - One NIC mapping.

- `spec.run.single-vm.multi-disk.multi-nic.yaml`
  - One VM.
  - Boot disk plus multiple data disks.
  - Multiple NIC mappings.

- `spec.run.single-vm.defaults-only.yaml`
  - One VM.
  - Uses `config.yaml -> cloudstack_defaults` and other config defaults.
  - Minimal operator input.

- `spec.run.multi-vm.single-disk.single-nic.yaml`
  - Multiple VMs.
  - Each VM uses one boot disk and one NIC.

- `spec.run.multi-vm.multi-disk.multi-nic.yaml`
  - Multiple VMs.
  - Each VM demonstrates data disk mapping and multi-NIC mapping.

- `spec.run.multi-vm.defaults-only.yaml`
  - Multiple VMs.
  - Uses config defaults for target fields and migration defaults.

## Important default behavior

- `run` mode applies defaults from `config.yaml -> cloudstack_defaults` to:
  - `target.cloudstack.zoneid`
  - `target.cloudstack.clusterid`
  - `target.cloudstack.storageid`
  - `target.cloudstack.networkid`
  - `target.cloudstack.serviceofferingid`
  - `target.cloudstack.diskofferingid`

- For data disks, `diskofferingid` resolves in this order:
  - `disks.<unit>.diskofferingid`
  - `target.cloudstack.diskofferingid`
  - `config.yaml -> cloudstack_defaults.diskofferingid`

- For data disk storage, `storageid` resolves in this order:
  - `disks.<unit>.storageid`
  - `target.cloudstack.storageid`
  - `config.yaml -> cloudstack_defaults.storageid`

- If `nic_mappings` is present:
  - `Network adapter 1` / `source_index: 0` is used for `importVm`
  - additional NICs are attached after VM creation

- If `nic_mappings` is omitted:
  - engine falls back to `target.cloudstack.networkid`
  - if that is missing in spec, `config.yaml -> cloudstack_defaults.networkid` is used

