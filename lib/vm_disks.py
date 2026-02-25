from pyVmomi import vim

def discover_vm_disks(vm):
    """
    Returns ordered list of VM disks with metadata:
    [
      {
        "index": 0,
        "device_key": 2000,
        "vmdk_path": "[datastore1] vm/vm.vmdk",
        "capacity": 53687091200
      }
    ]
    """
    disks = []

    for dev in vm.config.hardware.device:
        if isinstance(dev, vim.vm.device.VirtualDisk):
            disks.append({
                "device_key": dev.key,
                "vmdk_path": dev.backing.fileName,
                "capacity": dev.capacityInBytes
            })

    # Stable ordering: controller + unit number
    disks.sort(key=lambda d: d["device_key"])

    # Assign deterministic disk index
    for idx, d in enumerate(disks):
        d["index"] = idx

    return disks
