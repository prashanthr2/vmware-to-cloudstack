from pyVmomi import vim

class CBTManager:
    def __init__(self, vm):
        self.vm = vm

    def ensure_cbt_enabled(self):
        if not self.vm.config.changeTrackingEnabled:
            spec = vim.vm.ConfigSpec()
            spec.changeTrackingEnabled = True
            task = self.vm.ReconfigVM_Task(spec)
            while task.info.state not in ["success", "error"]:
                pass

    def get_changes(self, snapshot):
        changes = []
        for dev in self.vm.config.hardware.device:
            if isinstance(dev, vim.vm.device.VirtualDisk):
                res = self.vm.QueryChangedDiskAreas(
                    snapshot=snapshot,
                    deviceKey=dev.key,
                    startOffset=0
                )
                for a in res.changedArea:
                    changes.append({"offset": a.start, "length": a.length})
        return changes
