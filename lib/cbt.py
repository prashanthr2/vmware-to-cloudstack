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
            if task.info.state == "error":
                raise Exception(task.info.error)

    def get_changes(self, snapshot, device_key):
        """
        Returns list of changed blocks for a specific disk
        """
        res = self.vm.QueryChangedDiskAreas(
            snapshot=snapshot,
            deviceKey=device_key,
            startOffset=0
        )

        return [
            {"offset": a.start, "length": a.length}
            for a in res.changedArea
        ]
