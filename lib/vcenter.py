import ssl
from pyVim.connect import SmartConnect
from pyVmomi import vim

class VCenter:
    def __init__(self, host, user, password):
        ctx = ssl._create_unverified_context()
        self.si = SmartConnect(host=host, user=user, pwd=password, sslContext=ctx)
        self.content = self.si.RetrieveContent()

    def get_vm_by_name(self, name):
        for dc in self.content.rootFolder.childEntity:
            for vm in dc.vmFolder.childEntity:
                if vm.name == name:
                    return vm
        return None

    def power_off(self, vm):
        if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
            task = vm.PowerOffVM_Task()
            self._wait(task)

    def _wait(self, task):
        while task.info.state not in ["success", "error"]:
            pass
        if task.info.state == "error":
            raise Exception(task.info.error)
