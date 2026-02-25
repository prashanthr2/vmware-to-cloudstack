class VMImporter:
    def __init__(self, cs):
        self.cs = cs

    def deploy(self, vm_cfg, networks, volumes):
        vm = self.cs.run([
            "deploy", "virtualmachine",
            f"name={vm_cfg['name']}",
            f"zoneid={vm_cfg['zone']}",
            f"serviceofferingid={vm_cfg['service_offering']}",
            f"ostypeid={vm_cfg['os_type']}"
        ])["virtualmachine"]

        for v in volumes:
            self.cs.run([
                "attach", "volume",
                f"id={v['id']}",
                f"virtualmachineid={vm['id']}"
            ])
