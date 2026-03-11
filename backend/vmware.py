from __future__ import annotations

import os
import ssl
from collections.abc import Generator
from typing import Optional

try:
    from pyVim.connect import Disconnect, SmartConnect
    from pyVmomi import vim
except ImportError:  # pragma: no cover - handled at runtime
    Disconnect = None
    SmartConnect = None
    vim = None


def _parse_bool(raw: Optional[str], default: bool) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


class VMwareClient:
    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        port: int = 443,
        verify_ssl: bool = False,
    ) -> None:
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.verify_ssl = verify_ssl

    @classmethod
    def from_sources(cls, config: Optional[dict] = None) -> "VMwareClient":
        cfg = (config or {}).get("vcenter", {})

        host = os.getenv("VMWARE_HOST", cfg.get("host", ""))
        username = os.getenv("VMWARE_USER", cfg.get("user", ""))
        password = os.getenv("VMWARE_PASSWORD", cfg.get("password", ""))

        cfg_port = cfg.get("port", 443)
        port = int(os.getenv("VMWARE_PORT", str(cfg_port)))

        cfg_verify_ssl = cfg.get("verify_ssl")
        if cfg_verify_ssl is None and "insecure" in cfg:
            cfg_verify_ssl = not bool(cfg.get("insecure"))

        verify_default = bool(cfg_verify_ssl) if cfg_verify_ssl is not None else False
        verify_ssl = _parse_bool(os.getenv("VMWARE_VERIFY_SSL"), verify_default)

        return cls(
            host=host,
            username=username,
            password=password,
            port=port,
            verify_ssl=verify_ssl,
        )

    def _validate_config(self) -> None:
        if SmartConnect is None or vim is None:
            raise RuntimeError("pyVmomi is not installed. Install pyvmomi to use VMware endpoints.")

        if not self.host or not self.username or not self.password:
            raise ValueError(
                "VMware credentials are missing. Configure vcenter in config.yaml or set VMWARE_HOST/VMWARE_USER/VMWARE_PASSWORD."
            )

    def _connect(self):
        context = None
        if not self.verify_ssl:
            context = ssl._create_unverified_context()

        return SmartConnect(
            host=self.host,
            user=self.username,
            pwd=self.password,
            port=self.port,
            sslContext=context,
        )

    def _iter_vms(self, content) -> Generator:
        view = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
        try:
            for vm in view.view:
                yield vm
        finally:
            view.Destroy()

    def list_vms(self) -> list[dict]:
        self._validate_config()

        si = self._connect()
        try:
            content = si.RetrieveContent()
            result = []

            for vm in self._iter_vms(content):
                config = vm.config
                if config is None:
                    continue

                disks = []
                for device in config.hardware.device:
                    if isinstance(device, vim.vm.device.VirtualDisk):
                        datastore_name = None
                        backing = getattr(device, "backing", None)
                        datastore = getattr(backing, "datastore", None)
                        if datastore is not None:
                            datastore_name = datastore.name

                        disks.append(
                            {
                                "label": device.deviceInfo.label if device.deviceInfo else "Virtual Disk",
                                "size_gb": round(device.capacityInKB / (1024 * 1024), 2),
                                "datastore": datastore_name,
                            }
                        )

                datastores = [ds.name for ds in getattr(vm, "datastore", []) if getattr(ds, "name", None)]

                result.append(
                    {
                        "name": vm.name,
                        "moref": vm._moId,
                        "cpu": int(config.hardware.numCPU),
                        "memory": int(config.hardware.memoryMB),
                        "disks": disks,
                        "datastore": datastores,
                    }
                )

            return result
        finally:
            Disconnect(si)
