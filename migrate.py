import argparse
import os

from lib.vcenter import VCenter
from lib.snapshot import SnapshotManager
from lib.cbt import CBTManager
from lib.vddk_reader import VDDKReader
from lib.raw_writer import RawWriter
from lib.metadata import MigrationMetadata
from lib.vm_disks import discover_vm_disks

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--vcenter", required=True)
    p.add_argument("--username", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--vm-name", required=True)
    p.add_argument("--target-path", required=True)
    p.add_argument("--mode", choices=["base", "delta", "finalize"], required=True)
    return p.parse_args()

def main():
    args = parse_args()
    os.makedirs(args.target_path, exist_ok=True)

    vc = VCenter(args.vcenter, args.username, args.password)
    vm = vc.get_vm_by_name(args.vm_name)
    if not vm:
        raise Exception(f"VM {args.vm_name} not found")

    snap_mgr = SnapshotManager(vc)
    cbt_mgr = CBTManager(vm)
    reader = VDDKReader()
    writer = RawWriter()
    meta = MigrationMetadata(args.target_path)

    # 🔑 NEW: auto-discover disks
    disks = discover_vm_disks(vm)

    cbt_mgr.ensure_cbt_enabled()

    if args.mode == "base":
        snap = snap_mgr.create(vm, "base-migration")

        for d in disks:
            raw_path = os.path.join(
                args.target_path, f"disk{d['index']}.raw"
            )

            reader.export_full_disk(
                d["vmdk_path"],
                raw_path
            )

        meta.data["base_snapshot"] = snap._moId
        meta.data["firmware"] = vm.config.firmware
        meta.data["disk_count"] = len(disks)
        meta.save()

    elif args.mode in ["delta", "finalize"]:
        if args.mode == "finalize":
            vc.power_off(vm)

        snap = snap_mgr.create(vm, f"{args.mode}-sync")

        for d in disks:
            raw_path = os.path.join(
                args.target_path, f"disk{d['index']}.raw"
            )

            changes = cbt_mgr.get_changes(
                snapshot=snap,
                device_key=d["device_key"]
            )

            for c in changes:
                tmp = reader.read_blocks(
                    d["vmdk_path"],
                    c["offset"],
                    c["length"]
                )
                writer.write(raw_path, tmp, c["offset"])
                os.unlink(tmp)

        meta.data["applied_snapshots"].append(snap._moId)
        meta.save()

if __name__ == "__main__":
    main()
