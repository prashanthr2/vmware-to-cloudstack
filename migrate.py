import argparse
import os
from lib.vcenter import VCenter
from lib.snapshot import SnapshotManager
from lib.cbt import CBTManager
from lib.vddk_reader import VDDKReader
from lib.raw_writer import RawWriter
from lib.metadata import MigrationMetadata

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--vcenter", required=True)
    p.add_argument("--username", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--vm-name", required=True)
    p.add_argument("--vmdk-path", required=True)
    p.add_argument("--target-path", required=True)
    p.add_argument("--mode", choices=["base", "delta", "finalize"], required=True)
    return p.parse_args()

def main():
    args = parse_args()
    os.makedirs(args.target_path, exist_ok=True)

    vc = VCenter(args.vcenter, args.username, args.password)
    vm = vc.get_vm_by_name(args.vm_name)
    if not vm:
        raise Exception("VM not found")

    snap_mgr = SnapshotManager(vc)
    cbt_mgr = CBTManager(vm)
    reader = VDDKReader()
    writer = RawWriter()
    meta = MigrationMetadata(args.target_path)

    cbt_mgr.ensure_cbt_enabled()

    raw_disk = os.path.join(args.target_path, "disk0.raw")

    if args.mode == "base":
        snap = snap_mgr.create(vm, "base-migration")
        reader.export_full_disk(args.vmdk_path, raw_disk)
        meta.data["base_snapshot"] = snap._moId
        meta.data["firmware"] = vm.config.firmware
        meta.save()

    elif args.mode in ["delta", "finalize"]:
        if args.mode == "finalize":
            vc.power_off(vm)

        snap = snap_mgr.create(vm, f"{args.mode}-sync")
        changes = cbt_mgr.get_changes(snap)

        for c in changes:
            tmp = reader.read_blocks(
                args.vmdk_path,
                c["offset"],
                c["length"]
            )
            writer.write(raw_disk, tmp, c["offset"])
            os.unlink(tmp)

        meta.data["applied_snapshots"].append(snap._moId)
        meta.save()

if __name__ == "__main__":
    main()
