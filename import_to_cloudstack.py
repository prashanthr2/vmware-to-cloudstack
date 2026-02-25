import argparse
import os
import subprocess

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--raw-disk", required=True)
    p.add_argument("--output-dir", required=True)
    args = p.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    env = os.environ.copy()
    env["LIBGUESTFS_BACKEND"] = "direct"

    subprocess.check_call([
        "virt-v2v",
        "-i", "disk", args.raw_disk,
        "-o", "local",
        "-os", args.output_dir,
        "-of", "qcow2",
        "--root", "first"
    ], env=env)

if __name__ == "__main__":
    main()
