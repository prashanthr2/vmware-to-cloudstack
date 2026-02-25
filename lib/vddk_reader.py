import subprocess
import tempfile

class VDDKReader:
    def export_full_disk(self, vmdk, output):
        subprocess.check_call([
            "/opt/vmware/vddk/bin/vixDiskLibSample",
            "-read", vmdk, output
        ])

    def read_blocks(self, vmdk, offset, length):
        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()
        subprocess.check_call([
            "/opt/vmware/vddk/bin/vixDiskLibSample",
            "-read-range",
            vmdk,
            str(offset),
            str(length),
            tmp.name
        ])
        return tmp.name
