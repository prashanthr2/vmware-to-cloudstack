import subprocess
import json

class CloudStack:
    def run(self, cmd):
        out = subprocess.check_output(["cmk"] + cmd + ["-o", "json"], text=True)
        return json.loads(out)
