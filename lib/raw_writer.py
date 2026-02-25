import os

class RawWriter:
    def write(self, raw_disk, delta_file, offset):
        with open(raw_disk, "r+b") as r, open(delta_file, "rb") as d:
            r.seek(offset)
            r.write(d.read())
            r.flush()
            os.fsync(r.fileno())
