class VolumeManager:
    def __init__(self, cs):
        self.cs = cs

    def import_volume(self, name, path, offering):
        return self.cs.run([
            "import", "volume",
            f"name={name}",
            f"path={path}",
            f"diskoffering={offering}",
            "format=QCOW2"
        ])["volume"]
