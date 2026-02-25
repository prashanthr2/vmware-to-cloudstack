import json
import os

class MigrationMetadata:
    def __init__(self, path):
        self.file = os.path.join(path, "migration.json")
        self.data = self._load()

    def _load(self):
        if os.path.exists(self.file):
            return json.load(open(self.file))
        return {
            "base_snapshot": None,
            "applied_snapshots": [],
            "firmware": None
        }

    def save(self):
        json.dump(self.data, open(self.file, "w"), indent=2)
