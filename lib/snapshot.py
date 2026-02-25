class SnapshotManager:
    def __init__(self, vc):
        self.vc = vc

    def create(self, vm, name):
        task = vm.CreateSnapshot_Task(
            name=name,
            description="Migration snapshot",
            memory=False,
            quiesce=True
        )
        self.vc._wait(task)
        return task.info.result
