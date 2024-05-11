class DB:
    """Mock db for the tests"""
    def __init__(self):
        self.exec_id = 0
        self.batches = []
    def get_exec_id(self):
        self.exec_id += 1
        return self.exec_id
    def insert_single_ops(self, ops):
        self.insert_ops([ops])
    def insert_ops(self, ops):
        if len(ops) == 0:
            return
        self.batches += ops
    def flush(self):
        pass
