class Backend:
    """Mock backend for the tests"""
    def __init__(self):
        self.span_id = 0
        self.batches = []
    def get_span_id(self):
        self.span_id += 1
        return self.span_id
    def add_ops(self, span_id, cursor, ops):
        self.batches += [o.astuple(span_id, cursor) for o in ops]
    def insert_single_ops(self, ops):
        self.insert_ops([ops])
    def insert_ops(self, ops):
        if len(ops) == 0:
            return
        self.batches += ops
    def flush(self):
        pass
