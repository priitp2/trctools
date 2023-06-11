class DB:
    def __init__(self):
        self.exec_id = 0
        self.batches = []
        self.cursor_statement_batches = []
    def add_rows(self, sql_id, rt):
        pass
    def add_cursor(self, c):
        pass
    def get_exec_id(self):
        self.exec_id += 1
        return self.exec_id
    def insert_ops(self, ops):
        if len(ops) == 0:
            return
        self.batches.append(ops)
    def insert_cursors(self, cs):
        self.cursor_statement_batches.append(ops)
    def flush(self, fname):
        pass

