class CurrentStatement:
    def __init__(self, cursor, db, sql_id=None):
        self.__slots__ = ('cursor', 'parsing_in', 'parse', 'exec', 'waits', 'fetches',
                            'close', 'sql_id', 'db', 'stat')
        if len(cursor) < 2 and cursor != '#0':
            raise ValueError("init: got empty cursor")
        self.cursor = cursor
        self.known_ops = ('PIC', 'PARSE', 'EXEC', 'WAIT', 'FETCH', 'CLOSE', 'STAT', 'BINDS')
        self.ops = {'PIC':None, 'PARSE':None, 'EXEC':None, 'WAIT':[], 'FETCH':[], 'CLOSE':None, 'STAT':[], 'BINDS':None}
        self.parsing_in = None
        self.parse = None
        self.exec = None
        self.waits = []
        self.fetches = []
        self.close = None
        self.sql_id = sql_id
        self.db = db
        self.stat = []
        self.binds = None
        self.pic = None
    def is_not_empty(self):
        for op in self.ops:
            if self.ops[op]:
                return True
        return False

    def add_ops(self, ops):
        if self.cursor != ops.cursor:
            raise KeyError(f"add_ops: wrong cursor, got {ops.cursor}, have {self.cursor}")
        if ops.op_type not in self.known_ops:
            raise KeyError(f"add_ops: unknown ops type: {ops.op_type}")
        if self.ops[ops.op_type] and not isinstance(self.ops[ops.op_type], list):
            raise KeyError(f"add_ops: already set: ops {ops.op_type}")
        if ops.op_type in ('WAIT', 'FETCH', 'STAT'):
            self.ops[ops.op_type].append(ops)
            return
        self.ops[ops.op_type] = ops
    def is_set(self, op_type):
        if self.ops[op_type]:
            return True
        return False
    def count_ops(self, op_type):
        """Counts number of (listy) ops. Useful for tests."""
        count = 0
        for op in self.ops:
            if op == op_type and self.ops[op]:
                if isinstance(self.ops[op], list):
                    return len(self.ops[op])
                return 1
        return count
    def dump_to_db(self):
        if not self.db:
            raise TypeError("dump_to_db: database not set!")
        exec_id = self.db.get_exec_id()

        for op_type in self.ops:
            if self.ops[op_type]:
                if isinstance(self.ops[op_type], list):
                    for listy_op in self.ops[op_type]:
                        self.db.insert_ops(listy_op.to_list(exec_id, self.sql_id))
                    continue
                self.db.insert_ops(self.ops[op_type].to_list(exec_id, self.sql_id))
