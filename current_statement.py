class CurrentStatement:
    """Tracks operations done within one database interaction."""
    def __init__(self, cursor, dbs, sql_id=None):
        self.__slots__ = ('cursor', 'parsing_in', 'parse', 'exec', 'waits', 'fetches',
                            'close', 'sql_id', 'dbs', 'stat')
        if len(cursor) < 2 and cursor != '#0':
            raise ValueError("init: got empty cursor")
        self.cursor = cursor
        # These calls are tracked as a client interaction(exec_id)
        self.known_ops = ('PIC', 'PARSE', 'EXEC', 'WAIT', 'FETCH', 'CLOSE', 'STAT', 'BINDS', 'ERROR')
        self.ops = {'PIC':None, 'PARSE':None, 'EXEC':None, 'WAIT':[], 'FETCH':[], 'CLOSE':None,
                'STAT':[], 'BINDS':None, 'ERROR':None}
        self.sql_id = sql_id
        self.dbs = dbs
    def is_not_empty(self):
        for ops in self.ops.items():
            if ops[1]:
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
        for ops in self.ops.values():
            if ops and isinstance(ops, list):
                if ops[0].op_type == op_type:
                    return len(ops)
            if ops and ops.op_type == op_type:
                return 1
        return 0
    def dump_to_db(self):
        """Turns ops into lists and adds to the database"""
        if not self.dbs:
            raise TypeError("dump_to_db: database not set!")
        exec_id = self.dbs.get_exec_id()

        for ops in self.ops.values():
            if ops:
                if isinstance(ops, list):
                    for listy_op in ops:
                        self.dbs.insert_ops(listy_op.to_list(exec_id, self.sql_id))
                    continue
                self.dbs.insert_ops(ops.to_list(exec_id, self.sql_id))
