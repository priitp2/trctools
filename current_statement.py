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
        if self.parsing_in:
            return True
        if self.parse:
            return True
        if self.exec:
            return True
        if len(self.waits) > 0:
            return True
        if len(self.fetches) > 0:
            return True
        if self.close:
            return True
        if len(self.stat) > 0:
            return True
        return False

    # FIXME: merge add_parsing_in and add_pic
    def add_ops(self, ops):
        if self.cursor != ops.cursor:
            raise KeyError(f"add_ops: wrong cursor, got {ops.cursor}, have {self.cursor}")
        if ops.op_type not in self.known_ops:
            raise KeyError(f"add_ops: unknown ops type: {ops.op_type}")
        if self.ops[ops.op_type] and ops.op_type not in ('WAIT', 'FETCH', 'STAT'):
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
            if self.ops[op] and self.ops[op].op_type == op_type:
                count += 1
        return count
    def add_parsing_in(self, params):
        if self.parsing_in:
            raise ValueError("add_parsing_in: already set!")
        self.parsing_in = params
    def add_pic(self, ops):
        if ops.op_type != 'PIC':
            raise TypeError(f"add_parse: wrong op_type = {ops.op_type}")
        if self.pic:
            raise ValueError("add_parse: already set!")
        if self.cursor != ops.cursor:
            raise ValueError(f"add_parse: got cursor {ops.cursor}, have: {self.cursor}," \
                                + f" params: {ops}")
        self.pic = ops
    def add_parse(self, ops):
        if ops.op_type != 'PARSE':
            raise TypeError(f"add_parse: wrong op_type = {ops.op_type}")
        if self.parse:
            raise ValueError("add_parse: already set!")
        if self.cursor != ops.cursor:
            raise ValueError(f"add_parse: got cursor {ops.cursor}, have: {self.cursor}," \
                                + f" params: {ops}")
        self.parse = ops
    def add_exec(self, ops):
        if ops.op_type != 'EXEC':
            raise TypeError(f"add_exec: wrong op_type = {ops.op_type}")
        if self.exec:
            raise ValueError("add_exec: already set!")
        if self.cursor != ops.cursor:
            raise ValueError(f"add_exec: got cursor {ops.cursor}, have: {self.cursor}")
        self.exec = ops
    def add_wait(self, ops):
        if ops.op_type != 'WAIT':
            raise TypeError(f"add_wait: wrong op_type = {ops.op_type}")
        if self.cursor != ops.cursor:
            raise ValueError(f"add_wait: got cursor {ops.cursor}, have: {self.cursor}")
        self.waits.append(ops)
    def add_fetch(self, ops):
        if ops.op_type != 'FETCH':
            raise TypeError(f"add_fetch: wrong op_type = {ops.op_type}")
        if self.cursor != ops.cursor:
            raise ValueError(f"add_fetch: got cursor {ops.cursor}, have: {self.cursor}")
        self.fetches.append(ops)
    def add_close(self, ops):
        if ops.op_type != 'CLOSE':
            raise TypeError(f"add_close: wrong op_type = {ops.op_type}")
        if self.cursor != ops.cursor:
            raise ValueError(f"add_close: got cursor {ops.cursor}, have: {self.cursor}")
        if self.close:
            #FIXME: why is this needed?
            self.close = self.close.merge(ops)
            #raise(BaseException("add_close: already set! "))
        else:
            self.close = ops
    def add_stat(self, ops):
        if ops.op_type != 'STAT':
            raise TypeError(f"add_stat: wrong op_type = {ops.op_type}")
        if self.cursor != ops.cursor:
            raise ValueError(f"add_stat: got cursor {ops.cursor}, have: {self.cursor}")
        self.stat.append(ops)
    def add_binds(self, ops):
        if ops.op_type != 'BINDS':
            raise TypeError(f"add_stat: wrong op_type = {ops.op_type}")
        if self.cursor != ops.cursor:
            raise ValueError(f"add_stat: got cursor {ops.cursor}, have: {self.cursor}")
        self.binds = ops
    def dump_to_db(self):
        if not self.db:
            raise TypeError("dump_to_db: database not set!")
        exec_id = self.db.get_exec_id()

        for op_type in self.ops:
            if self.ops[op_type]:
                if op_type in ('WAIT', 'FETCH', 'STAT'):
                    for listy_op in self.ops[op_type]:
                        self.db.insert_ops(listy_op.to_list(exec_id, self.sql_id))
                    continue
                self.db.insert_ops(self.ops[op_type].to_list(exec_id, self.sql_id))

        if self.parse:
            self.db.insert_ops(self.parse.to_list(exec_id, self.sql_id))
        if self.exec:
            self.db.insert_ops(self.exec.to_list(exec_id, self.sql_id))
        for fetch in self.fetches:
            self.db.insert_ops(fetch.to_list(exec_id, self.sql_id))
        for wait in self.waits:
            self.db.insert_ops(wait.to_list(exec_id, self.sql_id))
        if self.close:
            self.db.insert_ops(self.close.to_list(exec_id, self.sql_id))
        for s in self.stat:
            self.db.insert_ops(s.to_list(exec_id, self.sql_id))
        if self.binds:
            self.db.insert_ops(self.binds.to_list(exec_id, self.sql_id))
        if self.pic:
            self.db.insert_ops(self.pic.to_list(exec_id, self.sql_id))
