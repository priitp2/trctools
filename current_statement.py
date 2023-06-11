import util
from ops import Ops

class CurrentStatement:
    def __init__(self, cursor, db, sql_id=None):
        if len(cursor) < 2 and cursor != '#0':
            raise(BaseException("init: got empty cursor"))
        self.cursor = cursor
        self.parsing_in = None
        self.parse = None
        self.exec = None
        self.waits = []
        self.fetches = []
        self.fetch_count = 0
        self.close = None
        self.sql_id = sql_id
        self.__slots__ = ('max_list_size', 'cursor', 'parsing_in', 'parse', 'exec', 'waits', 'fetches', 'fetch_count', 'close')
        self.db = db
        self.stat = []
    def add_parsing_in(self, params):
        if self.parsing_in:
            raise(BaseException("add_parsing_in: already set!"))
        else:
            self.parsing_in = params
    def add_parse(self, ops):
        if ops.op_type != 'PARSE':
            raise(BaseException("add_parse: wrong op_type = {}".format(ops.op_type)))
        if self.parse:
            raise(BaseException("add_parse: already set!"))
        if self.cursor != ops.cursor:
            raise(BaseException("add_parse: got cursor {}, have: {}, params: {}".format(ops.cursor, self.cursor, ops)))
        self.parse = ops
    def add_exec(self, ops):
        if ops.op_type != 'EXEC':
            raise(BaseException("add_exec: wrong op_type = {}".format(ops.op_type)))
        if self.exec:
            raise(BaseException("add_exec: already set!"))
        if self.cursor != ops.cursor:
            raise(BaseException("add_exec: got cursor {}, have: {}".format(ops.cursor, self.cursor)))
        self.exec = ops
    def add_wait(self, ops):
        if ops.op_type != 'WAIT':
            raise(BaseException("add_wait: wrong op_type = {}".format(ops.op_type)))
        if self.cursor != ops.cursor:
            raise(BaseException("add_wait: got cursor {}, have: {}".format(ops.cursor, self.cursor)))
        self.waits.append(ops)
    def add_fetch(self, ops):
        if ops.op_type != 'FETCH':
            raise(BaseException("add_fetch: wrong op_type = {}".format(ops.op_type)))
        if self.cursor != ops.cursor:
            raise(BaseException("add_fetch: got cursor {}, have: {}".format(ops.cursor, self.cursor)))
        self.fetch_count += 1
        self.fetches.append(ops)
    def add_close(self, ops):
        if ops.op_type != 'CLOSE':
            raise(BaseException("add_close: wrong op_type = {}".format(ops.op_type)))
        if self.cursor != ops.cursor:
            raise(BaseException("add_close: got cursor {}, have: {}".format(ops.cursor, self.cursor)))
        if self.close:
            #FIXME: why is this needed?
            self.close = self.close.merge(ops)
            #raise(BaseException("add_close: already set! "))
        else:
            self.close = ops
    def add_stat(self, ops):
        if ops.op_type != 'STAT':
            raise(BaseException("add_stat: wrong op_type = {}".format(ops.op_type)))
        if self.cursor != ops.cursor:
            raise(BaseException("add_stat: got cursor {}, have: {}".format(ops.cursor, self.cursor)))
        self.stat.append(ops)
    def merge(self):
        ret = Ops('EXEC', self.cursor, '', '', 0)
        ret = ret.merge(self.parse)
        ret = ret.merge(self.exec)
        ret = ret.merge(self.waits)
        ret = ret.merge(self.fetches)
        ret = ret.merge(self.close)
        return ret
    def get_elapsed(self):
        start = None
        if self.parse:
            start = self.parse.tim
        elif self.exec:
            start = self.exec.tim
        if start and self.close:
            return self.close.tim - start
        else:
            return None 
    def dump_to_db(self):
        if not self.db:
            raise BaseException("dump_to_db: database not set!")
        exec_id = self.db.get_exec_id()

        if self.parse:
            self.db.insert_ops(self.parse.to_list(exec_id, self.sql_id))
        if self.exec:
            self.db.insert_ops(self.exec.to_list(exec_id, self.sql_id))
        for f in self.fetches:
            self.db.insert_ops(f.to_list(exec_id, self.sql_id))
        for w in self.waits:
            self.db.insert_ops(w.to_list(exec_id, self.sql_id))
        if self.close:
            self.db.insert_ops(self.close.to_list(exec_id, self.sql_id))
        for s in self.stat:
            self.db.insert_ops(s.to_list(exec_id, self.sql_id))
