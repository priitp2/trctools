import util

class CurrentStatement:
    def __init__(self, cursor, db):
        if len(cursor) < 2 and cursor != '#0':
            raise(BaseException("init: got empty cursor"))
        self.max_list_size = 10
        self.cursor = cursor
        self.parsing_in = None
        self.parse = None
        self.exec = None
        self.waits = []
        self.wait_count = 0
        self.fetches = []
        self.fetch_count = 0
        self.close = None
        self.__slots__ = ('max_list_size', 'cursor', 'parsing_in', 'parse', 'exec', 'waits', 'wait_count', 'fetches', 'fetch_count', 'close')
        self.db = db
    def add_parsing_in(self, params):
        if self.parsing_in:
            raise(BaseException("add_parsing_in: already set!"))
        else:
            self.parsing_in = params
    def add_parse(self, params):
        if self.parse:
            raise(BaseException("add_parse: already set!"))
        if self.cursor != params[0]:
            raise(BaseException("add_parse: got cursor {}, have: {}, params: {}".format(params[0], self.cursor, params)))
        self.parse = params
    def add_exec(self, params):
        if self.exec:
            raise(BaseException("add_exec: already set!"))
        if self.cursor != params[0]:
            raise(BaseException("add_exec: got cursor {}, have: {}".format(params[0], self.cursor)))
        self.exec = params
    def add_wait(self, params):
        if self.cursor != params[0]:
            raise(BaseException("add_wait: got cursor {}, have: {}".format(params[0], self.cursor)))
        self.wait_count += 1
        self.waits.append(params)
        if len(self.waits) > self.max_list_size:
            ret = util.merge_lat_objects((self.cursor, 0, 0), self.waits)
            self.waits = [ret]
    def add_fetch(self, params):
        if self.cursor != params[0]:
            raise(BaseException("add_exec: got cursor {}, have: {}".format(params[0], self.cursor)))
        self.fetch_count += 1
        self.fetches.append(params)
        if len(self.fetches) > self.max_list_size:
            ret = util.merge_lat_objects((self.cursor, 0, 0), self.fetches)
            self.fetches = [ret]
    def add_close(self, params):
        if self.cursor != params[0]:
            raise(BaseException("add_close: got cursor {}, have: {}".format(params[0], self.cursor)))
        if self.close:
            util.merge_lat_objects(self.close, params)
            #raise(BaseException("add_close: already set! "))
        else:
            self.close = params
    def merge(self):
        ret = (self.cursor, 0, 0)
        ret = util.merge_lat_objects(ret, self.parse)
        ret = util.merge_lat_objects(ret, self.exec)
        ret = util.merge_lat_objects(ret, self.waits)
        ret = util.merge_lat_objects(ret, self.fetches)
        ret = util.merge_lat_objects(ret, self.close)
        return ret
    def get_elapsed(self):
        start = None
        if self.parse:
            start = self.parse[4]['tim']
        elif self.exec:
            start = self.exec[4]['tim']
        if start and self.close:
            return int(self.close[3]['tim']) - int(start)
        else:
            return None 
    def dump_to_db(self):
        exec_id = self.db.get_exec_id()[0]
        st = []
        if self.parse:
            st.append(util.ops2tuple(exec_id, self.cursor, 'PARSE', self.parse[4]))
        if self.exec:
            st.append(util.ops2tuple(exec_id, self.cursor, 'EXEC', self.exec[4]))
        for f in self.fetches:
            # FIXME: why is f[3] missing?
            if len(f) > 3:
                st.append(util.ops2tuple(exec_id, self.cursor, 'FETCH', f[3]))
        #if self.close:
        #    st.append(util.ops2tuple_close(exec_id, 'CLOSE', self.close))
        if self.close:
            cl = util.ops2tuple(exec_id, self.cursor, 'CLOSE', self.close[3])
        else:
            cl = None
        self.db.insert_ops(st, cl)

