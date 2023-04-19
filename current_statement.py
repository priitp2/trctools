import util

class CurrentStatement:
    def __init__(self, cursor):
        if len(cursor) < 2 and cursor != '#0':
            raise(BaseException("init: got empty cursor"))
        self.cursor = cursor
        self.parsing_in = None
        self.parse = None
        self.exec = None
        self.waits = []
        self.fetches = []
        self.close = None
    def add_parsing_in(self, params):
        if self.parsing_in != None:
            raise(BaseException("add_parsing_in: already set!"))
        else:
            self.parsing_in = params
    def add_parse(self, params):
        if self.parse != None:
            raise(BaseException("add_parse: already set!"))
        if self.cursor != params[0]:
            raise(BaseException("add_parse: got cursor {}, have: {}, params: {}".format(params[0], self.cursor, params)))
        self.parse = params
    def add_exec(self, params):
        if self.exec != None:
            raise(BaseException("add_exec: already set!"))
        if self.cursor != params[0]:
            raise(BaseException("add_exec: got cursor {}, have: {}".format(params[0], self.cursor)))
        self.exec = params
    def add_wait(self, params):
        if self.cursor != params[0]:
            raise(BaseException("add_wait: got cursor {}, have: {}".format(params[0], self.cursor)))
        self.waits.append(params)
    def add_fetch(self, params):
        if self.cursor != params[0]:
            raise(BaseException("add_exec: got cursor {}, have: {}".format(params[0], self.cursor)))
        self.fetches.append(params)
    def add_close(self, params):
        if self.cursor != params[0]:
            raise(BaseException("add_close: got cursor {}, have: {}".format(params[0], self.cursor)))
        if self.close != None:
            util.merge_lat_objects(self.close, params)
            #raise(BaseException("add_close: already set! "))
        else:
            self.close = params
    def merge(self):
        ret = (self.cursor, 0, 0)
        ret = util.merge_lat_objects(ret, self.parse)
        ret = util.merge_lat_objects(ret, self.exec)
        #util.merge_lat_objects(ret, self.waits)
        util.merge_lat_objects(ret, self.fetches)
        ret = util.merge_lat_objects(ret, self.close)
        return ret
