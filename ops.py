import re

__doc__ = """
    Contains classes representing the various operations/lines in the trace file.
    Some classes handle multiple operations, for brevity.
"""

wait_matcher = re.compile(r""" nam='(.*)' ela= (\d+) (.*) tim=(\d+)""")

def ops_factory(op_type, cursor, params, fmeta, ts_callback, name=None, ts2=None):
    """
        Factory method for operations.
    """
    ops = None
    match op_type:
        case 'WAIT':
            ops = Wait(op_type, cursor, params, fmeta, ts_callback)
        case 'STAT':
            ops = Stat(op_type, cursor, params, fmeta, ts_callback)
        case 'BINDS':
            ops = Binds(op_type, cursor, params, fmeta, ts_callback)
        case 'STAR' | 'HEADER':
            ops = Meta(op_type, cursor, params, fmeta, name, ts2)
        case 'XCTEND':
            ops = Xctend(op_type, cursor, params, fmeta, ts_callback)
        case 'PIC':
            ops = Pic(op_type, cursor, params, fmeta, ts_callback)
        case 'PARSE' | 'EXEC' | 'CLOSE' | 'FETCH':
            ops = Exec(op_type, cursor, params, fmeta, ts_callback)
        case 'ERROR':
            ops = Error(op_type, cursor, params, fmeta, ts_callback)
        case _ if op_type.startswith('LOB'):
            ops = Lob(op_type, cursor, params, fmeta, ts_callback)
        case _:
            raise AttributeError(f"Wrong op_type: {op_type}")
    return ops

class Ops:
    """
        Base class for various operations.
    """
    def __init__(self, op_type, cursor, fmeta, ts_callback):
        self.op_type = op_type
        self.cursor = cursor
        self.fname = fmeta['FILE_NAME']
        self.line = fmeta['LINE_COUNT']
        self.ts_callback = ts_callback
        self.fmeta = fmeta
    def __getattr__(self, name):
        """ In case of missing attribute returns 0 if attribute is in __slots__. This is needed in
            to_list(). """
        if name in self.__slots__:
            return 0
        raise AttributeError(f"Wrong attribute: {name}")
    def to_list(self, exec_id, sql_id):
        """ Generates list that is used to persist Ops in the database. Children are supposed to 
            override this."""
        return [exec_id, sql_id, None, self.op_type, None, None, None, None,
                    None, None, None, None, None, None, None, None, None,
                    None, None, None, None, None, None, None, None, None,
                    None, None, None, None, None, None, None, None, None,
                    None, None, None, None]
    def to_dict(self, exec_id, sql_id):
        out = {}
        out['sql_id'] = sql_id
        out['exec_id'] = exec_id
        if self.op_type == 'PIC':
            out['op_type'] = 'PARSING IN CURSOR'
        else:
            out['op_type'] = self.op_type
        out['cursor'] = self.cursor
        for i in self.__dict__.keys():
            if i in self.__slots__:
                if i == 'tim':
                    out['ts'] = self.ts_callback(self.__dict__[i])
                out[i] = self.__dict__[i]
        return out | {k:v  for (k, v) in self.fmeta.items() if v }
    def __str__(self):
        return ''

class Wait(Ops):
    """ Handles WAIT lines. Wait event name is parsed out, everything else is persisted as-is."""
    def __init__(self, op_type, cursor, params, fmeta, ts_callback):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        self.__dict__['raw'] = params
        match = wait_matcher.match(params)
        if match:
            self.__dict__['name'] = match.group(1).strip("'")
            self.__dict__['e'] = int(match.group(2))

            self.__dict__['tim'] = int(match.group(4))
            self.__slots__ = (op_type, cursor, 'raw', 'name', 'e', 'tim')
    def to_list(self, exec_id, sql_id):
        return [exec_id, sql_id, self.cursor, self.op_type, None, self.e, None, None,
                    None, None, None, None, None, None, self.tim, None, self.name,
                    self.raw, self.fname, self.line, self.ts_callback(self.tim), None,
                    None, None, None, None, None, None, None, None, None,
                    self.fmeta['SESSION ID'], self.fmeta['CLIENT ID'], self.fmeta['SERVICE NAME'],
                    self.fmeta['MODULE NAME'], self.fmeta['ACTION NAME'],
                    self.fmeta['CONTAINER ID'], None]
    def __str__(self):
        return f"{self.cursor}: {self.op_type} {self.raw}"

class Stat(Ops):
    """ Execuion plan and statistics(STAT). Persisted as-is."""
    def __init__(self, op_type, cursor, params, fmeta, ts_callback):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        self.__dict__['raw'] = params
        self.__slots__ = (op_type, cursor, 'raw')
    def to_list(self, exec_id, sql_id):
        return [exec_id, sql_id, self.cursor, self.op_type, None, None, None, None, None,
                    None, None, None, None, None, None, None, None, self.raw, self.fname,
                    self.line, self.ts_callback(None), None, None, None, None, None, None,
                    None, None, None, None,
                    self.fmeta['SESSION ID'], self.fmeta['CLIENT ID'], self.fmeta['SERVICE NAME'],
                    self.fmeta['MODULE NAME'], self.fmeta['ACTION NAME'],
                    self.fmeta['CONTAINER ID'], None]
    def __str__(self):
        return f"{self.cursor}: {self.op_type} {self.raw}"

class Meta(Ops):
    """ Handles trace file header lines and lines that start with stars (***). These lines contain
        wall clock readings, these are persisted in ts2."""
    def __init__(self, op_type, cursor, params, fmeta, name, ts2):
        super().__init__(op_type, cursor, fmeta, None)
        self.__dict__['name'] = name
        self.__dict__['raw'] = params
        self.__dict__['ts2'] = ts2
        self.__slots__ = (op_type, cursor, 'raw', 'name', 'ts2')
    def to_list(self, exec_id, sql_id):
        return [exec_id, sql_id, self.cursor, self.op_type, None, None, None, None, None,
                    None, None, None, None, None, None, None, self.name, self.raw, self.fname,
                    self.line, self.ts2, None, None, None, None, None, None, None, None,
                    None, None, self.fmeta['SESSION ID'], self.fmeta['CLIENT ID'],
                    self.fmeta['SERVICE NAME'], self.fmeta['MODULE NAME'],
                    self.fmeta['ACTION NAME'], self.fmeta['CONTAINER ID'], None]
    def __str__(self):
        if self.op_type == 'HEADER':
            return f"{self.name}: {self.raw}"
        return f"*** {self.name}:({self.raw}) {self.ts2}"

class Binds(Ops):
    """ Bind values. Everything is persisted as-is, in one string."""
    def __init__(self, op_type, cursor, params, fmeta, ts_callback):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        self.__dict__['raw'] = params
        self.__slots__ = (op_type, cursor, 'raw')
    def to_list(self, exec_id, sql_id):
        return [exec_id, sql_id, self.cursor, self.op_type, None, None, None, None, None,
                    None, None, None, None, None, None, None, None, "".join(self.raw), self.fname,
                    self.line, self.ts_callback(None), None, None, None, None, None, None, None,
                    None, None, None,
                    self.fmeta['SESSION ID'], self.fmeta['CLIENT ID'], self.fmeta['SERVICE NAME'],
                    self.fmeta['MODULE NAME'], self.fmeta['ACTION NAME'],
                    self.fmeta['CONTAINER ID'], None]
    def __str__(self):
        return f"{self.cursor}: {self.op_type} {self.raw}"

class Xctend(Ops):
    """ Commits (XCTEND)."""
    def __init__(self, op_type, cursor, params, fmeta, ts_callback):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        for item in params.split(', '):
            key = item.split('=')
            self.__dict__[key[0]] = int(key[1])
        self.__slots__ = (op_type, cursor, 'rlbk', 'rd_only', 'tim')
    def to_list(self, exec_id, sql_id):
        return [exec_id, sql_id, self.cursor, self.op_type, None, None, None, None, None,
                    None, None, None, None, None, self.tim, None, '', None, self.fname,
                    self.line, self.ts_callback(self.tim), None, None, None, None, None, None,
                    self.rlbk, self.rd_only, None, None,
                    self.fmeta['SESSION ID'], self.fmeta['CLIENT ID'], self.fmeta['SERVICE NAME'],
                    self.fmeta['MODULE NAME'], self.fmeta['ACTION NAME'],
                    self.fmeta['CONTAINER ID'], None]
    def __str__(self):
        return f"XCTEND rlbk={self.rlbk}, rd_only={self.rd_only}, tim={self.tim}"

class Pic(Ops):
    """ PARSE IN CURSOR lines. SQL statement is persisted as one string, in `raw` field"""
    def __init__(self, op_type, cursor, params, fmeta, ts_callback):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        for item in params.split(' '):
            if len(item):
                key = item.split('=')
                if key[0] in ['ad', 'sqlid']:
                    self.__dict__[key[0]] = key[1].strip("'")
                else:
                    self.__dict__[key[0]] = int(key[1])
        self.__dict__['raw'] = []
        self.__slots__ = (op_type, cursor, 'dep', 'tim', 'len', 'uid', 'oct', 'lid',
                            'hv', 'ad', 'raw', 'sqlid')
    def to_list(self, exec_id, sql_id):
        return [exec_id, sql_id, self.cursor, self.op_type, None, None, None, None, None,
                    None, None, self.dep, None, None, self.tim, None, '', "".join(self.raw),
                    self.fname, self.line, self.ts_callback(self.tim), self.len, self.uid,
                    self.oct, self.lid, self.hv, self.ad, None, None, None, None,
                    self.fmeta['SESSION ID'], self.fmeta['CLIENT ID'], self.fmeta['SERVICE NAME'],
                    self.fmeta['MODULE NAME'], self.fmeta['ACTION NAME'],
                    self.fmeta['CONTAINER ID'], None]
    def __str__(self):
        return f"PARSING IN CURSOR len={self.len} dep={self.dep} uid={self.uid} " \
               + f"oct={self.oct} lid={self.lid} tim={self.tim} hv={self.hv} "    \
               + f"ad={self.ad} sqlid={self.sqlid}\n{self.raw}\nEND OF STMT"

class Lob(Ops):
    """ Various LOB* operations."""
    def __init__(self, op_type, cursor, params, fmeta, ts_callback):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        for item in params.split(','):
            if len(item):
                key = item.split('=')
                if key[0] == 'type':
                    self.__dict__[key[0]] = key[1]
                else:
                    self.__dict__[key[0]] = int(key[1])
        self.__slots__ = ('op_type', 'cursor', 'type', 'c', 'e', 'p', 'cr', 'cu', 'tim', 'bytes')
    def to_list(self, exec_id, sql_id):
        return [exec_id, None, None, self.op_type, self.c, self.e, self.p, self.cr,
                self.cu, None, None, None, None, None, self.tim, None,
                '', '', self.fname, self.line, self.ts_callback(self.tim), None, None, None, None,
                None, None, None, None, self.type, self.bytes,
                self.fmeta['SESSION ID'], self.fmeta['CLIENT ID'], self.fmeta['SERVICE NAME'],
                self.fmeta['MODULE NAME'], self.fmeta['ACTION NAME'],
                self.fmeta['CONTAINER ID'], None]
    def __str__(self):
        return f"{self.op_type}: type={self.type},bytes={self.r},c={self.c},e={self.e},"   \
               + f"p={self.p},cr={self.cr},cu={self.cu},tim={self.tim}"

class Exec(Ops):
    """ Events related to the database client calls (EXEC, FETCH, PARSE, CLOSE). These have similar
        enough properties."""
    def __init__(self, op_type, cursor, params, fmeta, ts_callback):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        for item in params.split(','):
            if len(item):
                key = item.split('=')
                self.__dict__[key[0]] = int(key[1])
        self.__slots__ = ('op_type', 'cursor', 'c', 'e', 'p', 'cr', 'cu', 'mis', 'r',
                            'dep', 'og', 'plh', 'tim', 'type')
    def to_list(self, exec_id, sql_id):
        return [exec_id, sql_id, self.cursor, self.op_type, self.c, self.e, self.p, self.cr,
                    self.cu, self.mis, self.r, self.dep, self.og, self.plh, self.tim, self.type,
                    '', '', self.fname, self.line, self.ts_callback(self.tim), None, None, None,
                    None, None, None, None, None, None, None,
                    self.fmeta['SESSION ID'], self.fmeta['CLIENT ID'], self.fmeta['SERVICE NAME'],
                    self.fmeta['MODULE NAME'], self.fmeta['ACTION NAME'],
                    self.fmeta['CONTAINER ID'], None]
    def __str__(self):
        str0 = f"{self.cursor}: {self.op_type} "
        return str0 + f"c={self.c},e={self.e},p={self.p},cr={self.cr},cu={self.cu},"           \
                    + f"mis={self.mis},r={self.r},dep={self.dep},og={self.og},plh={self.plh}," \
                    + f"tim={self.tim},fname={self.fname},line={self.line}"

class Error(Ops):
    """Covers ERROR call. Has two attributes, error code and tim. """
    def __init__(self, op_type, cursor, params, fmeta, ts_callback):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        for item in params.split(' '):
            if len(item):
                key = item.split('=')
                self.__dict__[key[0]] = int(key[1])
        self.__slots__ = ('op_type', 'cursor', 'err', 'tim')
    def to_list(self, exec_id, sql_id):
        return [exec_id, sql_id, self.cursor, self.op_type, None, None, None, None,
                    None, None, None, None, None, None, self.tim, None,
                    '', '', self.fname, self.line, self.ts_callback(self.tim), None, None, None,
                    None, None, None, None, None, None, None,
                    self.fmeta['SESSION ID'], self.fmeta['CLIENT ID'], self.fmeta['SERVICE NAME'],
                    self.fmeta['MODULE NAME'], self.fmeta['ACTION NAME'],
                    self.fmeta['CONTAINER ID'], self.err]
    def __str__(self):
        str0 = f"{self.op_type} {self.cursor}:"
        return str0 + f"err={self.err} tim={self.tim}"
