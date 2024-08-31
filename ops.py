from dataclasses import dataclass, astuple, fields, asdict
import datetime
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

@dataclass(init=False, kw_only=True)
class DatabaseOp:
    """Container for the various fields in the trace files. Field names correspond 1:1 to the
       contents of the trace file, except: 
           * sql_id is called sqlid in PARSING IN CURSOR event
           * exec_id is synthetically generated, it has no counterpart in the trace file
           * ts is the timestamp found here and there in the trace file. Can be generated
             for all operations.
           * lobtype is 'type' in the trace file. Renamed to avoid data type clash with 'type'
             parameter in CLOSE.
           """
    exec_id: int = None
    sql_id: str = None
    cursor: str = None
    op_type: str = None
    c: int = None
    e: int = None
    p: int = None
    cr: int = None
    cu: int = None
    mis: int = None
    r: int = None
    dep: int = None
    og: int = None
    plh: int = None
    tim: int = None
    type: int = None
    name: str = None
    raw: str = None
    fname: str = None
    line: int = None
    ts: datetime.datetime = None
    len: int = None
    uid: int = None
    oct: int = None
    lid: int = None
    hv: int = None
    ad: str = None
    rlbk: str = None
    rd_only: int = None
    lobtype: str = None
    bytes: int = None
    sid: str = None
    client_id: str = None
    service_name: str = None
    module: str = None
    action: str = None
    container_id: int = None
    err: int = None

class Ops:
    """
        Base class for various operations.
    """
    def __init__(self, op_type, cursor, fmeta, ts_callback):
        self.dbop = DatabaseOp()
        self.dbop.op_type = op_type
        self.dbop.cursor = cursor
        self.dbop.fname = fmeta['FILE_NAME']
        self.dbop.line = fmeta['LINE_COUNT']
        self.dbop.sid = fmeta['SID']
        self.dbop.client_id = fmeta['CLIENT ID']
        self.dbop.service_name = fmeta['SERVICE NAME']
        self.dbop.module = fmeta['MODULE']
        self.dbop.action = fmeta['ACTION']
        self.dbop.container_id = fmeta['CONTAINER ID']

        self.ts_callback = ts_callback
    def __getattr__(self, name):
        """ In case of missing attribute returns 0 if attribute is in __slots__. This is needed in
            to_list(). """
        # FIXME: rewrite after last subclass is converted!
        # FIXME: PIC has sqlid, this is everywhere else called sql_id
        if name == 'sqlid':
            return self.dbop.sql_id
        if name in self.dbop.__dict__:
            return self.dbop.__dict__[name]
        if name in [f.name for f in fields(self.dbop)]:
            return 0
        raise AttributeError(f"Wrong attribute: {name}")
    def to_list(self, exec_id, sql_id):
        """ Generates list that is used to persist Ops in the database. Children are supposed to 
            override this."""
        self.dbop.exec_id = exec_id
        self.dbop.sql_id = sql_id
        if self.dbop.ts == None and self.ts_callback != None:
            self.dbop.ts = self.ts_callback(self.dbop.tim)
        return astuple(self.dbop)
    def to_dict(self, exec_id, sql_id):
        out = asdict(self.dbop)

        out['sql_id'] = sql_id
        out['exec_id'] = exec_id
        if out['op_type'] == 'PIC':
             out['op_type']= 'PARSING IN CURSOR'
        if self.dbop.ts == None and self.ts_callback != None:
            out['ts'] = self.ts_callback(self.dbop.tim)

        return out 
    def __str__(self):
        return ''

class Wait(Ops):
    """ Handles WAIT lines. Wait event name is parsed out, everything else is persisted as-is."""
    def __init__(self, op_type, cursor, params, fmeta, ts_callback):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        self.dbop.__dict__['raw'] = params
        match = wait_matcher.match(params)
        if match:
            self.dbop.__dict__['name'] = match.group(1).strip("'")
            self.dbop.__dict__['e'] = int(match.group(2))

            self.dbop.__dict__['tim'] = int(match.group(4))
            self.__slots__ = (op_type, cursor, 'raw', 'name', 'e', 'tim')
    def __str__(self):
        return f"{self.cursor}: {self.op_type} {self.raw}"

class Stat(Ops):
    """ Execuion plan and statistics(STAT). Persisted as-is."""
    def __init__(self, op_type, cursor, params, fmeta, ts_callback):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        self.dbop.__dict__['raw'] = params
    def __str__(self):
        return f"{self.cursor}: {self.op_type} {self.raw}"

class Meta(Ops):
    """ Handles trace file header lines and lines that start with stars (***). These lines contain
        wall clock readings, these are persisted in ts2."""
    def __init__(self, op_type, cursor, params, fmeta, name, ts2):
        super().__init__(op_type, cursor, fmeta, None)
        self.dbop.__dict__['name'] = name
        self.dbop.__dict__['raw'] = params
        self.dbop.__dict__['ts'] = ts2
    def __str__(self):
        if self.op_type == 'HEADER':
            return f"{self.dbop.name}: {self.dbop.raw}"
        return f"*** {self.dbop.name}:({self.dbop.raw}) {self.dbop.ts}"

class Binds(Ops):
    """ Bind values. Everything is persisted as-is, in one string."""
    def __init__(self, op_type, cursor, params, fmeta, ts_callback):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        self.dbop.__dict__['raw'] = params
    def add_line(self, line):
        self.dbop.__dict__['raw'] = "".join((self.dbop.__dict__['raw'], line))
    def __str__(self):
        return f"{self.dbop.cursor}: {self.dbop.op_type} {self.dbop.raw}"

class Xctend(Ops):
    """ Commits (XCTEND)."""
    def __init__(self, op_type, cursor, params, fmeta, ts_callback):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        for item in params.split(', '):
            key = item.split('=')
            self.dbop.__dict__[key[0]] = int(key[1])
    def __str__(self):
        return f"XCTEND rlbk={self.dbop.rlbk}, rd_only={self.dbop.rd_only}, tim={self.dbop.tim}"

class Pic(Ops):
    """ PARSE IN CURSOR lines. SQL statement is persisted as one string, in `raw` field"""
    def __init__(self, op_type, cursor, params, fmeta, ts_callback):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        for item in params.split(' '):
            if len(item):
                key = item.split('=')
                if key[0] == 'sqlid':
                    self.dbop.__dict__['sql_id'] = key[1].strip("'")
                elif key[0] == 'ad':
                    self.dbop.__dict__[key[0]] = key[1].strip("'")
                else:
                    self.dbop.__dict__[key[0]] = int(key[1])
        self.dbop.__dict__['raw'] = ''
    def add_line(self, line):
        self.dbop.__dict__['raw'] = "".join((self.dbop.__dict__['raw'], line))
    def __str__(self):
        return f"PARSING IN CURSOR len={self.dbop.len} dep={self.dbop.dep} uid={self.dbop.uid} " \
               + f"oct={self.dbop.oct} lid={self.dbop.lid} tim={self.dbop.tim} hv={self.dbop.hv} "    \
               + f"ad={self.dbop.ad} sqlid={self.dbop.sql_id}\n{self.dbop.raw}\nEND OF STMT"

class Lob(Ops):
    """ Various LOB* operations."""
    def __init__(self, op_type, cursor, params, fmeta, ts_callback):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        for item in params.split(','):
            if len(item):
                key = item.split('=')
                if key[0] == 'type':
                    self.dbop.__dict__['lobtype'] = key[1]
                else:
                    self.dbop.__dict__[key[0]] = int(key[1])
    def __str__(self):
        return f"{self.dbop.op_type}: type={self.dbop.type},bytes={self.dbop.r},c={self.dbop.c}," \
               + f"e={self.dbop.e},p={self.dbop.p},cr={self.dbop.cr},cu={self.dbop.cu}," \
               + f"tim={self.dbop.tim}"

class Exec(Ops):
    """ Events related to the database client calls (EXEC, FETCH, PARSE, CLOSE). These have similar
        enough properties."""
    def __init__(self, op_type, cursor, params, fmeta, ts_callback):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        for item in params.split(','):
            if len(item):
                key = item.split('=')
                self.dbop.__dict__[key[0]] = int(key[1])
    def __str__(self):
        str0 = f"{self.dbop.cursor}: {self.dbop.op_type} "
        return str0 + f"c={self.dbop.c},e={self.dbop.e},p={self.dbop.p},cr={self.dbop.cr}," \
                    + f"cu={self.dbop.cu},mis={self.dbop.mis},r={self.dbop.r}," \
                    + f"dep={self.dbop.dep},og={self.dbop.og},plh={self.dbop.plh}," \
                    + f"tim={self.dbop.tim},fname={self.dbop.fname},line={self.dbop.line}"

class Error(Ops):
    """Covers ERROR call. Has two attributes, error code and tim. """
    def __init__(self, op_type, cursor, params, fmeta, ts_callback):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        for item in params.split(' '):
            if len(item):
                key = item.split('=')
                self.dbop.__dict__[key[0]] = int(key[1])
    def __str__(self):
        str0 = f"{self.dbop.op_type} {self.dbop.cursor}:"
        return str0 + f"err={self.dbop.err} tim={self.dbop.tim}"
