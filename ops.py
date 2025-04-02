from dataclasses import dataclass, fields, asdict
import datetime
import re
from sys import exception
from traceback import print_exception
from typing import Any, Optional, Callable

__doc__ = """
    Contains classes representing the various operations/lines in the trace file.
    Some classes handle multiple operations, for brevity.
"""

wait_matcher = re.compile(r""" nam='(.*)' ela= (\d+) (.*) tim=(\d+)""")

@dataclass(init=True, kw_only=True)
class DatabaseOp:
    """Container for the various fields in the trace files. Field names correspond 1:1 to the
       contents of the trace file, except: 
           * sql_id is called sqlid in PARSING IN CURSOR event
           * span_id is synthetically generated, it has no counterpart in the trace file
           * ts is the timestamp found here and there in the trace file. Can be generated
             for all operations.
           * lobtype is 'type' in the trace file. Renamed to avoid data type clash with 'type'
             parameter in CLOSE.
           """
    span_id: int = 0
    sql_id: str = ''
    cursor: str = ''
    op_type: str = ''
    c: int = 0
    e: int = 0
    p: int = 0
    cr: int = 0
    cu: int = 0
    mis: int = 0
    r: int = 0
    dep: int = 0
    og: int = 0
    plh: int = 0
    tim: int = 0
    type: int = 0
    name: str = ''
    raw: str = ''
    fname: str = ''
    line: int = 0
    ts: datetime.datetime = None
    len: int = 0
    uid: int = 0
    oct: int = 0
    lid: int = 0
    hv: int = 0
    ad: str = ''
    rlbk: int = 0
    rd_only: int = 0
    lobtype: str = ''
    bytes: int = 0
    sid: str = ''
    client_id: str = ''
    service_name: str = ''
    module: str = ''
    action: str = ''
    container_id: int = 0
    err: int = 0

class Ops:
    """
        Base class for various operations.
    """
    def __init__(self, op_type: str, cursor: str, fmeta: dict,
            ts_callback: Optional[Callable[[int], datetime.datetime]]) -> None:

        self.dbop = DatabaseOp(op_type = op_type,
            cursor = cursor,
            fname = fmeta['FILE_NAME'],
            line = fmeta['LINE_COUNT'],
            sid = fmeta['SID'],
            client_id = fmeta['CLIENT ID'],
            service_name = fmeta['SERVICE NAME'],
            module = fmeta['MODULE'],
            action = fmeta['ACTION'],
            container_id = fmeta['CONTAINER ID']
        )

        self.ts_callback = ts_callback
    def __getattr__(self, name: str) -> Any:
        """Redirects attributes to self.dbop."""
        if name == 'sqlid':
            return self.dbop.sql_id
        if name in self.dbop.__dict__:
            return self.dbop.__dict__[name]
        raise AttributeError(f"Wrong attribute: {name}")
    def astuple(self, span_id: int, sql_id: str) -> tuple:
        """ Generates list that is used to persist Ops in the database. Children are supposed to 
            override this."""
        self.dbop.span_id = span_id
        self.dbop.sql_id = sql_id
        if self.dbop.ts is None and self.ts_callback is not None:
            self.dbop.ts = self.ts_callback(self.dbop.tim)
        return (
            self.dbop.span_id,
            self.dbop.sql_id,
            self.dbop.cursor,
            self.dbop.op_type,
            self.dbop.c,
            self.dbop.e,
            self.dbop.p,
            self.dbop.cr,
            self.dbop.cu,
            self.dbop.mis,
            self.dbop.r,
            self.dbop.dep,
            self.dbop.og,
            self.dbop.plh,
            self.dbop.tim,
            self.dbop.type,
            self.dbop.name,
            self.dbop.raw,
            self.dbop.fname,
            self.dbop.line,
            self.dbop.ts,
            self.dbop.len,
            self.dbop.uid,
            self.dbop.oct,
            self.dbop.lid,
            self.dbop.hv,
            self.dbop.ad,
            self.dbop.rlbk,
            self.dbop.rd_only,
            self.dbop.lobtype,
            self.dbop.bytes,
            self.dbop.sid,
            self.dbop.client_id,
            self.dbop.service_name,
            self.dbop.module,
            self.dbop.action,
            self.dbop.container_id,
            self.dbop.err,
        )
        #return astuple(self.dbop)
    def to_dict(self, span_id: int, sql_id: str) -> dict:
        """Returns DatabaseOp as a dict."""
        out = asdict(self.dbop)

        out['sql_id'] = sql_id
        out['span_id'] = span_id
        if out['op_type'] == 'PIC':
            out['op_type']= 'PARSING IN CURSOR'
        if self.dbop.ts is None and self.ts_callback is not None:
            out['ts'] = self.ts_callback(self.dbop.tim)

        return out
    def add_line(self, line: str) -> None:
        """Adds another line to the container."""
        self.dbop.__dict__['raw'] = "".join((self.dbop.__dict__['raw'], line))
    def __str__(self) -> str:
        return ''

class Wait(Ops):
    """ Handles WAIT lines. Wait event name is parsed out, everything else is persisted as-is."""
    def __init__(self, op_type: str, cursor: str, params: str, fmeta: dict,
            ts_callback: Callable[[int], datetime.datetime]) -> None:
        super().__init__(op_type, cursor, fmeta, ts_callback)
        self.dbop.__dict__['raw'] = params
        match = wait_matcher.match(params)
        if match:
            self.dbop.__dict__['name'] = match.group(1).strip("'")
            self.dbop.__dict__['e'] = int(match.group(2))

            self.dbop.__dict__['tim'] = int(match.group(4))
            self.__slots__ = (op_type, cursor, 'raw', 'name', 'e', 'tim')
    def __str__(self) -> str:
        return f"{self.cursor}: {self.op_type} {self.raw}"

class Stat(Ops):
    """ Execuion plan and statistics(STAT). Persisted as-is."""
    def __init__(self, op_type: str, cursor: str, params: str, fmeta: dict,
            ts_callback: Callable[[int], datetime.datetime]) -> None:
        super().__init__(op_type, cursor, fmeta, ts_callback)
        self.dbop.__dict__['raw'] = params
    def __str__(self) -> str:
        return f"{self.cursor}: {self.op_type} {self.raw}"

class Meta(Ops):
    """ Handles trace file header lines and lines that start with stars (***). These lines contain
        wall clock readings, these are persisted in ts2."""
    def __init__(self, op_type: str, cursor: str, params: str, fmeta: dict, name: str,
            ts2: datetime.datetime) -> None:
        super().__init__(op_type, cursor, fmeta, None)
        self.dbop.__dict__['name'] = name
        self.dbop.__dict__['raw'] = params
        self.dbop.__dict__['ts'] = ts2
    def __str__(self) -> str:
        if self.op_type == 'HEADER':
            return f"{self.dbop.name}: {self.dbop.raw}"
        return f"*** {self.dbop.name}:({self.dbop.raw}) {self.dbop.ts}"

class Binds(Ops):
    """ Bind values. Everything is persisted as-is, in one string."""
    def __init__(self, op_type: str, cursor: str, params: str, fmeta: dict,
            ts_callback: Callable[[int], datetime.datetime]) -> None:
        super().__init__(op_type, cursor, fmeta, ts_callback)
        self.dbop.__dict__['raw'] = params
    def __str__(self) -> str:
        return f"{self.dbop.cursor}: {self.dbop.op_type} {self.dbop.raw}"

class Xctend(Ops):
    """ Commits (XCTEND)."""
    def __init__(self, op_type: str, cursor: str, params: str, fmeta: dict,
            ts_callback: Callable[[int], datetime.datetime]) -> None:
        super().__init__(op_type, cursor, fmeta, ts_callback)
        for item in params.split(', '):
            key = item.split('=')
            self.dbop.__dict__[key[0]] = int(key[1])
    def __str__(self) -> str:
        return f"XCTEND rlbk={self.dbop.rlbk}, rd_only={self.dbop.rd_only}, tim={self.dbop.tim}"

class Pic(Ops):
    """ PARSE IN CURSOR lines. SQL statement is persisted as one string, in `raw` field"""
    def __init__(self, op_type: str, cursor: str, params: str, fmeta: dict,
            ts_callback: Callable[[int], datetime.datetime]):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        for item in params.split(' '):
            # In case of broken line just ignore it. This allows us to capture rest of the lines
            # From PIC
            try:
                if len(item):
                    key = item.split('=')
                    if key[0] == 'sqlid':
                        self.dbop.__dict__['sql_id'] = key[1].strip("'")
                    elif key[0] == 'ad':
                        self.dbop.__dict__[key[0]] = key[1].strip("'")
                    else:
                        self.dbop.__dict__[key[0]] = int(key[1])
            except (IndexError, ValueError):
                print(f"Pic: got exception at line {fmeta['LINE_COUNT']}, offending line: {params}")
                print_exception(exception())
        self.dbop.__dict__['raw'] = ''
    def __str__(self) -> str:
        return f"PARSING IN CURSOR len={self.dbop.len} dep={self.dbop.dep} uid={self.dbop.uid} " \
               + f"oct={self.dbop.oct} lid={self.dbop.lid} tim={self.dbop.tim} hv={self.dbop.hv} "\
               + f"ad={self.dbop.ad} sqlid={self.dbop.sql_id}\n{self.dbop.raw}\nEND OF STMT"

class Lob(Ops):
    """ Various LOB* operations."""
    def __init__(self, op_type: str, cursor: str, params: str, fmeta: dict,
            ts_callback: Callable[[int], datetime.datetime]):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        for item in params.split(','):
            if len(item):
                key = item.split('=')
                if key[0] == 'type':
                    self.dbop.__dict__['lobtype'] = key[1]
                else:
                    self.dbop.__dict__[key[0]] = int(key[1])
    def __str__(self) -> str:
        return f"{self.dbop.op_type}: type={self.dbop.type},bytes={self.dbop.r},c={self.dbop.c}," \
               + f"e={self.dbop.e},p={self.dbop.p},cr={self.dbop.cr},cu={self.dbop.cu}," \
               + f"tim={self.dbop.tim}"

class Exec(Ops):
    """ Events related to the database client calls (EXEC, FETCH, PARSE, CLOSE). These have similar
        enough properties."""
    def __init__(self, op_type: str, cursor: str, params: str, fmeta: dict,
            ts_callback: Callable[[int], datetime.datetime]):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        for item in params.split(','):
            if len(item):
                key = item.split('=')
                self.dbop.__dict__[key[0]] = int(key[1])
    def __str__(self) -> str:
        str0 = f"{self.dbop.cursor}: {self.dbop.op_type} "
        return str0 + f"c={self.dbop.c},e={self.dbop.e},p={self.dbop.p},cr={self.dbop.cr}," \
                    + f"cu={self.dbop.cu},mis={self.dbop.mis},r={self.dbop.r}," \
                    + f"dep={self.dbop.dep},og={self.dbop.og},plh={self.dbop.plh}," \
                    + f"tim={self.dbop.tim},fname={self.dbop.fname},line={self.dbop.line}"

class Error(Ops):
    """Covers ERROR and PARSE ERROR calls."""
    def __init__(self, op_type: str, cursor: str, params: str, fmeta: dict,
            ts_callback: Callable[[int], datetime.datetime]):
        super().__init__(op_type, cursor, fmeta, ts_callback)
        for item in params.split(' '):
            try:
                if len(item):
                    key = item.split('=')
                    self.dbop.__dict__[key[0]] = int(key[1])
            except (IndexError, ValueError):
                print(f"Error: got exception at line {fmeta['LINE_COUNT']}, offending line: {params}")
                print_exception(exception())
        if op_type == 'PARSE ERROR':
            self.dbop.__dict__['raw'] = ''
    def __str__(self) -> str:
        str0 = f"{self.dbop.op_type} {self.dbop.cursor}:"
        if self.dbop.op_type == 'ERROR':
            return str0 + f"err={self.dbop.err} tim={self.dbop.tim}"
        return str0 + f"len={self.dbop.len} dep={self.dbop.dep} uid={self.dbop.uid} " \
                    + f"oct={self.dbop.oct} lid={self.dbop.lid} tim={self.dbop.tim} " \
                    + f"err={self.dbop.err}"

def ops_factory(op_type: str, cursor: str, params: str, fmeta: dict,
        ts_callback: Callable[[int], datetime.datetime],
        name: Optional[str]=None, ts2: Optional[datetime.datetime]=None) -> Ops:
    """
        Factory method for operations.
    """
    ops: Optional[Ops] = None
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
        case 'ERROR' | 'PARSE ERROR':
            ops = Error(op_type, cursor, params, fmeta, ts_callback)
        case _ if op_type.startswith('LOB'):
            ops = Lob(op_type, cursor, params, fmeta, ts_callback)
        case _:
            raise AttributeError(f"Wrong op_type: {op_type}")
    return ops
