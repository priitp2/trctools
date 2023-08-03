import re

class Ops:
    """
        Ops corresponds to a line in SQL trace file, with some exceptions (empty lines,
        some parts of of the file header). Since different operations have different formats,
        this class is a hot mess. Do not touch!
    """
    def __init__(self, op_type, cursor, params, fname, line, name=None, ts2=None):
        self.op_type = op_type
        self.cursor = cursor
        self.fname = fname
        self.line = line
        if op_type == 'WAIT':
            self.__dict__['raw'] = params
            match = re.match(r""" nam='(.*)' ela= (\d+) (.*) tim=(\d+)""", params)
            if match:
                self.__dict__['name'] = match.group(1).strip("'")
                self.__dict__['e'] = int(match.group(2))

                self.__dict__['tim'] = int(match.group(4))
            # WAIT does not have cpu component, but merging the Ops needs c
            self.__slots__ = (op_type, cursor, 'raw', 'name', 'e', 'tim', 'c', fname, line)
        elif op_type == 'STAT':
            self.__dict__['raw'] = params
            self.__slots__ = (op_type, cursor, 'raw', fname, line)
        elif op_type in ('STAR', 'HEADER'):
            self.__dict__['name'] = name
            self.__dict__['raw'] = params
            self.__dict__['ts2'] = ts2
            self.__slots__ = (op_type, cursor, 'raw', fname, line, 'name', 'ts2')
        elif op_type == 'BINDS':
            self.__dict__['raw'] = params
            self.__slots__ = (op_type, cursor, 'raw', fname, line)
        elif op_type == 'XCTEND':
            for item in params.split(', '):
                key = item.split('=')
                self.__dict__[key[0]] = int(key[1])
            self.__slots__ = (op_type, cursor, 'rlbk', 'rd_only', 'tim', fname, line)
        elif op_type == 'PIC':
            for item in params.split(' '):
                if len(item):
                    key = item.split('=')
                    if key[0] in ['ad', 'sqlid']:
                        self.__dict__[key[0]] = key[1].strip("'")
                    else:
                        self.__dict__[key[0]] = int(key[1])
            self.__slots__ = (op_type, cursor, 'dep', 'tim', 'len', 'uid', 'oct', 'lid',
                                'hv', 'ad', 'raw', fname, line, 'sqlid')
            self.__dict__['raw'] = []
        elif op_type.startswith('LOB'):
            for item in params.split(','):
                if len(item):
                    key = item.split('=')
                    if key[0] == 'type':
                        self.__dict__[key[0]] = key[1]
                    else:
                        self.__dict__[key[0]] = int(key[1])
            self.__slots__ = ('op_type', 'cursor', 'type', 'c', 'e', 'p', 'cr', 'cu', 'tim',
                                'bytes')
        else:
            for item in params.split(','):
                if len(item):
                    key = item.split('=')
                    self.__dict__[key[0]] = int(key[1])
            self.__slots__ = ('op_type', 'cursor', 'c', 'e', 'p', 'cr', 'cu', 'mis', 'r',
                                'dep', 'og', 'plh', 'tim', 'type', fname, line)

    def __getattr__(self, name):
        if name in self.__slots__:
            return 0
        raise AttributeError(f"Wrong attribute: {name}")
    def to_list(self, exec_id, sql_id):
        if self.op_type == 'WAIT':
            return [exec_id, sql_id, self.cursor, self.op_type, None, self.e, None, None,
                        None, None, None, None, None, None, self.tim, None, self.name,
                        self.raw, self.fname, self.line, None, None, None, None, None, None,
                        None, None, None]
        if self.op_type in 'STAT':
            return [exec_id, sql_id, self.cursor, self.op_type, None, None, None, None, None,
                        None, None, None, None, None, None, None, None, self.raw, self.fname,
                        self.line, None, None, None, None, None, None, None, None, None]
        if self.op_type in 'BINDS':
            return [exec_id, sql_id, self.cursor, self.op_type, None, None, None, None, None,
                        None, None, None, None, None, None, None, None, "".join(self.raw),
                        self.fname, self.line, None, None, None, None, None, None, None, None,
                        None]
        if self.op_type in ('STAR', 'HEADER'):
            return [exec_id, sql_id, self.cursor, self.op_type, None, None, None, None, None,
                        None, None, None, None, None, None, None, self.name, self.raw, self.fname,
                        self.line, self.ts2, None, None, None, None, None, None, None, None]
        if self.op_type == 'PIC':
            return [exec_id, sql_id, self.cursor, self.op_type, None, None, None, None, None,
                        None, None, self.dep, None, None, self.tim, None, '', "".join(self.raw),
                        self.fname, self.line, None, self.len, self.uid, self.oct, self.lid,
                        self.hv, self.ad, None, None]
        if self.op_type == 'XCTEND':
            return [exec_id, sql_id, self.cursor, self.op_type, None, None, None, None, None,
                        None, None, None, None, None, self.tim, None, '', None, self.fname,
                        self.line, None, None, None, None, None, None, None, self.rlbk,
                        self.rd_only]
        if self.op_type.startswith('LOB'):
            return [exec_id, None, None, self.op_type, self.c, self.e, self.p, self.cr,
                    self.cu, None, self.bytes, None, None, None, self.tim, self.type,
                    '', '', self.fname, self.line, None, None, None, None, None, None, None, None,
                    None]
        return [exec_id, sql_id, self.cursor, self.op_type, self.c, self.e, self.p, self.cr,
                    self.cu, self.mis, self.r, self.dep, self.og, self.plh, self.tim, self.type,
                    '', '', self.fname, self.line, None, None, None, None, None, None, None, None,
                    None]

    def __str__(self):
        str0 = f"{self.cursor}: {self.op_type} "
        if self.op_type in ['WAIT', 'STAT', 'BINDS']:
            return str0 + f"{self.raw}"
        if self.op_type == 'HEADER':
            return f"{self.name}: {self.raw}"
        if self.op_type == 'STAR':
            return f"*** {self.name}: ({self.raw}) {self.ts2}"
        if self.op_type == 'CLOSE':
            return str0 + f"c={self.c},e={self.e},type={self.type},tim={self.tim}"
        if self.op_type == 'XCTEND':
            return f"XCTEND rlbk={self.rlbk}, rd_only={self.rd_only}, tim={self.tim}"
        if self.op_type == 'PIC':
            return f"PARSING IN CURSOR len={self.len} dep={self.dep} uid={self.uid} " \
                   + f"oct={self.oct} lid={self.lid} tim={self.tim} hv={self.hv} "    \
                   + f"ad={self.ad} sqlid={self.sqlid}\n{self.raw}\nEND OF STMT"
        if self.op_type.startswith('LOB'):
            return f"{self.op_type}: type={self.type},bytes={self.r},c={self.c},e={self.e},"   \
                   + f"p={self.p},cr={self.cr},cu={self.cu},tim={self.tim}"
        return str0 + f"c={self.c},e={self.e},p={self.p},cr={self.cr},cu={self.cu},"           \
                    + f"mis={self.mis},r={self.r},dep={self.dep},og={self.og},plh={self.plh}," \
                    + f"tim={self.tim},fname={self.fname},line={self.line}"
