import re

class Ops:
    # FIXME: add __str__ method
    def __init__(self, op_type, cursor, params, fname, line):
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
        else:
            for item in params.split(','):
                if len(item):
                    key = item.split('=')
                    self.__dict__[key[0]] = int(key[1])
            self.__slots__ = ('op_type', 'cursor', 'c', 'e', 'p', 'cr', 'cu', 'mis', 'r', 'dep', 'og', 'plh', 'tim', 'type', fname, line)

    def __getattr__(self, name):
            if name in self.__slots__:
                return 0
            else:
                raise AttributeError("Wrong attribute: {}".format(name))
    def merge(self, ops1):
        if not ops1:
            return self
        if not isinstance(ops1, list):
            ops1 = [ops1]
        out = Ops(self.op_type, self.cursor, '', '', 0)
        out.c = self.c
        out.e = self.e
        for o in ops1:
            if out.cursor != o.cursor:
                raise ValueError("Ops.merge: cursor = {}, o.cursor = {}".format(self.cursor, o.cursor))
            out.c += o.c
            out.e += o.e

        return out
    def to_list(self, exec_id, sql_id):
        # FIXME: does not handle WAIT
        if self.op_type == 'WAIT':
            return [exec_id, sql_id, self.cursor, self.op_type, 0, self.e, 0, 0, 0, 0, 0, 0, 0, 0, self.tim, 0, self.name, self.raw, self.fname, self.line]
        elif self.op_type == 'STAT':
            return [exec_id, sql_id, self.cursor, self.op_type, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, None, self.raw, self.fname, self.line]
        else:
            return [exec_id, sql_id, self.cursor, self.op_type, self.c, self.e, self.p, self.cr, self.cu, self.mis, self.r, self.dep, self.og, self.plh, self.tim, self.type, '', '', self.fname, self.line]
    def __str__(self):
        str0 = "{}: {} ".format(self.cursor, self.op_type)
        if self.op_type in ['WAIT', 'STAT']:
            return str0 + "{}".format(self.raw)
        elif self.op_type == 'CLOSE':
            return str0 + "c={},e={},type={},tim={}, fname={},line={}".format(self.c, self.e, self.type, self.tim, self.fname, self.line)
        else:
            return str0 + "c={},e={},p={},cr={},cu={},mis={},r={},dep={},og={},plh={},tim={}, fname={},line={}".format(self.c, self.e, self.p, self.cr, self.cu, self.mis, self.r, self.dep, self.og, self.plh, self.tim, self.fname, self.line)
