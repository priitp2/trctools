import re

class Ops:
    def __init__(self, op_type, cursor, params):
        self.op_type = op_type
        self.cursor = cursor
        if op_type == 'WAIT':
            self.__dict__['raw'] = params
            match = re.match(r""" nam='(.*)' ela= (\d+) (.*) tim=(\d+)""", params)
            self.__dict__['name'] = match.group(1).strip("'")
            self.__dict__['e'] = int(match.group(2))
            self.__dict__['tim'] = int(match.group(4))
            self.__slots__ = (op_type, cursor, 'raw', 'name', 'e', 'tim')
        else:
            for item in params.split(','):
                key = item.split('=')
                self.__dict__[key[0]] = int(key[1])
            self.__slots__ = ('op_type', 'cursor', 'c', 'e', 'p', 'cr', 'cu', 'mis', 'r', 'dep', 'og', 'plh', 'tim', 'type')

    def __getattr__(self, name):
            if name in self.__slots__:
                return 0
            else:
                raise AttributeError("Wrong attribute: {}".format(name))
