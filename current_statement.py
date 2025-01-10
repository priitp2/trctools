from collections.abc import Mapping
from typing import Optional
from ops import Ops

class CurrentStatement:
    """Tracks operations done within one database interaction."""
    def __init__(self, cursor: str, dbs, sql_id: Optional[str]=None) -> None:
        self.__slots__ = ('cursor', 'sql_id', 'dbs', 'known_ops', 'ops')
        if len(cursor) < 2 and cursor != '#0':
            raise ValueError("init: got empty cursor")
        self.cursor = cursor
        # These calls are tracked as a client interaction(span_id)
        self.known_ops: tuple[str, ...] = ('PIC', 'PARSE', 'EXEC', 'WAIT', 'FETCH', 'CLOSE', 'STAT',
                'BINDS', 'ERROR', 'PARSE ERROR')
        self.iterable_ops: tuple[str, ...] = ('WAIT', 'FETCH', 'STAT')
        self.ops: Mapping[str, Ops | list[Ops]] = {}
        self.sql_id = sql_id
        self.dbs = dbs
    def is_not_empty(self) -> bool:
        """Checks if any of the ops is set. """
        if len(self.ops) > 0:
            return True
        return False

    def add_ops(self, ops: Ops) -> None:
        if self.cursor != ops.cursor:
            raise KeyError(f"add_ops: wrong cursor, got {ops.cursor}, have {self.cursor}")
        if ops.op_type not in self.known_ops:
            raise KeyError(f"add_ops: unknown ops type: {ops.op_type}")
        if ops.op_type in self.ops and ops.op_type not in self.iterable_ops:
            raise KeyError(f"add_ops: already set: ops {ops.op_type}")
        if ops.op_type in self.iterable_ops:
            if ops.op_type in self.ops:
                self.ops[ops.op_type].append(ops)
            else:
                self.ops[ops.op_type] = [ops]
            return
        self.ops[ops.op_type] = ops
    def is_set(self, op_type: str) -> bool:
        """Checks if specific ops is set"""
        if op_type in self.ops:
            return True
        return False
    def count_ops(self, op_type: str) -> int:
        """Counts number of (listy) ops. Useful for tests."""
        for ops in self.ops.values():
            if isinstance(ops, list):
                return len(ops)
            if ops.op_type == op_type:
                return 1
        return 0
    def dump_to_db(self) -> None:
        """Turns ops into lists and adds to the database"""
        if not self.dbs:
            raise TypeError("dump_to_db: database not set!")
        span_id = self.dbs.get_span_id()
        out = []

        for ops in self.ops.values():
            if isinstance(ops, list):
                out += ops
            else:
                out.append(ops)
        self.dbs.add_ops(span_id, self.sql_id, out)
