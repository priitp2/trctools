from typing import Optional
from ops import Ops

KNOWN_OPS: tuple[str, ...] = ('PIC', 'PARSE', 'EXEC', 'WAIT', 'FETCH', 'CLOSE', 'STAT',
    'BINDS', 'ERROR', 'PARSE ERROR')

ITERABLE_OPS: tuple[str, ...] = ('WAIT', 'FETCH', 'STAT')

class CurrentStatement:
    """Tracks operations done within one database interaction/span."""
    def __init__(self, cursor: str, dbs, sql_id: Optional[str]=None) -> None:
        self.__slots__ = ('cursor', 'sql_id', 'dbs', 'known_ops', 'ops', 'ops_container')

        # Cursor is 0 or a large number 
        if len(cursor) <= 2 and cursor != '#0':
            raise ValueError("init: got empty cursor")

        self.cursor = cursor
        # These calls are tracked as a client interaction(span_id)
        self.ops: dict[str, Ops] = {}
        self.ops_container: list[Ops] = []
        self.sql_id = sql_id
        self.dbs = dbs

    def __len__(self) -> int:
        """Returns number of operations in span"""
        return len(self.ops) + len(self.ops_container)
    def add_ops(self, ops: Ops) -> None:
        """Adds database operation to the current statement"""
        if self.cursor != ops.cursor:
            raise KeyError(f"add_ops: wrong cursor, got {ops.cursor}, have {self.cursor}")
        if ops.op_type not in KNOWN_OPS:
            raise KeyError(f"add_ops: unknown ops type: {ops.op_type}")
        if ops.op_type in self.ops and ops.op_type not in ITERABLE_OPS:
            raise KeyError(f"add_ops: already set: ops {ops.op_type}")
        if ops.op_type in ITERABLE_OPS:
            self.ops_container.append(ops)
            return
        self.ops[ops.op_type] = ops
    def count_ops(self, op_type: str) -> int:
        """Counts number of (listy) ops. Useful for tests."""
        for ops in self.ops.values():
            if ops.op_type == op_type:
                return 1
        out = 0
        for ops in self.ops_container:
            if ops.op_type == op_type:
                out += 1
        return out
    def dump_to_db(self) -> None:
        """Turns ops into lists and adds to the database"""
        if not self.dbs:
            raise TypeError("dump_to_db: database not set!")
        span_id = self.dbs.get_span_id()
        out = []

        for ops in self.ops.values():
            out.append(ops)
        out += self.ops_container
        self.dbs.add_ops(span_id, self.sql_id, out)
