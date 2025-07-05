from typing import Optional
from ops import Ops

KNOWN_OPS: tuple[str, ...] = ('PIC', 'PARSE', 'EXEC', 'WAIT', 'FETCH', 'CLOSE', 'STAT',
    'BINDS', 'ERROR', 'PARSE ERROR')

ITERABLE_OPS: tuple[str, ...] = ('WAIT', 'FETCH', 'STAT')

class CurrentStatement:
    """Tracks operations done within one database interaction/span."""
    def __init__(self, cursor: str, sql_id: Optional[str]=None) -> None:
        self.__slots__ = ('cursor', 'sql_id', 'known_ops', 'ops', 'ops_container')

        # Cursor is 0 or a large number
        if len(cursor) <= 2 and cursor != '#0':
            raise ValueError("init: got empty cursor")

        self.cursor = cursor
        # These calls are tracked as a client interaction(span_id)
        self.ops: dict[str, Ops] = {}
        self.ops_container: list[Ops] = []
        self.sql_id = sql_id

    def __len__(self) -> int:
        """Returns number of operations in span"""
        return len(self.ops) + len(self.ops_container)
    def add_ops(self, ops: Ops) -> None:
        """Adds database operation to the current statement"""
        if ops.op_type not in KNOWN_OPS:
            raise KeyError(f"add_ops: unknown ops type: {ops.op_type}")
        if self.cursor != ops.cursor:
            raise KeyError(f"add_ops: wrong cursor, got {ops.cursor}, have {self.cursor}")
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
    def to_list(self, span_id: int) -> list:
        """Turns ops into list"""
        out = []

        for ops in self.ops.values():
            out.append(ops)
        out += self.ops_container
        return out
