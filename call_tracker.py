from collections import defaultdict
from typing import Optional
from current_statement import CurrentStatement
from ops import Ops
from time_tracker import TimeTracker

class CallTracker:
    '''
        Tracks database client interactions.
    '''
    def __init__(self, db) -> None:
        self.db = db
        # {cursor handle: CurrentStatement}
        self.latest_cursors: defaultdict[str, CurrentStatement] = defaultdict(lambda: None)
        # {cursor handle: sql_id}
        self.cursors: dict[str, Optional[str]] = {}

        self.dummy_counter = 0
        self.time_tracker = TimeTracker()
        self.stat: CurrentStatement = None
    def _get_cursor(self, cursor: str) -> CurrentStatement:
        if not self.stat or self.stat.cursor != cursor:
            self.stat = self.latest_cursors[cursor]
        return self.stat
    def _set_cursor(self, cursor: str) -> None:
        # Cursors/statements come either throug PARSING IN CURSOR with sql_id,
        # or from here with dummy sql_id
        if cursor not in self.cursors:
            self.cursors[cursor] = None
        crs = CurrentStatement(cursor, self.cursors[cursor])
        self.latest_cursors[cursor] = crs
        self.stat = crs
        return crs
    def add_latest_cursor(self, cursor: str) -> CurrentStatement:
        ''' Tracks client interactions. If cursor is present then this is new execution,
            so dump the cursor to the database and overwrite the latest_cursor.
        '''
        stat = self._get_cursor(cursor)
        if not stat:
            return self._set_cursor(cursor)
        if self.db:
            self.dump_to_db(stat)
        return self._set_cursor(cursor)
    def add_ops(self, cursor: str, ops: Ops) -> None:
        ''' Adds tracked operation.'''

        if ops.op_type == 'PIC':
            self.cursors[cursor] = ops.sqlid
            cstat = self.add_latest_cursor(cursor)
        else:
            cstat = self._get_cursor(cursor)
        # If non-list ops is already set we assume previous client interaction is over and
        # we can close latest cursor/interaction
        if not cstat or (ops.op_type in cstat.ops):
            cstat = self.add_latest_cursor(cursor)
        cstat.add_ops(ops)
    def add_lob(self, lob: Ops) -> bool:
        '''Add LOB operation to the current span. This bypasses normal flow since LOB ops have
            cursor #0.'''
        if not lob:
            return False
        if not self.stat:
            return False
        self.stat.add_lob(lob)
        return True
    def reset(self) -> None:
        '''Cleans the state of the tracker. Removes empty statements from latest_cursor. '''
        empty = []
        for cursor in self.latest_cursors:
            if len(self.latest_cursors[cursor]) > 0:
                self.add_latest_cursor(cursor)
            else:
                empty.append(cursor)
        for cursor in empty:
            del self.latest_cursors[cursor]
    def flush(self) -> None:
        '''Resets the tracker and flushes the db. '''
        if not self.db:
            raise RuntimeError("Backend not set!")
        self.reset()
        self.db.flush()
        self.time_tracker.reset(None)
    def dump_to_db(self, stat: CurrentStatement) -> None:
        """Adds CurrentStatement to the database"""
        if not self.db:
            raise TypeError("dump_to_db: database not set!")
        span_id = self.db.get_span_id()
        out = stat.to_list(span_id)
        self.db.add_ops(span_id, stat.sql_id, out)
