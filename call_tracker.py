import logging
from current_statement import CurrentStatement

class CallTracker:
    '''
        Tracks database client interactions.
    '''
    def __init__(self, db):
        self.logger = logging.getLogger(__name__)
        self.db = db
        self.latest_cursors = {}
        # {cursor: sql_id}
        self.cursors = {}
        # {statement: sql_id}
        self.statements = {}

        self.dummy_counter = 0
        self._add_dummy_statement('#0')
    def _get_cursor(self, cursor):
        if cursor in self.latest_cursors:
            return self.latest_cursors[cursor]
        return None
    def _add_dummy_statement(self, cursor):
        if cursor in self.cursors:
            raise KeyError(f"_add_dummy_statement: cursor {cursor} already present with" \
                            + f" sql_id = {self.cursors[cursor]}")
        sql_id = f'dummy{self.dummy_counter}'
        self.statements[sql_id] = ''
        self.cursors[cursor] = sql_id
        self.add_latest_cursor(cursor)
        self.dummy_counter += 1
    def add_latest_cursor(self, cursor):
        ''' If cursor is present then this is new execution, so dump the cursor to the
            database and overwrite the latest_cursor.
        '''
        #self.logger.debug('add_latest_cursor: start')
        cs = self._get_cursor(cursor)
        if not cs:
            # Cursors/statements come either throug add_parsing_in() with sql_id,
            # or from here with dummy sql_id
            if cursor not in self.cursors:
                self._add_dummy_statement(cursor)
            self.latest_cursors[cursor] = CurrentStatement(cursor, self.db, self.cursors[cursor])
            # Trace can contain cursor without matching PARSING IN CURSOR
            #self.logger.debug('add_latest_cursor: done')
            return self.latest_cursors[cursor]
        if self.db:
            #self.logger.debug('add_latest_cursor: dump to db')
            cs.dump_to_db()
            #self.logger.debug('add_latest_cursor: dump to db done')
        self.latest_cursors[cursor] = CurrentStatement(cursor, self.db, self.cursors[cursor])
        #self.logger.debug('add_latest_cursor: done')
        return self.latest_cursors[cursor]
    def add_ops(self, cursor, ops):

        cstat = self._get_cursor(cursor)
        # If non-list ops is already set we assume previous client interaction is over and
        # we can close latest cursor/interaction
        if not cstat or (cstat.is_set(ops.op_type)
                            and not isinstance(cstat.ops[ops.op_type], list)):
            cstat = self.add_latest_cursor(cursor)
        cstat.add_ops(ops)
        # FIXME: should we have special handling for close?
    def add_pic(self, cursor, params):
        cstat = self._get_cursor(cursor)
        if cstat:
            cstat = self.add_latest_cursor(cursor)
            cstat.add_ops(params)

        if params.sqlid not in self.statements:
            self.statements[params.sqlid] = ''
        self.cursors[cursor] = params.sqlid
        cstat = CurrentStatement(cursor, self.db, params.sqlid)
        self.latest_cursors[cursor] = cstat

        cstat.add_ops(params)
    def reset(self):
        empty = []
        for cursor in self.latest_cursors:
            if self.latest_cursors[cursor].is_not_empty():
                self.add_latest_cursor(cursor)
            else:
                empty.append(cursor)
        for cursor in empty:
            del self.latest_cursors[cursor]
    def flush(self):
        self.logger.debug('flush')
        if not self.db:
            return
        self.reset()
        self.db.flush()
        self.logger.debug('flush: done')