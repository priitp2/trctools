import logging
from current_statement import CurrentStatement

class CallTracker:
    '''
        Keeps track of the statements and cursors. add_parsing_in() creates new statement, 
        add_parse, and add_exec add new cursors if needed. If new cursor is added, events
        in the old one are added to the statement.
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
        ''' If cursor is present then this is new execution, so merge the cursor with
            the statement and overwrite the latest_cursor.
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
    def add_pic(self, cursor, params):
        if params.sqlid not in self.statements:
            self.statements[params.sqlid] = ''
        self.cursors[cursor] = params.sqlid
        cs = CurrentStatement(cursor, self.db, params.sqlid)
        self.latest_cursors[cursor] = cs

        cs.add_pic(params)
    def add_parse(self, cursor, params):
        cs = self._get_cursor(cursor)
        if cs:
            if cs.parse:
                cs = self.add_latest_cursor(cursor)
        else:
            cs = self.add_latest_cursor(cursor)
        cs.add_parse(params)
    def add_exec(self, cursor, params):
        cs = self._get_cursor(cursor)
        if cs:
            if cs.exec:
                cs = self.add_latest_cursor(cursor)
        else:
            cs = self.add_latest_cursor(cursor)
        cs.add_exec(params)
    def add_fetch(self, cursor, params):
        cs = self._get_cursor(cursor)
        if not cs:
            cs = self.add_latest_cursor(cursor)
        cs.add_fetch(params)
    def add_wait(self, cursor, params):
        cs = self._get_cursor(cursor)
        if not cs:
            cs = self.add_latest_cursor(cursor)
        cs.add_wait(params)
    def add_close(self, cursor, params):
        cs = self._get_cursor(cursor)
        if not cs:
            # PARSE/PIC happened before start of the trace. Just add the call
            # to the database w/o sql_id
            self.db.insert_ops(params.to_list(self.db.get_exec_id(), ''))
            return
        cs.add_close(params)
        self.add_latest_cursor(cursor)
        return
    def add_stat(self, cursor, params):
        cs = self._get_cursor(cursor)
        if not cs:
            cs = self.add_latest_cursor(cursor)
        cs.add_stat(params)
    def add_binds(self, cursor, params):
        cs = self._get_cursor(cursor)
        if not cs:
            cs = self.add_latest_cursor(cursor)
        cs.add_binds(params)
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
        for cursor in self.latest_cursors:
            self.add_latest_cursor(cursor)
        self.db.flush()
        self.logger.debug('flush: done')
