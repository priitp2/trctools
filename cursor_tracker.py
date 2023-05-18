from current_statement import CurrentStatement
from statement import Statement

class CursorTracker:
    '''
        Keeps track of the statements and cursors. add_parsing_in() creates new statement, 
        add_parse, and add_exec add new cursors if needed. If new cursor is added, events
        in the old one are added to the statement.
    '''
    def __init__(self, db):
        self.db = db
        self.latest_cursors = {}
        self.cursors = {}
        self.statements = {}

        s = Statement('#0', "sqlid='dummy'", False, db)
        self.statements[s.sql_id] = s
        self.cursors['#0'] = s.sql_id
        self.add_latest_cursor('#0')
    def _get_cursor(self, cursor):
        if cursor in self.latest_cursors.keys():
            return self.latest_cursors[cursor]
        else:
            return None
    def add_latest_cursor(self, cursor):
        ''' If cursor is present then this is new execution, so merge the cursor with the statement and
            overwrite the latest_cursor.
        '''
        cs = self._get_cursor(cursor)
        if cs:
            # FIXME: Trace can contain cursor without matching statement
            if cursor in self.cursors.keys():
                statement = self.statements[self.cursors[cursor]]
            else:
                print("add_latest_cursor: unknown cursor {}, probably missing PARSE* operation".format(cursor))
                return None
            statement.add_current_statement(cs)
        self.latest_cursors[cursor] = CurrentStatement(cursor, self.db)
        return self.latest_cursors[cursor]
    def add_parsing_in(self, cursor, params):
        s = Statement(cursor, params, False, self.db)
        if s.sql_id not in self.statements.keys():
            self.statements[s.sql_id] = s
        self.cursors[cursor] = s.sql_id
        self.latest_cursors[cursor] = CurrentStatement(cursor, self.db, s.sql_id)
    def add_parse(self, cursor, params):
        cs_old = None
        cs = self._get_cursor(cursor)
        if cs:
            cs_old = cs
            if cs.parse:
                cs = self.add_latest_cursor(cursor)
        else:
            cs = self.add_latest_cursor(cursor)
        cs.add_parse(params)
        return cs_old
    def add_exec(self, cursor, params):
        old_cs = None
        cs = self._get_cursor(cursor)
        if cs:
            if cs.exec:
                old_cs = cs
                cs = self.add_latest_cursor(cursor)
                if not cs:
                    return None
            cs.add_exec(params)
        else:
            #cs = self.add_latest_cursor(cursor)
            #old_cs = cs
            print("add_exec: second None")
            return None
        return old_cs
    def add_fetch(self, cursor, params):
        # FIXME: stray cursors, PARSING is probably not in the trace file
        cs = self._get_cursor(cursor)
        if not cs:
            cs = CurrentStatement(cursor, self.db)
            self.latest_cursors[cursor] = cs
        cs.add_fetch(params)
    def add_wait(self, cursor, params):
        cs = self._get_cursor(cursor)
        if not cs:
            cs = CurrentStatement(cursor, self.db)
            self.latest_cursors[cursor] = cs
        cs.add_wait(params)
    def add_close(self, cursor, params):
        cs = self._get_cursor(cursor)
        if not cs:
            print("add_close: stray cursor {}".format(cursor))
            return None
        cs.add_close(params)
        self.add_latest_cursor(cursor)
        return cs
    def flush(self, fname):
        if not self.db:
            return
        for c in self.latest_cursors:
            self.add_latest_cursor(c)
        self.db.insert_cursors([(c, self.cursors[c]) for c in self.cursors.keys()])
        self.db.flush(fname)
