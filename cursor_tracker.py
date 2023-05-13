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
    def add_latest_cursor(self, cursor):
        ''' If cursor is present then this is new execution, so merge the cursor with the statement and
            overwrite the latest_cursor.
        '''
        if cursor in self.latest_cursors.keys():
            # FIXME: Trace can contain cursor without matching statement
            if cursor in self.cursors.keys():
                statement = self.statements[self.cursors[cursor]]
            else:
                print("add_latest_cursor: unknown cursor {}, probably missing PARSE* operation".format(cursor))
                return None
            cs = self.latest_cursors[cursor]
            statement.add_current_statement(cs)
        self.latest_cursors[cursor] = CurrentStatement(cursor, self.db)
        return self.latest_cursors[cursor]
    def add_parsing_in(self, cursor, params):
        s = Statement(cursor, params, False, self.db)
        if s.sql_id not in self.statements.keys():
            self.statements[s.sql_id] = s
        self.cursors[cursor] = s.sql_id
        self.latest_cursors[cursor] = CurrentStatement(cursor, self.db)
    def add_parse(self, cursor, params):
        cs_old = None
        if cursor in self.latest_cursors.keys():
            cs = self.latest_cursors[cursor]
            cs_old = cs
            if cs.parse:
                cs = self.add_latest_cursor(cursor)
        else:
            cs = self.add_latest_cursor(cursor)
        cs.add_parse(params)
        return cs_old
    def add_exec(self, cursor, params):
        old_cs = None
        cs = None
        if cursor in self.latest_cursors.keys():
            cs = self.latest_cursors[cursor]
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
        if cursor not in self.latest_cursors.keys():
            self.latest_cursors[cursor] = CurrentStatement(cursor, self.db)
        cs = self.latest_cursors[cursor]
        cs.add_fetch(params)
    def add_wait(self, cursor, params):
        if cursor not in self.latest_cursors.keys():
            self.latest_cursors[cursor] = CurrentStatement(cursor, self.db)
        cs = self.latest_cursors[cursor]
        cs.add_wait(params)
    def add_close(self, cursor, params):
        try:
            cs = self.latest_cursors[cursor]
        except KeyError:
            print("add_close: stray cursor {}".format(cursor))
            return None
        cs.add_close(params)
        self.add_latest_cursor(cursor)
        return cs
    def flush(self):
        for c in self.latest_cursors:
            self.add_latest_cursor(c)

