from current_statement import CurrentStatement
from statement import Statement

class CursorTracker:
    '''
        Keeps track of the statements and cursors. add_parsing_in() creates new statement, 
        add_parse, and add_exec add new cursors if needed. If new cursor is added, events
        in the old one are added to the statement.
    '''
    def __init__(self, cursors, statements):
        self.latest_cursors = {}
        self.cursors = cursors
        self.statements = statements

        self.add_latest_cursor('#0')
    def add_latest_cursor(self, cursor):
        ''' If cursor is present then this is new execution, so merge the cursor with the statement and
            overwrite the latest_cursor.
        '''
        if cursor in self.latest_cursors.keys():
            statement = self.statements[self.cursors[cursor]]
            cs = self.latest_cursors[cursor]
            statement.add_current_statement(cs)
        self.latest_cursors[cursor] = CurrentStatement(cursor)
        return self.latest_cursors[cursor]
    def add_parsing_in(self, cursor, params):
        s = Statement(cursor, params, False)
        if s.sql_id not in self.statements.keys():
            self.statements[s.sql_id] = s
        self.cursors[cursor] = s.sql_id
        self.latest_cursors[cursor] = CurrentStatement(cursor)
    def add_parse(self, cursor, params):
        if cursor in self.latest_cursors.keys():
            cs = self.latest_cursors[cursor]
            if cs.parse != None:
                cs = self.add_latest_cursor(cursor)
        else:
            cs = self.add_latest_cursor(cursor)
        cs.add_parse(params)
    def add_exec(self, cursor, params):
        if cursor in self.latest_cursors.keys():
            cs = self.latest_cursors[cursor]
            if cs.exec != None:
                cs = self.add_latest_cursor(cursor)
        else:
            cs = self.add_latest_cursor(cursor)
        cs.add_exec(params)
    def add_fetch(self, cursor, params):
        # FIXME: stray cursors, PARSING is probably not in the trace file
        if cursor not in self.latest_cursors.keys():
            self.latest_cursors[cursor] = CurrentStatement(cursor)
        cs = self.latest_cursors[cursor]
        cs.add_fetch(params)
    def add_wait(self, cursor, params):
        cs = self.latest_cursors[cursor]
        cs.add_wait(params)
    def add_close(self, cursor, params):
        cs = self.latest_cursors[cursor]
        cs.add_close(params)
        self.add_latest_cursor(cursor)
