from ops import Ops

class Statement:
    def __init__(self, cursor, params):
        self.cursor = cursor

        for item in params.split():
            key = item.split('=')
            if key[0] == 'len':
                self.statement_length = key[1]
            if key[0] == 'dep':
                self.rec_depth = key[1]
            if key[0] == 'uid':
                self.schema_uid = key[1]
            if key[0] == 'oct':
                self.command_type = key[1]
            if key[0] == 'lid':
                self.priv_user_id = key[1]
            if key[0] == 'tim':
                self.timestamp = key[1]
            if key[0] == 'hv':
                self.hash_id = key[1]
            if key[0] == 'ad':
                self.address = key[1].strip("'")
            if key[0] == 'sqlid':
                self.sql_id = key[1].strip("'")

        self.execs = 0
        self.fetches = 0

    def increase_exec_count(self):
        self.execs = self.execs + 1
    def add_current_statement(self, s):
        if s.exec:
            self.increase_exec_count()
        self.fetches += s.fetch_count
