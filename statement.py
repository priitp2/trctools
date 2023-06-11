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
