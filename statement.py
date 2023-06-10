from hdrh.histogram import HdrHistogram
import util
from ops import Ops

class Statement:
    def __init__(self, cursor, params, db):
        self.cursor = cursor
        self.db = db

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

        self.exec_hist_elapsed = HdrHistogram(1, 1000000000, 1)
        self.exec_hist_cpu = HdrHistogram(1, 1000000000, 1)

        self.resp_hist = HdrHistogram(1, 1000000000, 1)

    def increase_exec_count(self):
        self.execs = self.execs + 1
    def record_exec_cpu(self, cpu):
        self.exec_hist_cpu.record_value(cpu)
    def record_exec_elapsed(self, elapsed):
        self.exec_hist_elapsed.record_value(elapsed)
    def add_current_statement(self, s):
        lat = s.merge()
        if s.exec:
            self.record_exec_cpu(lat.c)
            self.record_exec_elapsed(lat.e)
            self.increase_exec_count()
        self.fetches += s.fetch_count

        elapsed = s.get_elapsed()
        if elapsed:
            self.resp_hist.record_value(elapsed)

        if self.db:
            s.dump_to_db()

