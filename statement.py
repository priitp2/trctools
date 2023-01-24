from hdrh.histogram import HdrHistogram

class Statement:
    def __init__(self, cursor, params, norm):
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
                self.address = key[1]
            if key[0] == 'sqlid':
                self.sql_id = key[1].replace("'", "")

        self.execs = 0
        self.fetches = 0
        self.norm = False

        self.exec_hist_elapsed = HdrHistogram(1, 1000000000, 1)
        self.exec_hist_cpu = HdrHistogram(1, 1000000000, 1)
        self.fetch_hist_elapsed = HdrHistogram(1, 1000000000, 1)
        self.fetch_hist_cpu = HdrHistogram(1, 1000000000, 1)

        if norm:
            self.exec_elapsed = []
            self.exec_cpu = []
            self.fetch_elapsed = []
            self.fetch_cpu = []
            self.norm = True

    def increase_exec_count(self):
        self.execs = self.execs + 1
    def increase_fetch_count(self):
        self.fetches = self.fetches + 1
    def record_exec_cpu(self, cpu):
        self.exec_hist_cpu.record_value(cpu)
        if self.norm:
            self.exec_cpu.append(cpu)
    def record_exec_elapsed(self, elapsed):
        self.exec_hist_elapsed.record_value(elapsed)
        if self.norm:
            self.exec_elapsed.append(elapsed)
    def record_fetch_cpu(self, cpu):
        self.fetch_hist_cpu.record_value(cpu)
        if self.norm:
            self.fetch_cpu.append(cpu)
    def record_fetch_elapsed(self, elapsed):
        self.fetch_hist_elapsed.record_value(elapsed)
        if self.norm:
            self.fetch_elapsed.append(elapsed)

