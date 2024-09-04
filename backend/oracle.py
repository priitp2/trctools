import oracledb
import sys

class Backend:
    def __init__(self):
        self.connection = oracledb.connect(
            user="",
            password='',
            dsn="<hostname>/orclpdb1")
        self.batches = []
        self.span_ids = []
        # should match with sequence cursor_span_id increment by
        self.seq_batch_size = 100
        self.max_batch_size = 1000

    def get_span_id(self):
        if len(self.span_ids) == 0:
            cursor = self.connection.cursor()
            out = cursor.execute('select cursor_span_id.nextval from dual')
            nextval = out.fetchone()[0]
            self.span_ids = [i for i in range(nextval - self.seq_batch_size + 1, nextval)]
        return self.span_ids.pop()
    def insert_single_ops(self, ops):
        if len(ops) == 0:
            return
        insert_ops([ops])
    def insert_ops(self, ops):
        if len(ops) == 0:
            return
        self.batches += ops
        if len(self.batches) > self.max_batch_size:
            self.flush_batches()
    def flush_batches(self):
        if len(self.batches) == 0:
            return
        cursor = self.connection.cursor()
        try:
            cursor.executemany("insert into dbcall(span_id, sql_id, cursor_id, ops, cpu_time, elapsed_time, ph_reads, cr_reads, current_reads, cursor_missed, rows_processed, rec_call_dp, opt_goal, plh, tim, c_type, wait_name, wait_raw, file_name, line, ts, len, pic_uid, oct, lid, hv, ad, rlbk, rd_only, lobtype, bytes) values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, substr(:18, 0, 4000), :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32)", self.batches)
        except Exception as e:
            print(self.batches)
            print(e)
        self.connection.commit()
        self.batches = []
    def flush(self, fname):
        self.flush_batches()

