import pyarrow as pa
import pyarrow.parquet as pq

class DB:
    def __init__(self, dbdir, prefix):
        # How many rows are bufferd and flushed to the disk in one file. Bigger number means
        # larger memory usage and less parquet files
        self.max_batch_size = 10000000
        self.dbdir = dbdir
        self.prefix = prefix
        self.cursor_exec_schema = pa.schema([
            ('exec_id', pa.int64()),
            ('sql_id', pa.string()),
            ('cursor_id', pa.string()),
            ('ops', pa.string()),
            ('cpu_time', pa.int64()),
            ('elapsed_time', pa.int64()),
            ('ph_reads', pa.int64()),
            ('cr_reads', pa.int64()),
            ('current_reads', pa.int64()),
            ('cursor_missed', pa.int64()),
            ('rows_processed', pa.int64()),
            ('rec_call_dp', pa.int64()),
            ('opt_goal', pa.int64()),
            ('plh', pa.int64()),
            ('tim', pa.int64()),
            ('c_type', pa.int64()),
            ('event_name', pa.string()),
            ('event_raw', pa.string()),
            ('file_name', pa.string()),
            ('line', pa.int64()),
            ('ts', pa.timestamp('us')),
            # Next 6 is for PIC
            ('len', pa.int64()),
            ('uid', pa.int64()),
            ('oct', pa.int64()),
            ('lid', pa.int64()),
            ('hv', pa.int64()),
            ('ad', pa.string()),
            # For XCTEND
            ('rlbk', pa.string()),
            ('rd_only', pa.string()),
            # For LOBs
            ('lobtype', pa.string()),
            ('bytes', pa.int64()),
            ])
        self.exec_id = 0
        self.batches = []
        self.flush_count = 0
        self.table = None
        self.row_count = 0
    def add_rows(self, sql_id, rt):
        pass
    def add_cursor(self, c):
        pass
    def get_exec_id(self):
        self.exec_id += 1
        return self.exec_id
    def _batch2table(self):
        self.row_count += len(self.batches)
        arrays = [pa.array(i) for i in zip(*self.batches)]
        tbl = pa.Table.from_arrays(arrays, schema = self.cursor_exec_schema)
        if self.table:
            self.table = pa.concat_tables([self.table, tbl])
        else:
            self.table = tbl
        self.batches = []
    def insert_ops(self, ops):
        if len(ops) == 0:
            return
        self.batches.append(ops)
        if len(self.batches) > self.max_batch_size/100:
            self._batch2table()
        if self.row_count > self.max_batch_size:
            self.flush_batches()
            self.row_count = 0
    def flush_batches(self):
        pq.write_table(self.table, f'{self.dbdir}/{self.prefix}.{self.flush_count}',
                        compression='gzip')
        self.table = None
        self.flush_count += 1
    def flush(self):
        if len(self.batches) > 0:
            self._batch2table()
        if self.table.num_rows > 0:
            self.flush_batches()
