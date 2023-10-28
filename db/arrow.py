import pyarrow as pa
import pyarrow.parquet as pq

__doc__ = ''' Adapter for pyarrow: turns stuff into Parquet files.'''
# How many rows are bufferd and flushed to the disk in one file. Bigger number means
# larger memory usage and less parquet files
BATCH_SIZE = 10000000

PARQUET_SCHEMA = pa.schema([
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
    ('sid', pa.string()),
    ('client_id', pa.string()),
    ('service_name', pa.string()),
    ('module', pa.string()),
    ('action', pa.string()),
    ('container_id', pa.int16()),
])

class DB:
    def __init__(self, dbdir, prefix):
        self.dbdir = dbdir
        self.prefix = prefix
        self._exec_id = 0
        self._ops_list = []
        self._flush_count = 0
        self._table = None
        self._row_count = 0
    def get_exec_id(self):
        self._exec_id += 1
        return self._exec_id
    def _batch2table(self):
        ''' Compresses self._ops_list into arrays, turns arrays into table and merges it
            with self._table.'''
        self._row_count += len(self._ops_list)
        arrays = [pa.array(i) for i in zip(*self._ops_list)]
        tbl = pa.Table.from_arrays(arrays, schema = PARQUET_SCHEMA)
        if self._table:
            self._table = pa.concat_tables([self._table, tbl])
        else:
            self._table = tbl
        self._ops_list = []
    def insert_ops(self, ops):
        ''' Adds ops to the list. Checks the size of the _ops_list and _table.'''
        if len(ops) == 0:
            return
        self._ops_list.append(ops)
        if len(self._ops_list) > BATCH_SIZE/100:
            self._batch2table()
        if self._row_count > BATCH_SIZE:
            self.flush_batches()
            self._row_count = 0
    def flush_batches(self):
        '''Flushes everything to the disk.'''
        pq.write_table(self._table, f'{self.dbdir}/{self.prefix}.{self._flush_count}',
                        compression='gzip')
        self._table = None
        self._flush_count += 1
    def flush(self):
        '''Flushes the _ops_list to the table, and table to the disk.'''
        if len(self._ops_list) > 0:
            self._batch2table()
        if self._table.num_rows > 0:
            self.flush_batches()
