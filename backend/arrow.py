import pyarrow as pa
import pyarrow.parquet as pq

__doc__ = ''' Adapter for pyarrow: turns stuff into Parquet files.'''
# How many rows are bufferd and flushed to the disk in one file. Bigger number means
# larger memory usage and less parquet files
BATCH_SIZE = 10000000
PARQUET_SCHEMA_VERSION = '0.4'

PARQUET_SCHEMA = pa.schema([
    ('span_id', pa.uint64()),
    ('sql_id', pa.string()),
    ('cursor_id', pa.string()),
    ('ops', pa.string()),
    ('cpu_time', pa.uint64()),
    ('elapsed_time', pa.uint64()),
    ('ph_reads', pa.uint64()),
    ('cr_reads', pa.uint64()),
    ('current_reads', pa.uint64()),
    ('cursor_missed', pa.uint8()),
    ('rows_processed', pa.uint64()),
    ('rec_call_dp', pa.uint8()),
    ('opt_goal', pa.uint8()),
    ('plh', pa.uint64()),
    ('tim', pa.uint64()),
    ('c_type', pa.uint8()),
    ('event_name', pa.string()),
    ('event_raw', pa.string()),
    ('file_name', pa.string()),
    ('line', pa.uint64()),
    ('ts', pa.timestamp('us')),
    # Next 6 is for PIC
    ('len', pa.uint32()),
    ('uid', pa.uint32()),
    ('oct', pa.uint16()),
    ('lid', pa.uint16()),
    ('hv', pa.uint64()),
    ('ad', pa.string()),
    # For XCTEND
    ('rlbk', pa.uint8()),
    ('rd_only', pa.uint8()),
    # For LOBs
    ('lobtype', pa.string()),
    ('bytes', pa.uint64()),
    ('sid', pa.string()),
    ('client_id', pa.string()),
    ('service_name', pa.string()),
    ('module', pa.string()),
    ('action', pa.string()),
    ('container_id', pa.uint16()),
    ('error_code', pa.uint16()), # Populated for the ERROR call
])

class Backend:
    def __init__(self, dbdir, prefix):
        self.dbdir = dbdir
        self.prefix = prefix
        self._span_id = 0
        self._ops_list = []
        self._flush_count = 0
        self._table = None
        self._row_count = 0
    def get_span_id(self):
        self._span_id += 1
        return self._span_id
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
    def _inject_schema_version(self):
        '''Adds Parquet schema version record to the generated file.'''
        schema_record = [[self.get_span_id()], [None], [None], ['HEADER'], [None], [None], [None],
                         [None], [None], [None], [None], [None], [None], [None], [None], [None],
                         ['PARQUET_SCHEMA'], [PARQUET_SCHEMA_VERSION], [None], [None], [None],
                         [None], [None], [None], [None], [None], [None], [None], [None], [None],
                         [None], [None], [None], [None], [None], [None], [None], [None]]
        #a = pa.array(zip(*schema_record))
        tbl = pa.Table.from_arrays(schema_record, schema = PARQUET_SCHEMA)
        if self._table:
            self._table = pa.concat_tables([self._table, tbl])
    def add_ops(self, span_id, sql_id, ops):
        ''' Adds list of ops to the batch. Checks the size of the _ops_list and _table and
            triggers flush if needed.'''
        self._ops_list += [o.astuple(span_id, sql_id) for o in ops]
        if len(self._ops_list) > BATCH_SIZE/100:
            self._batch2table()
        if self._row_count > BATCH_SIZE:
            self.flush_batches()
            self._row_count = 0
    def flush_batches(self):
        '''Flushes everything to the disk.'''
        self._inject_schema_version()
        pq.write_table(self._table, f'{self.dbdir}/{self.prefix}.{self._flush_count}',
                        compression='gzip')
        self._table = None
        self._flush_count += 1
    def flush(self):
        '''Flushes the _ops_list to the table, and table to the disk.'''
        if len(self._ops_list) > 0:
            self._batch2table()
        if self._table is not None:
            self.flush_batches()
