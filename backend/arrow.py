from concurrent.futures import ThreadPoolExecutor, Future
from typing import Optional
import pyarrow as pa
import pyarrow.parquet as pq

__doc__ = ''' Adapter for pyarrow: turns stuff into Parquet files.'''
# How many rows are bufferd and flushed to the disk in one file. Bigger number means
# larger memory usage and less parquet files
BUFFER_SIZE = 500000000
PARQUET_SCHEMA_VERSION = '0.4'
DEFAULT_FS = 'local'

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

def make_fs(dbdir: str, fstype: str, fsopt: dict) -> pa.fs.FileSystem:
    fs = None
    match fstype:
        case 'local':
            fs = pa.fs.LocalFileSystem(**fsopt)
            fs.create_dir(dbdir)
        case 's3':
            fs = pa.fs.S3FileSystem(**fsopt)
        case 'gcs':
            fs = pa.fs.GcsFileSystem(**fsopt)
        case 'hadoop':
            fs = pa.fs.HadoopFileSystem(**fsopt)
        case 'subtree':
            fs = pa.fs.SubTreeFileSystem(**fsopt)
    return fs

class Backend:
    def __init__(self, dbdir: str, prefix: str) -> None:
        self.dbdir = dbdir
        self.filename = f'{dbdir}/{prefix}'
        self._span_id: int = 0
        self._ops_list: list = []
        self._flush_count: int = 0
        self._table: Optional[pa.Table] = None
        self.future: Future = None
        self.executor = ThreadPoolExecutor(max_workers=1)

        self.set_fs()
    def set_fs(self, fstype: str = DEFAULT_FS, fopt: dict = {}) -> None:
        self.fs = make_fs(self.dbdir, fstype, fopt)
    def get_span_id(self) -> int:
        '''Span id generator'''
        self._span_id += 1
        return self._span_id
    def _batch2table(self) -> None:
        ''' Compresses self._ops_list into arrays, turns arrays into table and merges it
            with self._table.'''
        arrays = [pa.array(i) for i in zip(*self._ops_list)]
        tbl = pa.Table.from_arrays(arrays, schema = PARQUET_SCHEMA)
        if self._table:
            self._table = pa.concat_tables([self._table, tbl])
        else:
            self._table = tbl
        self._ops_list = []
    def _inject_schema_version(self) -> pa.Table:
        '''Adds Parquet schema version record to the generated file.'''
        schema_record = [[self.get_span_id()], [None], [None], ['HEADER'], [None], [None], [None],
                         [None], [None], [None], [None], [None], [None], [None], [None], [None],
                         ['PARQUET_SCHEMA'], [PARQUET_SCHEMA_VERSION], [None], [None], [None],
                         [None], [None], [None], [None], [None], [None], [None], [None], [None],
                         [None], [None], [None], [None], [None], [None], [None], [None]]
        #a = pa.array(zip(*schema_record))
        tbl = pa.Table.from_arrays(schema_record, schema = PARQUET_SCHEMA)
        return tbl
    def check_and_execute(self) -> None:
        '''Flushes the data to the disk in the background and checks if any of the previous
            flushes have completed'''
        if self.future:
            ex = self.future.exception()
            if ex:
                raise RuntimeError(ex)
        self.future = self.executor.submit(self.flush_batches, self._table)

    def add_ops(self, span_id: int, sql_id: str, ops) -> None:
        ''' Adds list of ops to the batch. Checks the size of the _ops_list and _table and
            triggers flush if needed.'''
        self._ops_list += [o.astuple(span_id, sql_id) for o in ops]
        if len(self._ops_list) > BUFFER_SIZE/100:
            self._batch2table()
        if self._table and self._table.get_total_buffer_size() > BUFFER_SIZE:
            self.check_and_execute()
            self._table = None
    def flush_batches(self, tbl) -> None:
        '''Flushes everything to the disk.'''
        sch = self._inject_schema_version()
        tbl = pa.concat_tables([tbl, sch])
        with self.fs.open_output_stream(f'{self.filename}.{self._flush_count}') as fstream:
            pq.write_table(tbl, fstream, compression='gzip')
            fstream.flush()
        del tbl
        self._flush_count += 1
    def flush(self) -> None:
        '''Flushes the _ops_list to the table, and table to the disk.'''
        if len(self._ops_list) > 0:
            self._batch2table()
        if self._table is not None:
            self.check_and_execute()
            self._table = None
        ex = self.future.exception()
        if ex:
            raise RuntimeError(ex)
