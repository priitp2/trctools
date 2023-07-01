import pyarrow as pa
from pyarrow import fs
import pyarrow.parquet as pq
import logging

class DB:
    def __init__(self, dbdir):
        self.logger = logging.getLogger(__name__)
        self.max_batch_size = 8000000
        self.dbdir = dbdir
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
            ('ts', pa.int64()),
            ('c_type', pa.int64()),
            ('wait_name', pa.string()),
            ('wait_raw', pa.string()),
            ('file_name', pa.string()),
            ('line', pa.int64()),
            ])
        self.exec_id = 0
        self.batches = []
        self.cursor_statement_schema = pa.schema([
            ('cursor_id', pa.string()),
            ('sql_id', pa.string())
            ])
        self.cursor_statement_batches = []
        self.fname = None
        self.flush_count = 0
    def add_rows(self, sql_id, rt):
        pass
    def add_cursor(self, c):
        pass
    def get_exec_id(self):
        self.exec_id += 1
        return self.exec_id
    def insert_ops(self, ops):
        if len(ops) == 0:
            return
        self.batches.append(ops)
        #self.batches.append(ops)
        #batch = pa.record_batch([i for i in zip(ops)], self.cursor_exec_schema)
        #self.batches.append(batch)
        if len(self.batches) > self.max_batch_size:
            self.flush_batches()
    def insert_cursors(self, cs):
        batch = pa.record_batch([i for i in zip(*cs)], self.cursor_statement_schema)
        self.cursor_statement_batches.append(batch)
    def set_filename(self, fname):
        self.fname = fname
        self.flush_count = 0
    def flush_batches(self):
        #batch = pa.record_batch([i for i in zip(*self.batches)], self.cursor_exec_schema)
        self.logger.debug('flush_batches: flusing %d records', len(self.batches))

        arrays = [pa.array(i) for i in zip(*self.batches)]
        #batch = pa.record_batch(arrays, self.cursor_exec_schema)
        self.logger.debug('flush_batches: arrays done')

        table = pa.Table.from_arrays(arrays, schema = self.cursor_exec_schema)
        self.logger.debug('flush_batches: table done')

        pq.write_table(table, '{}/trace/{}.parquet.{}'.format(self.dbdir, self.fname, self.flush_count), compression='gzip')
        self.logger.debug('flush_batches: table written')

        self.batches = []
        self.flush_count += 1
    def flush(self, fname):
        if len(self.batches) == 0:
            return
        #table = pa.Table.from_batches(self.batches)
        #local = fs.LocalFileSystem()

        #with local.open_output_stream(fname +'.gz') as file:
        #    with pa.RecordBatchFileWriter(file, table.schema) as writer:
        #        writer.write_table(table)

        #pq.write_table(table, '{}/trace/{}.parquet'.format(self.dbdir, fname), compression='gzip')
        #self.batches = []
        if not self.fname:
            self.fname = fname
        self.flush_batches()

        table = pa.Table.from_batches(self.cursor_statement_batches)
        pq.write_table(table, '{}/cursors/{}.parquet'.format(self.dbdir, fname), compression='gzip')
        self.cursor_statement_batches = []

