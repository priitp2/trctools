#!/usr/bin/env python3.11

import argparse
import logging
import time
from filer import Filer
from call_tracker import CallTracker

max_fetch_elapsed = 500000
max_exec_elapsed = 500000

parser = argparse.ArgumentParser(description='Turn Oracle SQL trace files into Parquet, or put them " \
                                                +"into a Oracle database')
parser.add_argument('trace_files', metavar='files', type=str, nargs='+',
                            help='Trace files to process')
parser.add_argument('--db', type=str, default = 'parquet', dest='db',
                    help="Persists raw data in the db, supported implementations: oracle, parquet")
parser.add_argument('--dbdir', type=str, default = './', dest='dbdir',
                    help="Directory for the parquet files")
parser.add_argument('--db-file-prefix', type=str, default = 'parquet', dest='file_prefix',
                    help="Parquet file name prefix")
parser.add_argument('--logfile', type=str, default = 'trc2db.log', dest='logfile',
                    help="Sends output to the logfile")
parser.add_argument('--log-level', type=str, default = 'ERROR', dest='loglevel',
                    help="Sets logging level: from DEBUG to CRITICAL")

args = parser.parse_args()

log_level = logging.INFO

if args.loglevel:
    log_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(log_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
logging.basicConfig(filename = args.logfile, level = log_level,
        format = '%(asctime)s - %(name)s - %(levelname)s:%(message)s')

#if args.logfile:
#    logging.basicConfig(args.logfile)

if args.db == 'oracle':
    logging.info('Using database: oracle')
    from db.oracle import DB
    database = DB()
elif args.db == 'parquet':
    logging.info('Using database: arrow/parquet')
    from db.arrow import DB
    database = DB(args.dbdir, args.file_prefix)
else:
    logging.info('Using database: None')
    database = None


tracker = CallTracker(database)

no_files = len(args.trace_files)
fcount = 1
cumul_lines = 0
filer = Filer()
start_time = time.time_ns()
for fname in args.trace_files:
    print(f"[{fcount}/{no_files}] processing file {fname}")
    start = time.time_ns()
    lines = filer.process_file(tracker, fname)
    cumul_lines += lines
    fcount += 1
    print(f"   -> {lines} lines, {int((time.time_ns() - start)/1000000000)} seconds")

tracker.flush()
print(f"Processed {cumul_lines} lines in {int((time.time_ns() - start_time)/1000000000)} seconds")
