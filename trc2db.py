#!/usr/bin/env python3.11

import argparse
import time
import parser as trcparser
from call_tracker import CallTracker

__doc__ = "Turn Oracle SQL trace files into Parquet, or put them into a Oracle database"

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('trace_files', metavar='files', type=str, nargs='+',
                            help='Trace files to process')
parser.add_argument('--db', type=str, default = 'parquet', dest='db',
                    help="Persists raw data in the db, supported implementations: oracle, parquet")
parser.add_argument('--dbdir', type=str, default = './', dest='dbdir',
                    help="Directory for the parquet files")
parser.add_argument('--db-file-prefix', type=str, default = 'parquet', dest='file_prefix',
                    help="Parquet file name prefix")
parser.add_argument('--log-orphans', type=bool, default = False, dest='orphans',
                    help="Logs lines not matched by the parser")

args = parser.parse_args()

if args.db == 'oracle':
    print('Using database: oracle')
    from db.oracle import DB
    database = DB()
elif args.db == 'parquet':
    print('Using database: arrow/parquet')
    from db.arrow import DB
    database = DB(args.dbdir, args.file_prefix)
else:
    print('Using database: None')
    database = None


tracker = CallTracker(database)

no_files = len(args.trace_files)
fcount = 1
cumul_lines = 0
start_time = time.time_ns()
for fname in args.trace_files:
    print(f"[{fcount}/{no_files}] processing file {fname}")
    start = time.time_ns()
    lines = trcparser.process_file(tracker, fname, args.orphans)
    cumul_lines += lines
    fcount += 1
    print(f"   -> {lines} lines, {int((time.time_ns() - start)/1000000000)} seconds")

tracker.flush()
print(f"Processed {cumul_lines} lines in {int((time.time_ns() - start_time)/1000000000)} seconds")
