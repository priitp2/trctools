#!/usr/bin/env python3.11

import argparse
import re
import sys
#from scipy.stats import shapiro,kstest
from util import Filer
from cursor_tracker import CursorTracker
from pathlib import PurePath
import logging
import time

max_fetch_elapsed = 500000
max_exec_elapsed = 500000

parser = argparse.ArgumentParser(description='Do stuff with Oracle 19c trace files')
parser.add_argument('trace_files', metavar='files', type=str, nargs='+',
                            help='Trace files to process')
parser.add_argument('--norm', type=bool, default = False, dest='norm',
                            help="Perform Shapiro-Wilk normality test on values")
parser.add_argument('--db', type=str, default = None, dest='db', help="Persists raw data in the db, supported implementations: oracle, parquet")
parser.add_argument('--dbdir', type=str, default = 'arrow', dest='dbdir', help="Directory for the parquet files")
parser.add_argument('--logfile', type=str, default = None, dest='logfile', help="Sends output to the file")
parser.add_argument('--log-level', type=str, default = None, dest='loglevel', help="Sets logging level: from DEBUG to CRITICAL")

args = parser.parse_args()

log_level = logging.INFO

if args.loglevel:
    log_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(log_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
logging.basicConfig(filename = args.logfile, level = log_level, format = '%(asctime)s - %(name)s - %(levelname)s:%(message)s')

#if args.logfile:
#    logging.basicConfig(args.logfile)

if args.db == 'oracle':
    logging.info('Using database: oracle')
    from oracle import DB
    database = DB()
elif args.db == 'parquet':
    logging.info('Using database: arrow/parquet')
    from arrow import DB
    database = DB(args.dbdir)
else:
    logging.info('Using database: None')
    database = None


tracker = CursorTracker(database)

no_files = len(args.trace_files)
fcount = 1
cumul_lines = 0
cumul_time = 0
filer = Filer()
for fname in args.trace_files:
    p = PurePath(fname)
    tracker.db.set_filename(p.stem)
    print("[{}/{}] processing file {}".format(fcount, no_files, fname))
    start = time.time_ns()
    lines = filer.process_file(tracker, fname)
    cumul_lines += lines
    delta = int((time.time_ns() - start)/1000000000)
    cumul_time += delta
    fcount += 1
    print("   -> {} lines, {} seconds".format(lines, delta))

print("tracker.latest_cursors = {}".format(len(tracker.latest_cursors)))
tracker.flush(p.stem)
print("Processed {} lines in {} seconds".format(cumul_lines, cumul_time))
