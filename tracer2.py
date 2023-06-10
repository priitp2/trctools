#!/usr/bin/env python3.9

import argparse
import re
import sys
from scipy.stats import shapiro,kstest
import util
from cursor_tracker import CursorTracker
from pathlib import PurePath
import logging

max_fetch_elapsed = 500000
max_exec_elapsed = 500000

parser = argparse.ArgumentParser(description='Do stuff with Oracle 19c trace files')
parser.add_argument('trace_files', metavar='files', type=str, nargs='+',
                            help='Trace files to process')
parser.add_argument('--sql_id', type=str, dest='sqlid',
                            help="Comma separated list of sql_id's for which histograms are produced")
parser.add_argument('--norm', type=bool, default = False, dest='norm',
                            help="Perform Shapiro-Wilk normality test on values")
parser.add_argument('--db', type=str, default = None, dest='db', help="Persists raw data in the db, supported implementations: oracle, parquet")
parser.add_argument('--dbdir', type=str, default = 'arrow', dest='dbdir', help="Directory for the parquet files")
parser.add_argument('--logfile', type=str, default = None, dest='logfile', help="Sends output to the file")
parser.add_argument('--log-level', type=str, default = None, dest='loglevel', help="Sets logging level: from DEBUG to CRITICAL")

args = parser.parse_args()

if args.db == 'oracle':
    from oracle import DB
    database = DB()
elif args.db == 'parquet':
    from arrow import DB
    database = DB(args.dbdir)
else:
    database = None

log_level = logging.INFO

if args.loglevel:
    log_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
logging.basicConfig(level = log_level)

if args.logfile:
    logging.basicConfig(args.logfile)

tracker = CursorTracker(database)

# Silly bandaid in case sqlids argument isn't specified
if args.sqlid:
    ids = args.sqlid.split(',')
else:
    ids = []

no_files = len(args.trace_files)
fcount = 1
for fname in args.trace_files:
    print("[{}/{}] processing file {}".format(fcount, no_files, fname))
    util.process_file(tracker, fname, ids)
    fcount += 1
    p = PurePath(fname)
    tracker.flush(p.stem)

