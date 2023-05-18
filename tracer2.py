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
parser.add_argument('--merge_all', type=bool, default = False, dest='merge_all',
                            help="Merges all sql statements into one histogram. Helpful without the bind variables")
parser.add_argument('--logfile', type=str, default = None, dest='logfile', help="Sends output to the file")
parser.add_argument('--log-level', type=str, default = None, dest='loglevel', help="Sets logging level: from DEBUG to CRITICAL")

args = parser.parse_args()

if args.db == 'oracle':
    from oracle import DB
    database = DB()
elif args.db == 'parquet':
    from arrow import DB
    database = DB()
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

no_files = len(args.trace_files)
fcount = 1
for fname in args.trace_files:
    print("[{}/{}] processing file {}".format(fcount, no_files, fname))
    util.process_file(tracker, fname)
    fcount += 1
    p = PurePath(fname)
    tracker.flush(p.stem)

# Silly bandaid in case sqlids argument isn't specified
if args.sqlid:
    ids = args.sqlid.split(',')
else:
    ids = [tracker.statements[s].sql_id for s in tracker.statements.keys()]

if args.merge_all:
    exec_hist_elapsed = HdrHistogram(1, 1000000000, 1)
    exec_hist_cpu = HdrHistogram(1, 1000000000, 1)
    fetch_hist_elapsed = HdrHistogram(1, 1000000000, 1)
    fetch_hist_cpu = HdrHistogram(1, 1000000000, 1)
    response_hist = HdrHistogram(1, 1000000000, 1)

    for c in tracker.statements.keys():
        stat = tracker.statements[c]
        exec_hist_elapsed.add(stat.exec_hist_elapsed)
        exec_hist_cpu.add(stat.exec_hist_cpu)
        response_hist.add(stat.resp_hist)
    with open("exec_hist_elapsed_all.out", 'wb') as f:
        exec_hist_elapsed.output_percentile_distribution(f, 1.0)
    with open("exec_hist_cpu_all.out", 'wb') as f:
        exec_hist_cpu.output_percentile_distribution(f, 1.0)
    with open("response_hist_all.out", 'wb') as f:
        response_hist.output_percentile_distribution(f, 1.0)
    sys.exit(0)

for c in tracker.statements.keys():
    stat = tracker.statements[c]
    if stat.sql_id in ids:
        print('----------------------------------------')
        print("sql_id: {}, execs: {}, fetches: {}".format(stat.sql_id, stat.execs, stat.fetches))
        if args.norm:
            print("Normality test(Shapiro-Wilkes) on cpu: {}".format(shapiro(stat.exec_cpu)))
            print("Normality test(Shapiro-Wilkes) on elapsed: {}".format(shapiro(stat.fetch_elapsed)))
            print("Normality test(Kolmogorov-Smirnov) on cpu: {}".format(kstest(stat.exec_cpu, 'norm')))
            print("Normality test(Kolmogorov-Smirnov) on elapsed: {}".format(kstest(stat.fetch_elapsed, 'norm')))
        with open("exec_hist_elapsed_{}.out".format(stat.sql_id), 'wb') as f:
            stat.exec_hist_elapsed.output_percentile_distribution(f, 1.0)
        with open("exec_hist_cpu_{}.out".format(stat.sql_id), 'wb') as f:
            stat.exec_hist_cpu.output_percentile_distribution(f, 1.0)
        with open("response_hist_{}.out".format(stat.sql_id), 'wb') as f:
            stat.resp_hist.output_percentile_distribution(f, 1.0)
        with open("response_hist_without_waits_{}.out".format(stat.sql_id), 'wb') as f:
            stat.resp_without_waits_hist.output_percentile_distribution(f, 1.0)

