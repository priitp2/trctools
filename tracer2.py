#!/usr/bin/env python3

import argparse
from hdrh.histogram import HdrHistogram
import re
import sys
from scipy.stats import shapiro,kstest
from statement import Statement
from oracle import DB
from current_statement import CurrentStatement
import util
from cursor_tracker import CursorTracker

max_fetch_elapsed = 500000
max_exec_elapsed = 500000
cursors = {}
statements = {}
latest_waits = []
tracker = CursorTracker(cursors, statements)

def handle_parsing(cursor, params):
    if args.norm or args.db:
        record_data = True
    else:
        record_data = False
    tracker.add_parsing_in(cursor, params)
    return (cursor, None)

def split_event(ev):
    out = {}
    for item in ev.split(','):
        key = item.split('=')
        out[key[0]] = key[1]
    return out

def get_ce(params):
    for item in params.split(','):
        key = item.split('=')
        if key[0] == 'c':
            cpu = int(key[1])
        if key[0] == 'e':
            elapsed = int(key[1])
    return (cpu, elapsed)

def handle_parse(cursor, params):
    ev = split_event(params)
    id = []
    id.append(-1)
    if args.db:
        ev['parent_id'] = 0
        ev['cursor'] = cursor
        ev['event'] = 'PARSE'
        ev['type'] = 0
        id = database.add_event(ev)
    return (cursor, int(ev['c']), int(ev['e']), id[0])

def handle_exec(cursor, params):
    ce = get_ce(params)
    id = []
    id.append(-1)
    if args.db:
        ev = split_event(params)
        ev['parent_id'] = 0
        ev['cursor'] = cursor
        ev['event'] = 'EXEC'
        ev['type'] = 0
        id = database.add_event(ev)
    return (cursor, int(ce[0]), int(ce[1]), id[0])
#    print(statement)
#    print("handle_exec1: cursor = {}, params = {}, sql_id = {}".format(cursor, params, cursors[cursor]))

def handle_fetch(cursor, params, last_exec, last_exec_id, last_parse):
    if len(last_exec) > 0 and cursor != last_exec[0]:
        raise("handle_fetch: cursor mismatch: cursor = {}, cursor from last_exec = {}".format(cursor, last_exec[0]))
    ce = get_ce(params)

    lat = (cursor, ce[0], ce[1])
    id = -1
    if args.db:
        ev = split_event(params)
        ev['parent_id'] = last_exec_id
        ev['cursor'] = cursor
        ev['event'] = 'FETCH'
        ev['type'] = 0
        id = database.add_event(ev)
    return lat

def handle_wait(cursor, params):
    #match = re.match(r""" nam=([:alnum:]+) ela = (\d+) (.*) tim=(\d+)""", params)
    wait = {}
    match = re.match(r""" nam='(.*)' ela= (\d+) (.*) tim=(\d+)""", params)
    if match:
        wait['name'] = match.group(1)
        wait['elapsed'] = match.group(2)
        wait['timestamp'] = match.group(4)
        latest_waits.append(wait)
        return (cursor, 0, int(match.group(2)), params)
    else:
        print("handle_wait: no match: cursor={}, params = ->{}<-".format(cursor, params))

def handle_close(cursor, params):
    ce = get_ce(params)
    return (cursor, ce[0], ce[1])

def print_naughty_exec(cs):
    lat = cs.merge()
    if lat[2] > max_exec_elapsed:
        print('----------------------------------------------')
        statement = statements[cursors[lat[0]]]
        print("sql_id = {}, cursor = {}, elapsed = {}, fetches = {}".format(statement.sql_id, lat[0], lat[2], len(cs.fetches)))
        if len(cs.fetches) < 10:
            for f in cs.fetches:
                print("     {}".format(f))
        else:
            elapsed = util.merge_lat_objects((cs.cursor, 0, 0), cs.fetches)
            print("    fetches = {}, elapsed = {}".format(len(cs.fetches), elapsed[2]))
        if len(cs.waits) < 10:
            for w in cs.waits:
                print("     {}".format(w[3]))
        else:
            elapsed = util.merge_lat_objects((cs.cursor, 0, 0), cs.waits)
            print("    waits = {}, elapsed = {}".format(len(cs.waits), elapsed[2]))
        print('----------------------------------------------')

parser = argparse.ArgumentParser(description='Do stuff with Oracle 19c trace files')
parser.add_argument('trace_files', metavar='files', type=str, nargs='+',
                            help='Trace files to process')
parser.add_argument('--sql_id', type=str, dest='sqlid',
                            help="Comma separated list of sql_id's for which histograms are produced")
parser.add_argument('--norm', type=bool, default = False, dest='norm',
                            help="Perform Shapiro-Wilk normality test on values")
parser.add_argument('--db', type=bool, default = False, dest='db',
                            help="persists raw data in the db")
parser.add_argument('--merge_all', type=bool, default = False, dest='merge_all',
                            help="Merges all sql statements into one histogram. Helpful without the bind variables")
args = parser.parse_args()

if args.db:
    database = DB()

for fname in args.trace_files:
    print("Processing {}".format(fname))
    with open(fname, 'r') as f:
        current_statement = None
        last_exec = ()
        last_parse = ()
        last_parse_id = -1
        last_exec_id = -1
        for line in f:
            match = re.match(r'''^(PARSE|PARSING IN CURSOR|EXEC|FETCH|WAIT|CLOSE|BINDS) (#\d+)(:| )(.*)''', line)
            if match:
                #print(match.groups())
                if match.group(1) == 'PARSING IN CURSOR':
                    p = handle_parsing(match.group(2), match.group(4))
                    latest_waits = []
                if match.group(1) == 'PARSE':
                    last_parse = handle_parse(match.group(2), match.group(4))
                    last_parse_id = last_parse[3]
                    cs = tracker.add_parse(match.group(2), last_parse)
                    print_naughty_exec(cs)
                if match.group(1) == 'EXEC':
                    last_exec = handle_exec(match.group(2), match.group(4))
                    last_exec_id = last_exec[3]
                    cs = tracker.add_exec(match.group(2), last_exec)
                    if cs != None:
                        print_naughty_exec(cs)
                if match.group(1) == 'FETCH':
                    # FIXME: fetches should be added to execs, not other way around
                    f = handle_fetch(match.group(2), match.group(4), last_exec, last_exec_id, last_parse)
                    tracker.add_fetch(match.group(2), f)
                if match.group(1) == 'WAIT':
                    w = handle_wait(match.group(2), match.group(4))
                    tracker.add_wait(match.group(2), w)
                if match.group(1) == 'CLOSE':
                    c = handle_close(match.group(2), match.group(4))
                    last_parse = ()
                    last_parse_id = -1
                    last_exec_id = -1
                    cs = tracker.add_close(match.group(2), c)
                    print_naughty_exec(cs)

                if match.group(1) == 'BINDS':
                    pass

#for c in cursors.keys():
#    print("cursor: {}, sql_id: {}".format(c, cursors[c]))
#for s in statements.values():
#    print(s)

# Silly bandaid in case sqlids argument isn't specified
if args.sqlid:
    ids = args.sqlid.split(',')
else:
    ids = [statements[s].sql_id for s in statements.keys()]

if args.merge_all:
    exec_hist_elapsed = HdrHistogram(1, 1000000000, 1)
    exec_hist_cpu = HdrHistogram(1, 1000000000, 1)
    fetch_hist_elapsed = HdrHistogram(1, 1000000000, 1)
    fetch_hist_cpu = HdrHistogram(1, 1000000000, 1)

    for c in statements.keys():
        stat = statements[c]
        exec_hist_elapsed.add(stat.exec_hist_elapsed)
        exec_hist_cpu.add(stat.exec_hist_cpu)
        fetch_hist_elapsed.add(stat.fetch_hist_elapsed)
        fetch_hist_cpu.add(stat.fetch_hist_cpu)
    with open("exec_hist_elapsed_all.out", 'wb') as f:
        exec_hist_elapsed.output_percentile_distribution(f, 1.0)
    with open("exec_hist_cpu_all.out", 'wb') as f:
        exec_hist_cpu.output_percentile_distribution(f, 1.0)
    with open("fetch_hist_elapsed_all.out", 'wb') as f:
        fetch_hist_elapsed.output_percentile_distribution(f, 1.0)
    with open("fetch_hist_cpu_all.out", 'wb') as f:
        fetch_hist_cpu.output_percentile_distribution(f, 1.0)
    sys.exit(0)

for c in statements.keys():
    stat = statements[c]
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
            if args.db:
                database.add_rows(stat.sql_id, stat.exec_elapsed)
        with open("exec_hist_cpu_{}.out".format(stat.sql_id), 'wb') as f:
            stat.exec_hist_cpu.output_percentile_distribution(f, 1.0)
        with open("fetch_hist_elapsed_{}.out".format(stat.sql_id), 'wb') as f:
            stat.fetch_hist_elapsed.output_percentile_distribution(f, 1.0)
        with open("fetch_hist_cpu_{}.out".format(stat.sql_id), 'wb') as f:
            stat.fetch_hist_cpu.output_percentile_distribution(f, 1.0)

