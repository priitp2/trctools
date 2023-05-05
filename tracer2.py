#!/usr/bin/env python3.9

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
tracker = CursorTracker()

def handle_parsing(cursor, params):
    if args.norm or args.db:
        record_data = True
    else:
        record_data = False
    tracker.add_parsing_in(cursor, params)
    return (cursor, None)

def handle_parse(cursor, params):
    ev = util.split_event(params)
    id = []
    id.append(-1)
    if args.db:
        ev['parent_id'] = 0
        ev['cursor'] = cursor
        ev['event'] = 'PARSE'
        ev['type'] = 0
        id = database.add_event(ev)
    return (cursor, int(ev['c']), int(ev['e']), id[0], ev)

def handle_exec(cursor, params):
    ev = util.split_event(params)
    id = []
    id.append(-1)
    if args.db:
        ev = util.split_event(params)
        ev['parent_id'] = 0
        ev['cursor'] = cursor
        ev['event'] = 'EXEC'
        ev['type'] = 0
        id = database.add_event(ev)
    return (cursor, int(ev['c']), int(ev['e']), id[0], ev)
#    print(statement)
#    print("handle_exec1: cursor = {}, params = {}, sql_id = {}".format(cursor, params, cursors[cursor]))

def handle_fetch(cursor, params):
    ev = util.split_event(params)

    lat = (cursor, int(ev['c']), int(ev['e']), ev)
    id = -1
    if args.db:
        ev = util.split_event(params)
        ev['parent_id'] = -1
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
        return (cursor, 0, int(match.group(2)), wait)
    else:
        print("handle_wait: no match: cursor={}, params = ->{}<-".format(cursor, params))

def handle_close(cursor, params):
    ev = util.split_event(params)
    return (cursor, int(ev['c']), int(ev['e']), ev)

def print_naughty_exec(cs):
    lat = cs.merge()
    if lat[2] > max_exec_elapsed:
        print('----------------------------------------------')
        if lat[0] not in tracker.cursors.keys():
            print("print_naughty_exec: missing cursor {}".format(lat[0]))
            return
        statement = tracker.statements[tracker.cursors[lat[0]]]
        print("sql_id = {}, cursor = {}, elapsed = {}, fetches = {}".format(statement.sql_id, lat[0], lat[2], cs.fetch_count))
        if cs.exec:
            print("    exec: cpu = {}, elapsed = {}, timestamp = {}".format(cs.exec[1], cs.exec[2], cs.exec[4]['tim']))
        if cs.fetch_count < cs.max_list_size:
            for f in cs.fetches:
                print("     {}".format(f))
        else:
            elapsed = util.merge_lat_objects((cs.cursor, 0, 0), cs.fetches)
            print("    fetches = {}, elapsed = {}".format(cs.fetch_count, elapsed[2]))
        if cs.wait_count < cs.max_list_size:
            for w in cs.waits:
                print("     {}".format(w[3]))
        else:
            elapsed = util.merge_lat_objects((cs.cursor, 0, 0), cs.waits)
            print("    waits = {}, elapsed = {}".format(cs.wait_count, elapsed[2]))
        elapsed = cs.get_elapsed()
        if elapsed != None:
            print("    estimated elapsed time = {}".format(elapsed))

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
        for line in f:
            match = re.match(r'''^(PARSE|PARSING IN CURSOR|EXEC|FETCH|WAIT|CLOSE|BINDS) (#\d+)(:| )(.*)''', line)
            if match:
                #print(match.groups())
                if match.group(1) == 'PARSING IN CURSOR':
                    p = handle_parsing(match.group(2), match.group(4))
                if match.group(1) == 'PARSE':
                    last_parse = handle_parse(match.group(2), match.group(4))
                    cs = tracker.add_parse(match.group(2), last_parse)
                    if cs:
                        print_naughty_exec(cs)
                if match.group(1) == 'EXEC':
                    last_exec = handle_exec(match.group(2), match.group(4))
                    cs = tracker.add_exec(match.group(2), last_exec)
                    if cs:
                        print_naughty_exec(cs)
                if match.group(1) == 'FETCH':
                    # FIXME: fetches should be added to execs, not other way around
                    f = handle_fetch(match.group(2), match.group(4))
                    tracker.add_fetch(match.group(2), f)
                if match.group(1) == 'WAIT':
                    w = handle_wait(match.group(2), match.group(4))
                    tracker.add_wait(match.group(2), w)
                if match.group(1) == 'CLOSE':
                    c = handle_close(match.group(2), match.group(4))
                    cs = tracker.add_close(match.group(2), c)
                    if cs:
                        print_naughty_exec(cs)

                if match.group(1) == 'BINDS':
                    pass
tracker.flush()

#for c in cursors.keys():
#    print("cursor: {}, sql_id: {}".format(c, cursors[c]))
#for s in statements.values():
#    print(s)

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
            if args.db:
                database.add_rows(stat.sql_id, stat.exec_elapsed)
        with open("exec_hist_cpu_{}.out".format(stat.sql_id), 'wb') as f:
            stat.exec_hist_cpu.output_percentile_distribution(f, 1.0)
        with open("response_hist_{}.out".format(stat.sql_id), 'wb') as f:
            stat.resp_hist.output_percentile_distribution(f, 1.0)
        with open("response_hist_without_waits_{}.out".format(stat.sql_id), 'wb') as f:
            stat.resp_without_waits_hist.output_percentile_distribution(f, 1.0)

