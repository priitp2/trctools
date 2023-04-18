#!/usr/bin/env python3

import argparse
from hdrh.histogram import HdrHistogram
import re
import sys
from scipy.stats import shapiro,kstest
from statement import Statement
from oracle import DB

max_fetch_elapsed = 700000
max_exec_elapsed = 50000
cursors = {}
statements = {}
latest_waits = []

def handle_parsing(cursor, params):
    if args.norm or args.db:
        record_data = True
    else:
        record_data = False
    s = Statement(cursor, params, record_data)

    if s.sql_id not in statements.keys():
        statements[s.sql_id] = s

    # Not sure if (cursor, sql_id) is unique, just overwrite the mapping if they are not
    cursors[cursor] = s.sql_id
#    print("handle_parse: cursor = {}, params = {}".format(cursor, params))

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
    sql_id = cursors[cursor]
    statement = statements[sql_id]
#    print("handle_exec0: cursor = {}, sql_id = {}".format(cursor, sql_id))
    ce = get_ce(params)
    statement.record_exec_cpu(ce[0])
    statement.record_exec_elapsed(ce[1])
    statement.increase_exec_count()
    id = []
    id.append(-1)
    if args.db:
        ev = split_event(params)
        ev['parent_id'] = 0
        ev['cursor'] = cursor
        ev['event'] = 'EXEC'
        ev['type'] = 0
        id = database.add_event(ev)
    return (cursor, ce[0], ce[1], id[0])
#    print(statement)
#    print("handle_exec1: cursor = {}, params = {}, sql_id = {}".format(cursor, params, cursors[cursor]))

def handle_fetch(cursor, params, last_exec, last_exec_id, last_parse):
    statement = statements[cursors[cursor]]
    if len(last_exec) > 0 and cursor != last_exec[0]:
        raise("handle_fetch: cursor mismatch: cursor = {}, cursor from last_exec = {}".format(cursor, last_exec[0]))
    ce = get_ce(params)

    lat = (cursor, ce[0], ce[1])
    if args.merge and len(last_exec) != 0:
        merge_lat_objects(lat, last_exec)
        merge_lat_objects(lat, last_parse)

    statement.record_fetch_cpu(lat[1])
    statement.record_fetch_elapsed(lat[2])
    statement.increase_fetch_count()

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
    else:
        print("handle_wait: no match: cursor={}, params = ->{}<-".format(cursor, params))

def handle_close(cursor, params):
    ce = get_ce(params)
    return (cursor, ce[0], ce[1])

def merge_lat_objects(dest, source):
    if source != list():
        source = [source]
    if len(source) < 3:
        return dest
    for s in source:
        if dest[0] != s[0]:
            raise(BaseException("merge_lat_objects: cursor mismatch: dest cursor = {}, source cursor = {}".format(dest[0], s[0])))
        cpu = dest[1] + s[1]
        elapsed = dest[2] + s[2]
    return (dest[0], cpu, elapsed)

parser = argparse.ArgumentParser(description='Do stuff with Oracle 19c trace files')
parser.add_argument('trace_files', metavar='files', type=str, nargs='+',
                            help='Trace files to process')
parser.add_argument('--merge', type=bool, default=False, dest='merge',
                            help='EXEC should be merged to the next FETCH.')
parser.add_argument('--sql_id', type=str, dest='sqlid',
                            help="Comma separated list of sql_id's for which histograms are produced")
parser.add_argument('--norm', type=bool, default = False, dest='norm',
                            help="Perform Shapiro-Wilk normality test on values")
parser.add_argument('--db', type=bool, default = False, dest='db',
                            help="persists raw data in the db")
parser.add_argument('--merge_all', type=bool, default = False, dest='merge_all',
                            help="Merges all sql statements into one histogram. Helpful without the bind variables")
args = parser.parse_args()

if args.merge:
    print('Merging EXEC and FETCH')

if args.db:
    database = DB()

for fname in args.trace_files:
    print("Processing {}".format(fname))
    with open(fname, 'r') as f:
        last_exec = ()
        last_parse = ()
        last_parse_id = -1
        last_exec_id = -1
        for line in f:
            match = re.match(r'''^(PARSE|PARSING IN CURSOR|EXEC|FETCH|WAIT|CLOSE|BINDS) (#\d+)(:| )(.*)''', line)
            if match:
                #print(match.groups())
                if match.group(1) == 'PARSING IN CURSOR':
                    handle_parsing(match.group(2), match.group(4))
                    latest_waits = []
                if match.group(1) == 'PARSE':
                    last_parse = handle_parse(match.group(2), match.group(4))
                    last_parse_id = last_parse[3]
                if match.group(1) == 'EXEC':
                    last_exec = handle_exec(match.group(2), match.group(4))
                    last_exec_id = last_exec[3]
                    if last_exec[2] > max_exec_elapsed:
                        print("EXEC: sql_id: {}, cursor: {}, cpu: {}, elapsed: {}".format(cursors[last_exec[0]], last_exec[0], last_exec[1], last_exec[2]))
                        for w in latest_waits:
                            print("    name: {}, elapsed: {}, timestamp: {}".format(w['name'], w['elapsed'], w['timestamp']))
                    latest_waits = []
                if match.group(1) == 'FETCH':
                    # FIXME: fetches should be added to execs, not other way around
                    print(last_parse)
                    f = handle_fetch(match.group(2), match.group(4), last_exec, last_exec_id, last_parse)
                    if f[2] > max_fetch_elapsed:
                        print("FETCH: sql_id: {}, cursor: {}, cpu: {}, elapsed: {}".format(cursors[f[0]], f[0], f[1], f[2]))
                        for w in latest_waits:
                            print("    name: {}, elapsed: {}, timestamp: {}".format(w['name'], w['elapsed'], w['timestamp']))
                    latest_waits = []
                    last_exec = ()
                if match.group(1) == 'WAIT':
                    handle_wait(match.group(2), match.group(4))
                if match.group(1) == 'CLOSE':
                    c = handle_close(match.group(2), match.group(4))
                    last_parse = ()
                    last_parse_id = -1
                    last_exec_id = -1
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

