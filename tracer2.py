#!/usr/bin/env python3

import argparse
from hdrh.histogram import HdrHistogram
import re
import sys

max_elapsed = 500000
cursors = {}
statements = {}
latest_waits = []

def handle_parse(cursor, params):
    c = {}
    for item in params.split():
        key = item.split('=')
        if key[0] == 'len':
            c['statement_length'] = key[1]
        if key[0] == 'dep':
            c['rec_depth'] = key[1]
        if key[0] == 'uid':
            c['schema_uid'] = key[1]
        if key[0] == 'oct':
            c['command_type'] = key[1]
        if key[0] == 'lid':
            c['priv_user_id'] = key[1]
        if key[0] == 'tim':
            c['timestamp'] = key[1]
        if key[0] == 'hv':
            c['hash_id'] = key[1]
        if key[0] == 'ad':
            c['address'] = key[1]
        if key[0] == 'sqlid':
            c['sql_id'] = key[1]

    c['execs'] = 0
    c['fetches'] = 0
    c['exec_hist_elapsed'] = HdrHistogram(1, 1000000000, 0)
    c['exec_hist_cpu'] = HdrHistogram(1, 1000000000, 0)
    c['fetch_hist_elapsed'] = HdrHistogram(1, 1000000000, 0)
    c['fetch_hist_cpu'] = HdrHistogram(1, 1000000000, 0)

    if c['sql_id'] not in statements.keys():
        statements[c['sql_id']] = c

    # Not sure if (cursor, sql_id) is unique, just overwrite the mapping if they are not
    cursors[cursor] = c['sql_id']
#    print("handle_parse: cursor = {}, params = {}".format(cursor, params))

def handle_exec(cursor, params):
    sql_id = cursors[cursor]
    statement = statements[sql_id]
#    print("handle_exec0: cursor = {}, sql_id = {}".format(cursor, sql_id))
    elapsed = statement['exec_hist_elapsed']
    cpu = statement['exec_hist_cpu']
    for item in params.split(','):
        key = item.split('=')
        if key[0] == 'c':
            cpu.record_value(int(key[1]))
        if key[0] == 'e':
            elapsed.record_value(int(key[1]))
    statement['execs'] = statement['execs'] + 1
#    print(statement)
#    print("handle_exec1: cursor = {}, params = {}, sql_id = {}".format(cursor, params, cursors[cursor]))

def handle_fetch(cursor, params):
    statement = statements[cursors[cursor]]
    elapsed = statement['fetch_hist_elapsed']
    cpu = statement['fetch_hist_cpu']
    for item in params.split(','):
        key = item.split('=')
        if key[0] == 'c':
            c = int(key[1])
            cpu.record_value(c)
        if key[0] == 'e':
            e = int(key[1])
            elapsed.record_value(e)
    statement['fetches'] = statement['fetches'] + 1
    return (cursor, c, e)

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

parser = argparse.ArgumentParser(description='Do stuff with Oracle 19c trace files')
parser.add_argument('trace_files', metavar='files', type=str, nargs='+',
                            help='Trace files to process')
args = parser.parse_args()

for fname in args.trace_files:
    print("Processing {}".format(fname))
    with open(fname, 'r') as f:
        for line in f:
            match = re.match(r'''^(PARSING IN CURSOR|EXEC|FETCH|WAIT) (#\d+)(:| )(.*)''', line)
            if match:
                #print(match.groups())
                if match.group(1) == 'PARSING IN CURSOR':
                    handle_parse(match.group(2), match.group(4))
                    latest_waits = []
                if match.group(1) == 'EXEC':
                    handle_exec(match.group(2), match.group(4))
                    latest_waits = []
                if match.group(1) == 'FETCH':
                    f = handle_fetch(match.group(2), match.group(4))
                    if f[2] > max_elapsed:
                        print("sql_id: {}, cursor: {}, cpu: {}, elapsed: {}".format(cursors[f[0]], f[0], f[1], f[2]))
                        for w in latest_waits:
                            print("    name: {}, elapsed: {}, timestamp: {}".format(w['name'], w['elapsed'], w['timestamp']))
                    latest_waits = []
                if match.group(1) == 'WAIT':
                    handle_wait(match.group(2), match.group(4))

#for c in cursors.keys():
#    print("cursor: {}, sql_id: {}".format(c, cursors[c]))
#for s in statements.values():
#    print(s)
for c in statements.keys():
    cursor = statements[c]
    print('----------------------------------------')
    print("sql_id: {}, execs: {}, fetches: {}".format(cursor['sql_id'], cursor['execs'], cursor['fetches']))
    with open("exec_hist_elapsed_{}.out".format(cursor['sql_id']), 'wb') as f:
        cursor['exec_hist_elapsed'].output_percentile_distribution(f, 1.0)
    with open("exec_hist_cpu_{}.out".format(cursor['sql_id']), 'wb') as f:
        cursor['exec_hist_cpu'].output_percentile_distribution(f, 1.0)
    with open("fetch_hist_elapsed_{}.out".format(cursor['sql_id']), 'wb') as f:
        cursor['fetch_hist_elapsed'].output_percentile_distribution(f, 1.0)
    with open("fetch_hist_cpu_{}.out".format(cursor['sql_id']), 'wb') as f:
        cursor['fetch_hist_cpu'].output_percentile_distribution(f, 1.0)

