#!/usr/bin/env python3

import argparse
from hdrh.histogram import HdrHistogram
import re
import sys

cursors = {}

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

    c['exec_hist_elapsed'] = HdrHistogram(1, 1000000000, 4)
    c['exec_hist_cpu'] = HdrHistogram(1, 1000000000, 4)
    c['fetch_hist_elapsed'] = HdrHistogram(1, 1000000000, 4)
    c['fetch_hist_cpu'] = HdrHistogram(1, 1000000000, 4)
    # FIXME: what if cursors are not unique in different instances?
    cursors[cursor] = c

def handle_exec(cursor, params):
    elapsed = cursors[cursor]['exec_hist_elapsed']
    cpu = cursors[cursor]['exec_hist_cpu']
    for item in params.split(','):
        key = item.split('=')
        if key[0] == 'c':
            cpu.record_value(int(key[1]))
        if key[0] == 'e':
            elapsed.record_value(int(key[1]))

def handle_fetch(cursor, params):
    elapsed = cursors[cursor]['fetch_hist_elapsed']
    cpu = cursors[cursor]['fetch_hist_cpu']
    for item in params.split(','):
        key = item.split('=')
        if key[0] == 'c':
            cpu.record_value(int(key[1]))
        if key[0] == 'e':
            elapsed.record_value(int(key[1]))

parser = argparse.ArgumentParser(description='Do stuff with Oracle 19c trace files')
parser.add_argument('trace_files', metavar='files', type=str, nargs='+',
                            help='Trace files to process')
args = parser.parse_args()

for fname in args.trace_files:
    print("Processing {}".format(fname))
    with open(fname, 'r') as f:
        for line in f:
            match = re.match(r'''^(PARSING IN CURSOR|EXEC|FETCH) (#\d+)(:| )(.*)''', line)
            if match:
                #print(match.groups())
                if match.group(1) == 'PARSING IN CURSOR':
                    handle_parse(match.group(2), match.group(4))
                if match.group(1) == 'EXEC':
                    handle_exec(match.group(2), match.group(4))
                if match.group(1) == 'FETCH':
                    handle_fetch(match.group(2), match.group(4))
#            else:
#                print(line)
#            if re.match(r'^WAIT*', line):
#                print(line)

for c in cursors.keys():
    cursor = cursors[c]
    print('----------------------------------------')
    print("cursor: {}, sql_id: {}".format(c, cursor['sql_id']))
    with open("exec_hist_elapsed_{}.out".format(cursor['sql_id']), 'wb') as f:
        cursor['exec_hist_elapsed'].output_percentile_distribution(f, 1.0)
    with open("exec_hist_cpu_{}.out".format(cursor['sql_id']), 'wb') as f:
        cursor['exec_hist_cpu'].output_percentile_distribution(f, 1.0)
    with open("fetch_hist_elapsed_{}.out".format(cursor['sql_id']), 'wb') as f:
        cursor['fetch_hist_elapsed'].output_percentile_distribution(f, 1.0)
    with open("fetch_hist_cpu_{}.out".format(cursor['sql_id']), 'wb') as f:
        cursor['fetch_hist_cpu'].output_percentile_distribution(f, 1.0)

