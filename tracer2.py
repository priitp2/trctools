#!/usr/bin/env python3

import argparse
from hdrh.histogram import HdrHistogram
import re
import sys
from scipy.stats import shapiro,kstest

max_fetch_elapsed = 500000
max_exec_elapsed = 50000
cursors = {}
statements = {}
latest_waits = []

class Statement:
    def __init__(self, cursor, params, norm):
        self.cursor = cursor

        for item in params.split():
            key = item.split('=')
            if key[0] == 'len':
                self.statement_length = key[1]
            if key[0] == 'dep':
                self.rec_depth = key[1]
            if key[0] == 'uid':
                self.schema_uid = key[1]
            if key[0] == 'oct':
                self.command_type = key[1]
            if key[0] == 'lid':
                self.priv_user_id = key[1]
            if key[0] == 'tim':
                self.timestamp = key[1]
            if key[0] == 'hv':
                self.hash_id = key[1]
            if key[0] == 'ad':
                self.address = key[1]
            if key[0] == 'sqlid':
                self.sql_id = key[1].replace("'", "")

        self.execs = 0
        self.fetches = 0
        self.norm = False

        self.exec_hist_elapsed = HdrHistogram(1, 1000000000, 1)
        self.exec_hist_cpu = HdrHistogram(1, 1000000000, 1)
        self.fetch_hist_elapsed = HdrHistogram(1, 1000000000, 1)
        self.fetch_hist_cpu = HdrHistogram(1, 1000000000, 1)

        if norm:
            self.exec_elapsed = []
            self.exec_cpu = []
            self.fetch_elapsed = []
            self.fetch_cpu = []
            self.norm = True

    def increase_exec_count(self):
        self.execs = self.execs + 1
    def increase_fetch_count(self):
        self.fetches = self.fetches + 1
    def record_exec_cpu(self, cpu):
        self.exec_hist_cpu.record_value(cpu)
        if self.norm:
            self.exec_cpu.append(cpu)
    def record_exec_elapsed(self, elapsed):
        self.exec_hist_elapsed.record_value(elapsed)
        if self.norm:
            self.exec_elapsed.append(elapsed)
    def record_fetch_cpu(self, cpu):
        self.fetch_hist_cpu.record_value(cpu)
        if self.norm:
            self.fetch_cpu.append(cpu)
    def record_fetch_elapsed(self, elapsed):
        self.fetch_hist_elapsed.record_value(elapsed)
        if self.norm:
            self.fetch_elapsed.append(elapsed)

def handle_parse(cursor, params):
    s = Statement(cursor, params, args.norm)

    if s.sql_id not in statements.keys():
        statements[s.sql_id] = s

    # Not sure if (cursor, sql_id) is unique, just overwrite the mapping if they are not
    cursors[cursor] = s.sql_id
#    print("handle_parse: cursor = {}, params = {}".format(cursor, params))

def get_ce(params):
    for item in params.split(','):
        key = item.split('=')
        if key[0] == 'c':
            cpu = int(key[1])
        if key[0] == 'e':
            elapsed = int(key[1])
    return (cpu, elapsed)

def handle_exec(cursor, params):
    sql_id = cursors[cursor]
    statement = statements[sql_id]
#    print("handle_exec0: cursor = {}, sql_id = {}".format(cursor, sql_id))
    ce = get_ce(params)
    statement.record_exec_cpu(ce[0])
    statement.record_exec_elapsed(ce[1])
    statement.increase_exec_count()
    return (cursor, ce[0], ce[1])
#    print(statement)
#    print("handle_exec1: cursor = {}, params = {}, sql_id = {}".format(cursor, params, cursors[cursor]))

def handle_fetch(cursor, params, last_exec):
    statement = statements[cursors[cursor]]
    if len(last_exec) > 0 and cursor != last_exec[0]:
        raise("handle_fetch: cursor mismatch: cursor = {}, cursor from last_exec = {}".format(cursor, last_exec[0]))
    ce = get_ce(params)

    if args.merge and len(last_exec) != 0:
        cpu = ce[0] + last_exec[1]
    else:
        cpu = ce[0]
    statement.record_fetch_cpu(cpu)

    if args.merge and len(last_exec) != 0:
        elapsed = ce[1] + last_exec[2]
    else:
        elapsed = ce[1]
    statement.record_fetch_elapsed(elapsed)
    statement.increase_fetch_count()
    return (cursor, cpu, elapsed)

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

parser = argparse.ArgumentParser(description='Do stuff with Oracle 19c trace files')
parser.add_argument('trace_files', metavar='files', type=str, nargs='+',
                            help='Trace files to process')
parser.add_argument('--merge', type=bool, default=False, dest='merge',
                            help='EXEC should be merged to the next FETCH.')
parser.add_argument('--sql_id', type=str, dest='sqlid',
                            help="Comma separated list of sql_id's for which histograms are produced")
parser.add_argument('--norm', type=bool, default = False, dest='norm',
                            help="Perform Shapiro-Wilk normality test on values")
args = parser.parse_args()

if args.merge:
    print('Merging EXEC and FETCH')

for fname in args.trace_files:
    print("Processing {}".format(fname))
    with open(fname, 'r') as f:
        last_exec = ()
        for line in f:
            match = re.match(r'''^(PARSING IN CURSOR|EXEC|FETCH|WAIT|CLOSE|BINDS) (#\d+)(:| )(.*)''', line)
            if match:
                #print(match.groups())
                if match.group(1) == 'PARSING IN CURSOR':
                    handle_parse(match.group(2), match.group(4))
                    latest_waits = []
                if match.group(1) == 'EXEC':
                    last_exec = handle_exec(match.group(2), match.group(4))
                    if last_exec[2] > max_exec_elapsed:
                        print("EXEC: sql_id: {}, cursor: {}, cpu: {}, elapsed: {}".format(cursors[last_exec[0]], last_exec[0], last_exec[1], last_exec[2]))
                        for w in latest_waits:
                            print("    name: {}, elapsed: {}, timestamp: {}".format(w['name'], w['elapsed'], w['timestamp']))
                    latest_waits = []
                if match.group(1) == 'FETCH':
                    # FIXME: fetches should be added to execs, not other way around
                    f = handle_fetch(match.group(2), match.group(4), last_exec)
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
                if match.group(1) == 'BINDS':
                    pass

#for c in cursors.keys():
#    print("cursor: {}, sql_id: {}".format(c, cursors[c]))
#for s in statements.values():
#    print(s)
ids = args.sqlid.split(',')
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
        with open("exec_hist_cpu_{}.out".format(stat.sql_id), 'wb') as f:
            stat.exec_hist_cpu.output_percentile_distribution(f, 1.0)
        with open("fetch_hist_elapsed_{}.out".format(stat.sql_id), 'wb') as f:
            stat.fetch_hist_elapsed.output_percentile_distribution(f, 1.0)
        with open("fetch_hist_cpu_{}.out".format(stat.sql_id), 'wb') as f:
            stat.fetch_hist_cpu.output_percentile_distribution(f, 1.0)

