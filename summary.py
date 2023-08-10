#!/usr/bin/env python3.11

import argparse
import duckdb as d
from hdrh.histogram import HdrHistogram
from scipy import stats

class SummaryDuckdb:
    def __init__(self, dbdir):
        self.dbdir = dbdir
        d.sql(f"""create or replace view elapsed_time as
                select sql_id,
                    exec_id,
                    max(tim) - min(tim) as ela
                from
                    read_parquet('{dbdir}')
                where
                    tim is not null
                group by sql_id, exec_id;
              """)
        d.sql(f"""create or replace view cursor_elapsed_time as
                select cursor_id,
                    exec_id,
                    max(tim) - min(tim) as ela
                from
                    read_parquet('{dbdir}')
                where
                    tim is not null
                group by cursor_id, exec_id;
              """)

    def summary(self):
        res = d.sql(f"""
			SELECT
                            ela.sql_id,
                            dbc.sql_text,
                            ela.execs,
                            ela.median,
                            ela.p99
                        FROM
                            (
                                SELECT
                                    sql_id,
                                    COUNT(*)    execs,
                                    MEDIAN(ela) median,
                                    PERCENTILE_DISC(0.99) WITHIN GROUP( ORDER BY ela) p99
                                FROM
                                    elapsed_time
                                GROUP BY
                                    sql_id
                                ORDER BY
                                    execs, median
                            ) ela
                        LEFT JOIN (
                                SELECT
                                    sql_id,
                                    any_value(event_raw) "sql_text"
                                FROM
                                    read_parquet ( '{self.dbdir}' )
                                WHERE
                                    ops = 'PIC'
                                GROUP BY
                                    sql_id
                        ) dbc ON ( ela.sql_id = dbc.sql_id )
                        ORDER BY
                        ela.execs;
                    """)
        print(res)
    def cursor_summary(self):
        res = d.sql(f"""
			SELECT
                            ela.cursor_id,
                            dbc.sql_text,
                            ela.execs,
                            ela.median,
                            ela.p99
                        FROM
                            (
                                SELECT
                                    cursor_id,
                                    COUNT(*)    execs,
                                    MEDIAN(ela) median,
                                    PERCENTILE_DISC(0.99) WITHIN GROUP( ORDER BY ela) p99
                                FROM
                                    cursor_elapsed_time
                                GROUP BY
                                    cursor_id
                                ORDER BY
                                    execs, median
                            ) ela
                        LEFT JOIN (
                                SELECT
                                    cursor_id,
                                    any_value(event_raw) "sql_text"
                                FROM
                                    read_parquet ( '{self.dbdir}' )
                                WHERE
                                    ops = 'PIC'
                                GROUP BY
                                    cursor_id
                        ) dbc ON ( ela.cursor_id = dbc.cursor_id )
                        ORDER BY
                        ela.execs;
                    """)
        print(res)

    def create_hdrh(self, sql_id):
        r = d.sql(f"select ela from elapsed_time where sql_id = '{sql_id}'").fetchall()
        resp_hist = HdrHistogram(1, 1000000000, 1)
        for ela in r:
            resp_hist.record_value(ela[0])
        with open(f"elapsed_{sql_id}.out", 'wb') as f:
            resp_hist.output_percentile_distribution(f, 1.0)
    def outliers(self, sql_id, thresold):
        res = d.sql(f"""select cursor_id "cursor",
                            exec_id,
                            ops,
                            elapsed_time,
                            rows_processed "rows",
                            tim,
                            event_name,
                            file_name,
                            line
                        from
                            read_parquet('{self.dbdir}')
                        where
                            sql_id = '{sql_id}'
                            and exec_id in (select exec_id from elapsed_time where sql_id = '{sql_id}' and ela > {thresold})
                        order by exec_id, ts
                    """)
        print(res)
    def waits(self, sql_id):
        pred = ''
        if sql_id:
            pred = f"sql_id = '{sql_id}' and"
        res = d.sql(f"""
			SELECT
                            event_name           wait,
                            COUNT(*)             count,
                            SUM(elapsed_time)    sum,
                            MEDIAN(elapsed_time) median,
                            PERCENTILE_DISC(0.99) WITHIN GROUP( ORDER BY elapsed_time) "99th percentile",
                            MAX(elapsed_time)    max
                        FROM (
                            SELECT
                                CASE
                                    WHEN event_name LIKE 'SQL*Net message%' AND cursor_id = '#0' THEN
                                        event_name || '/idle'
                                    ELSE event_name
                                END "event_name",
                                elapsed_time,
                                ops
                            FROM
                                read_parquet ( '{self.dbdir}' )
                        )
                        WHERE {pred}
                            ops = 'WAIT'
                        GROUP BY
                            event_name
                        ORDER BY
                            COUNT(*) DESC
                    """)
        print(res)
    def wait_histogram(self, wait_name, fname):
        res = d.sql(f"""select elapsed_time
                        from
                            read_parquet('{self.dbdir}')
                        where
                            ops = 'WAIT'
                            and wait_name = '{wait_name}'
                    """).fetchall()
        resp_hist = HdrHistogram(1, 1000000000, 1)
        for ela in res:
            resp_hist.record_value(ela[0])
        with open(fname, 'wb') as f:
            resp_hist.output_percentile_distribution(f, 1.0)

    def db(self):
        res = d.sql(f"""select count(*) "rows",
                            count(distinct file_name) files,
                            count(distinct sql_id) "sql_id's",
                            count(distinct cursor_id) cursors
                        from read_parquet('{self.dbdir}')
                    """)
        print(res)

        res = d.sql(f"""select file_name, count(*) "rows", min(ts) "first timestamp"
                        from read_parquet('{self.dbdir}')
                        group by file_name order by count(*)
                    """)
        print(res)

parser = argparse.ArgumentParser(description='Generate summary from processed traces')
subparsers = parser.add_subparsers(dest='action', title='Available subcommands')

parser.add_argument('--dbdir', metavar='dbdir', type=str,
                                    help='Directory for Parquet files')

summary_parser = subparsers.add_parser('summary', help='''Generates summary of the executed sql_id's''')

hist_parser = subparsers.add_parser('histogram', help='Generates histogram for the specified '
                                        +'sql_id or wait event name.')
hist_parser.add_argument('--sql_id', type=str, dest='sql_id',
                     help="Comma separated list of sql_id's for which histogram, outliers or waits "
                         +"are produced")
hist_parser.add_argument('--wait_name', dest='wait_name', type=str,
                                    help='Name of the wait event')
hist_parser.add_argument('--output', dest='fname', type=str, help='Output filename')

out_parser = subparsers.add_parser('outliers', help='''Prints out executions that took longer than --thresold microseconds''')
out_parser.add_argument('--sql_id', type=str, dest='sql_id',
                     help="Comma separated list of sql_id's for which outliers are displayed")
out_parser.add_argument('--thresold', type=str, dest='thresold',
                     help="Thresold in microseconds")

waits_parser = subparsers.add_parser('waits', help='Shows wait events for the sql_id')
waits_parser.add_argument('--sql_id', type=str, dest='sql_id',
                     help="Comma separated list of sql_id's for which waits are displayed")

whist_parser = subparsers.add_parser('wait_histogram', help='''Generates histogram for the wait event''')
whist_parser.add_argument('--wait_name', dest='wait_name', type=str, help='Name of the wait event')
whist_parser.add_argument('--output', dest='fname', type=str, help='Output filename')

dbs_parser = subparsers.add_parser('db', help='''Shows summary information about processed trace files''')

args = parser.parse_args()

s = SummaryDuckdb(args.dbdir + '/*')

if args.action == 'summary':
    s.summary()
if args.action == 'cursor-summary':
    s.cursor_summary()
elif args.action == 'histogram':
    if args.sql_id:
        for sqlid in args.sql_id.split(','):
            s.create_hdrh(args.sql_id)
elif args.action == 'outliers':
    s.outliers(args.sql_id, args.thresold)
elif args.action == 'waits':
    s.waits(args.sql_id)
elif args.action == 'wait_histogram':
    s.wait_histogram(args.wait_name, args.fname)
elif args.action == 'db':
    s.db()
