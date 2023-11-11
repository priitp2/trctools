#!/usr/bin/env python3.11

import argparse
import sys
import datetime
import duckdb as d
from hdrh.histogram import HdrHistogram

__doc__ = """Some examples what can be done with Oracle SQL tracec using Duckdb and Parquet."""

def create_time_predicate(start, end):
    """ Turns datetime intervals into SQL predicates"""
    time_pred = ""
    if start:
        time_pred = time_pred + f"ts >= TIMESTAMP'{start}'"
    if end:
        time_pred = time_pred + f"{'and ' if start else ''} ts < TIMESTAMP'{end}'"

    return time_pred

class SummaryDuckdb:
    """ Initializes Duckdb with wiews and runs queries."""
    def __init__(self, dbdir):
        self.dbdir = dbdir
        d.sql(f"""create or replace view elapsed_time as
                select sql_id,
                    exec_id,
                    max(tim) - min(tim) as ela,
                    first(ts order by ts) ts
                from
                    read_parquet('{dbdir}')
                where
                    tim is not null
                group by sql_id, exec_id;
              """)
        d.sql(f"""create or replace view cursor_elapsed_time as
                select cursor_id,
                    exec_id,
                    max(tim) - min(tim) as ela,
                    first(ts order by ts) ts
                from
                    read_parquet('{dbdir}')
                where
                    tim is not null
                group by cursor_id, exec_id;
              """)

    def summary(self, start, end):
        pred = create_time_predicate(start, end)
        time_pred = f"{'WHERE ' if pred else ''} {pred}"

        res = d.sql(f"""
			SELECT
                            ela.sql_id,
                            dbc.sql_text,
                            ela.execs,
                            ela.total,
                            ela.median,
                            ela.p99,
                            ela.max
                        FROM
                            (
                                SELECT
                                    sql_id,
                                    COUNT(*)    execs,
                                    sum(ela)    total,
                                    MEDIAN(ela) median,
                                    PERCENTILE_DISC(0.99) WITHIN GROUP( ORDER BY ela) p99,
                                    max(ela)    max
                                FROM
                                    elapsed_time
                                {time_pred}
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

    def create_hdrh(self, sql_id, fname):
        res = d.sql(f"select ela from elapsed_time where sql_id = '{sql_id}'").fetchall()
        resp_hist = HdrHistogram(1, 1000000000, 1)
        for ela in res:
            resp_hist.record_value(ela[0])
        if not fname:
            fname = f"elapsed_{sql_id}.out"
        with open(fname, 'wb') as f:
            resp_hist.output_percentile_distribution(f, 1.0)
    def outliers(self, sql_id, thresold):
        res = d.sql(f"""select cursor_id "cursor",
                            exec_id,
                            ops,
                            elapsed_time ela,
                            rows_processed "rows",
                            event_name "event",
                            file_name,
                            line
                        from
                            read_parquet('{self.dbdir}')
                        where
                            sql_id = '{sql_id}'
                            and exec_id in (select exec_id from elapsed_time where sql_id = '{sql_id}' and ela > {thresold})
                            and ops not in ('BINDS')
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
                                ops,
                                sql_id
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
        if not fname:
            fname = 'wait_histogram'
        with open(fname, 'wb') as fdesc:
            resp_hist.output_percentile_distribution(fdesc, 1.0)

    def db(self):
        res = d.sql(f"""select count(*) "rows",
                            count(distinct file_name) files,
                            count(distinct sql_id) "sql_id's",
                            count(distinct cursor_id) cursors,
                            first(ts order by ts) "first timestamp",
                            last(ts order by ts) filter(ts is not null) "last timestamp"
                        from read_parquet('{self.dbdir}')
                    """)
        print(res)

        res = d.sql(f"""select file_name, count(*) "rows",
                            date_trunc('second', min(ts)) "first timestamp",
                            date_trunc('second', max(ts)) "last timestamp",
                            date_trunc('second', max(ts)) - date_trunc('second', min(ts)) "wallclock time in file",
                            cast(round((max(tim) - min(tim))/1000000) as integer) "elapsed(s)" 
                        from read_parquet('{self.dbdir}')
                        group by file_name order by count(*) desc
                    """)
        print(res)
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Generate summary from processed traces')
    subparsers = parser.add_subparsers(dest='action', title='Available subcommands', required=True)

    parser.add_argument('--dbdir', metavar='dbdir', type=str, required=True,
                                    help='Directory for Parquet files')

    summary_parser = subparsers.add_parser('summary', help='Generates summary of the executed SQL '
                        +'statements, execution counts, median and p99 execution times')
    summary_parser.add_argument('--start', metavar='start', type=datetime.datetime.fromisoformat, default=None,
                            help='Start timestamp in ISO 8601 format')
    summary_parser.add_argument('--end', metavar='end', type=datetime.datetime.fromisoformat, default=None,
                            help='End timestamp in ISO 8601 format')

    hist_parser = subparsers.add_parser('histogram', help='Generates response time histogram for the '
                                        +'sql_id or wait event')
    hist_parser.add_argument('--sql_id', type=str, dest='sql_id',
                     help="Comma separated list of sql_id's for which histogram, outliers or waits "
                         +"are produced")
    hist_parser.add_argument('--wait_name', dest='wait_name', type=str,
                                    help='Name of the wait event')
    hist_parser.add_argument('--output', dest='fname', type=str, help='Output filename')

    out_parser = subparsers.add_parser('outliers', help='Displays content of the trace files for the '
                         +'executions that took more than specified amount of time')
    out_parser.add_argument('--sql_id', type=str, dest='sql_id',
                     help="Comma separated list of sql_id's for which outliers are displayed")
    out_parser.add_argument('--thresold', type=str, dest='thresold',
                     help="Outlier thresold in microseconds")

    waits_parser = subparsers.add_parser('waits', help='Prints summary of the wait events for sql_id')
    waits_parser.add_argument('--sql_id', type=str, dest='sql_id',
                     help="Comma separated list of sql_id's for which waits are displayed")

    whist_parser = subparsers.add_parser('wait_histogram', help='Creates histogram of the elapsed '
                        +'time for a specific wait event')
    whist_parser.add_argument('--wait_name', dest='wait_name', type=str, help='Name of the wait event')
    whist_parser.add_argument('--output', dest='fname', type=str, help='Output filename')

    dbs_parser = subparsers.add_parser('db', help='Prints some statistics about the stuff in Parquet '
                        +'files and recorded trace files')

    args = parser.parse_args()

    s = SummaryDuckdb(args.dbdir + '/*')

    if args.action == 'summary':
        s.summary(args.start, args.end)
    if args.action == 'cursor-summary':
        s.cursor_summary()
    elif args.action == 'histogram':
        if args.sql_id:
            for sqlid in args.sql_id.split(','):
                s.create_hdrh(args.sql_id, args.fname)
        if args.wait_name:
            s.wait_histogram(args.wait_name, args.fname)
    elif args.action == 'outliers':
        if not args.sql_id:
            sys.exit('Error: sql_io is mandatory parameter')
        s.outliers(args.sql_id, args.thresold)
    elif args.action == 'waits':
        s.waits(args.sql_id)
    elif args.action == 'db':
        s.db()
