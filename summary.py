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
                    last( tim order by tim) - first(tim order by tim) as ela
                from
                    read_parquet('{dbdir}')
                where
                    tim is not null
                group by sql_id, exec_id;
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
                            ifnull(rows_processed, -1) "rows",
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
        res = d.sql(f"""select event_name wait,
                            count(*) count,
                            sum(elapsed_time) sum,
                            median(elapsed_time) median,
                            percentile_disc(0.99) within group(order by elapsed_time) "99th percentile",
                            max(elapsed_time) max
                        from read_parquet('{self.dbdir}')
                        where {pred}
                             ops = 'WAIT'
                        group by event_name order by count(*) desc
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
    def norm(self, test, dist, sql_id, wait_name):
        """Produces list of query response times or wait event elapsed times for goodness-of-fit
            test."""
        if sql_id:
            res = d.sql(f"select ela from elapsed_time where sql_id = '{sql_id}'").fetchall()
        elif wait_name:
            res = d.sql(f"select elapsed_time from read_parquet('{self.dbdir}') where event_name " \
                        +f"= '{wait_name}' and ops = 'WAIT'").fetchall()

        ret = self._stat_test(test, dist, [r[0] for r in res])
        print(ret)

    def db(self):
        res = d.sql(f"""select count(*) "rows",
                            count(distinct file_name) files,
                            count(distinct sql_id) "sql_id's",
                            count(distinct cursor_id) cursors
                        from read_parquet('{self.dbdir}')
                    """)
        print(res)

        res = d.sql(f"""select file_name, count(*) "rows", min(ts2) "first timestamp"
                        from read_parquet('{self.dbdir}')
                        group by file_name order by count(*)
                    """)
        print(res)
    def _stat_test(self, test, dist, lst):
        """Does goodness-of-fit test on list of numbers. Defaults to normal distribution."""
        if test == 'shapiro' and len(lst) > 5000:
            print('More than 5k items, switching from Shapiro to Anderson')
            test = 'anderson'
        if test == 'shapiro':
            return stats.shapiro(lst)
        if test == 'anderson':
            return stats.anderson(lst, dist)
        if test == 'kstest':
            return stats.kstest(lst, dist)
        print(f"stat_test: unknown test: {test}")
        return None

parser = argparse.ArgumentParser(description='Generate summary from processed traces')
parser.add_argument('action', type=str, choices=['summary', 'histogram', 'outliers', 'waits',
                                                    'wait_histogram', 'db', 'norm'],
                     help='Directory for Parquet files')
parser.add_argument('--sql_id', type=str, dest='sql_id',
                     help="Comma separated list of sql_id's for which summary is produced")
parser.add_argument('--thresold', type=str, dest='thresold',
                     help="Thresold in microsecond for which the outliers are displayed")
parser.add_argument('--dbdir', metavar='dbdir', type=str,
                                    help='Directory for Parquet files')
parser.add_argument('--wait_name', dest='wait_name', type=str,
                                    help='Name for the wait_histogram command')
parser.add_argument('--output', dest='fname', type=str,
                                    help='Output for the wait_histogram command')
parser.add_argument('--test', dest='test_type', type=str, default='shapiro',
                    help="For the normality test, type of the test performed. Accepted values: " \
                        +"shapiro, anderson (Anderson-Darling), kstest (Kolmogorov-Smirnov). See " \
                        +"scipy.statistics documentation for the explanation")
parser.add_argument('--dist', dest='dist', type=str, default='norm', help="For normality test," \
                    + " specifies cdf for Kolmogorov-Smirnov, or distribution for Anderson-Darling."
                    )
parser.add_argument('--dist-args', dest='dist-args', type=str, default=None,
                    help="Arguments for the CDF in normality/goodness-of-fit test")
args = parser.parse_args()

s = SummaryDuckdb(args.dbdir + '/*')

if args.action == 'summary':
    s.summary()
elif args.action == 'histogram':
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
elif args.action == 'norm':
    s.norm(args.test_type, args.dist, args.sql_id, args.wait_name)
