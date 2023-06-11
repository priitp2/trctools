import argparse
import duckdb as d
from hdrh.histogram import HdrHistogram

class SummaryDuckdb:
    def __init__(self, dbdir):
        self.dbdir = dbdir
        d.sql("""create or replace view elapsed_time as
                select sql_id,
                    exec_id,
                    last( ts order by ts) - first(ts order by ts) as ela
                from
                    read_parquet('{}')
                where
                    ts is not null
                group by sql_id, exec_id;
              """.format(dbdir))

    def summary(self):
        res = d.sql("""select sql_id,
                            count(*) execs,
                            median(ela) median,
                            percentile_disc(0.99) within group(order by ela) p99
                        from
                            elapsed_time
                        group by sql_id
                        order by execs, median
                    """)
        print(res)
        #for r in res:
        #    print("sq_id = {}, execs = {}, median = {}, 99th percentile = {}".format(r[0], r[1], r[2], r[3]))

    def create_hdrh(self, sql_id):
        r = d.sql("select ela from elapsed_time where sql_id = '{}'".format(sql_id)).fetchall()
        resp_hist = HdrHistogram(1, 1000000000, 1)
        for e in r:
            resp_hist.record_value(e[0])
        with open("elapsed_{}.out".format(sql_id), 'wb') as f:
            resp_hist.output_percentile_distribution(f, 1.0)
    def outliers(self, sql_id, thresold):
        res = d.sql("""select cursor_id "cursor",
                            exec_id,
                            ops,
                            elapsed_time,
                            ifnull(rows_processed, -1) "rows",
                            ts,
                            wait_name,
                            file_name,
                            line
                        from
                            read_parquet('{}')
                        where
                            sql_id = '{}'
                            and exec_id in (select exec_id from elapsed_time where sql_id = '{}' and ela > {})
                        order by exec_id, ts
                    """.format(self.dbdir, sql_id, sql_id, thresold))
        print(res)
    def waits(self, sql_id):
        res = d.sql("""select wait_name wait,
                            count(*) count,
                            sum(elapsed_time) sum,
                            median(elapsed_time) median,
                            percentile_disc(0.99) within group(order by elapsed_time) "99th percentile",
                            max(elapsed_time) max
                        from read_parquet('{}')
                        where sql_id = '{}'
                            and wait_name <> ''
                        group by wait_name order by count(*) desc
                    """.format(self.dbdir, sql_id))
        print(res)
#        for r in res:
#            print(r)
    def wait_histogram(self, wait_name, fname):
        res = d.sql("""select elapsed_time
                        from
                            read_parquet('{}')
                        where
                            ops = 'WAIT'
                            and wait_name = '{}'
                    """.format(self.dbdir, wait_name)).fetchall()
        resp_hist = HdrHistogram(1, 1000000000, 1)
        for w in res:
            resp_hist.record_value(w[0])
        with open(fname, 'wb') as f:
            resp_hist.output_percentile_distribution(f, 1.0)

parser = argparse.ArgumentParser(description='Generate summary from processed traces')
parser.add_argument('action', type=str, choices=['summary', 'histogram', 'outliers', 'waits', 'wait_histogram'],
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
args = parser.parse_args()

s = SummaryDuckdb(args.dbdir + '/trace/*')

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
