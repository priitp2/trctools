#!/usr/bin/env python3.11

import argparse
import sys
import duckdb as d
from hdrh.histogram import HdrHistogram

__doc__ = """Some examples what can be done with Oracle SQL tracec using Duckdb and Parquet."""

def sqlid2pred(sql_id):
    """Turns a list of sql_id's into IN expression"""
    ids = [f"'{s}'" for s in sql_id.split(',')]
    return f"sql_id in ({','.join(ids)})"

def create_preds(fis):
    """Turns parameters into SQL expressions."""
    preds = set()
    if 'start' in fis:
        preds.add(f"ts >= TIMESTAMP '{fis['start']}'")
    if 'end' in fis:
        preds.add(f"ts < TIMESTAMP '{fis['end']}'")
    if 'client_id' in fis:
        preds.add(f"client_id = '{fis['client_id']}'")
    if 'sql_id' in fis:
        preds.add(sqlid2pred(fis['sql_id']))

    return ' and '.join(preds)

class SummaryDuckdb:
    """ Initializes Duckdb with wiews and runs queries."""
    def __init__(self, dbdir):
        self.dbdir = dbdir
        d.sql(f"""create or replace view v_elapsed_time as
                select sql_id,
                    exec_id,
                    max(tim) - min(tim) as ela,
                    first(ts order by ts) ts,
                    first(client_id) client_id
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

    def summary(self, fis):
        """Prints out list of SQL queries executed and some summary statistics."""
        preds = create_preds(fis)
        filter_pred = f"""{'WHERE ' if preds else ''} {preds}"""

        query = f"""
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
                                    v_elapsed_time
                                {filter_pred}
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
                    """
        res = d.sql(query)
        print(res)
    def cursor_summary(self):
        """Prints out list of cursors queries executed and some summary statistics. Difference
            with method summary() is that not all of the PARSING IN CURSOR events might be in the
            trace files, in that case we will miss the sql_id and cursor is all we have."""
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
        res = d.sql(f"select ela from v_elapsed_time where sql_id = '{sql_id}'").fetchall()
        resp_hist = HdrHistogram(1, 1000000000, 1)
        for ela in res:
            resp_hist.record_value(ela[0])
        if not fname:
            fname = f"elapsed_{sql_id}.out"
        with open(fname, 'wb') as f:
            resp_hist.output_percentile_distribution(f, 1.0)
    def outliers(self, sql_id, thresold, statistic='elapsed_time'):
        res = d.sql(f"""select cursor_id "cursor",
                            sum({statistic}) "{statistic}",
                            first(ts),
                            first(file_name) file_name,
                            first(line) "first line"
                        from
                            read_parquet('{self.dbdir}')
                        where
                            sql_id = '{sql_id}'
                        group by
                            cursor_id, exec_id
                        having sum({statistic}) > {thresold}
                        order by sum({statistic}) desc
                    """)
        print(res)
    def waits(self, sql_id, thresold):
        pred = ''
        inner_where = ''
        if sql_id:
            pred = f"{sqlid2pred(sql_id)} and"
        if thresold:
            inner_where = f" WHERE exec_id in (select exec_id from v_elapsed_time where ela > {thresold})"
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
                            WHERE
                            {inner_where}
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
                            and event_name = '{wait_name}'
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
    def sql(self, sql_id):
        res = d.sql(f"""create or replace view ops_stats as
                            select sum(cpu_time) cpu,
                                sum(elapsed_time) elapsed,
                                sum(ph_reads) ph_reads,
                                sum(cr_reads) cr_reads,
                                sum(current_reads) current_reads,
                                sum(rows_processed) rows_processed,
                            from read_parquet('{self.dbdir}')
                            where
                                sql_id = '{sql_id}'
                            group by exec_id
                    """)
        res = d.sql("""select 'cpu' "stats", sum(cpu) "sum", median(cpu) "median",
                            PERCENTILE_DISC(0.99) WITHIN GROUP( ORDER BY cpu) "99th percentile",
                            max(cpu) "max"
                        from ops_stats
                        union all
                        select 'elapsed', sum(elapsed) "sum", median(elapsed) "median",
                            PERCENTILE_DISC(0.99) WITHIN GROUP( ORDER BY elapsed) "99th percentile",
                            max(elapsed) "max"
                        from ops_stats
                        union all
                        select 'physical_reads', sum(ph_reads) "sum", median(ph_reads) "median",
                            PERCENTILE_DISC(0.99) WITHIN GROUP( ORDER BY ph_reads) "99th percentile",
                            max(ph_reads) "max"
                        from ops_stats
                        union all
                        select 'consistent_reads', sum(cr_reads) "sum", median(cr_reads) "median",
                            PERCENTILE_DISC(0.99) WITHIN GROUP( ORDER BY cr_reads) "99th percentile",
                            max(cr_reads) "max"
                        from ops_stats
                        union all
                        select 'current_reads', sum(current_reads) "sum", median(current_reads) "median",
                            PERCENTILE_DISC(0.99) WITHIN GROUP( ORDER BY current_reads) "99th percentile",
                            max(current_reads) "max"
                        from ops_stats
                        union all
                        select 'rows_processed', sum(rows_processed) "sum", median(rows_processed) "median",
                            PERCENTILE_DISC(0.99) WITHIN GROUP( ORDER BY rows_processed) "99th percentile",
                            max(rows_processed) "max"
                        from ops_stats
                    """)
        print(res)
        res = d.sql(f"""select ops, cnt "count", sum_cpu "sum(cpu)", median_cpu "median cpu",
                            p99_cpu "p99 cpu", max_cpu "max cpu",
                            sum_ela "sum(elapsed)", median_ela "median elapsed",
                            p99_ela "p99 elapsed", max_ela "max elapsed"
                        from (
                            select case when ops = 'PIC' then 'PARSING IN CURSOR' else ops end "ops",
                                count(*) cnt, sum(cpu_time) sum_cpu, median(cpu_time) median_cpu, 
                                PERCENTILE_DISC(0.99) WITHIN GROUP( ORDER BY cpu_time) p99_cpu,
                                max(cpu_time) max_cpu,
                                sum(elapsed_time) sum_ela, median(elapsed_time) median_ela,
                                PERCENTILE_DISC(0.99) WITHIN GROUP( ORDER BY elapsed_time) p99_ela,
                                max(elapsed_time) max_ela,
                                case when ops = 'PIC' then 1 when ops = 'PARSE' then 2 when ops = 'EXEC' then 3
                                when ops = 'WAIT' then 4 when ops = 'FETCH' then 5 else 6 end dummy
                            from read_parquet('{self.dbdir}')
                            where
                                sql_id = '{sql_id}'
                                and ops in ('PIC', 'PARSE', 'EXEC', 'CLOSE', 'WAIT', 'FETCH', 'ERROR')
                            group by ops
                            order by dummy)
        """)
        print(res)

if __name__ == '__main__':

    filters = {}

    parser = argparse.ArgumentParser(description='Generate summary from processed traces')
    subparsers = parser.add_subparsers(dest='action', title='Available subcommands', required=True)

    parser.add_argument('--dbdir', metavar='dbdir', type=str, required=True,
                                    help='Directory for Parquet files')

    summary_parser = subparsers.add_parser('summary', help='Generates summary of the executed SQL '
                        +'statements, execution counts, median and p99 execution times')
    summary_parser.add_argument('--start', metavar='start',
                            type=lambda x: filters.__setitem__('start', x),
                            help='Start timestamp in ISO 8601 format')
    summary_parser.add_argument('--end', metavar='end',
                            type=lambda x: filters.__setitem__('end', x),
                            help='End timestamp in ISO 8601 format')
    summary_parser.add_argument('--client_id', type= lambda x: filters.__setitem__('client_id', x),
                            help='Filter by CLIENT ID')
    summary_parser.add_argument('--sql_id', type= lambda x: filters.__setitem__('sql_id', x),
                            help="Filter by comma separated list of sql_id's")

    hist_parser = subparsers.add_parser('histogram', help='Generates response time histogram for '
                                        +'the sql_id or wait event')
    hist_parser.add_argument('--sql_id', type=str, dest='sql_id',
                     help="Comma separated list of sql_id's for which histogram, outliers or waits "
                         +"are produced")
    hist_parser.add_argument('--wait_name', dest='wait_name', type=str,
                                    help='Name of the wait event')
    hist_parser.add_argument('--output', dest='fname', type=str, help='Output filename')

    out_parser = subparsers.add_parser('outliers', help='Displays content of the trace files for '
                         +'the executions that took more than specified amount of time')
    out_parser.add_argument('--sql_id', type=str, dest='sql_id',
                     help="Comma separated list of sql_id's for which outliers are displayed")
    out_parser.add_argument('--thresold', type=str, dest='thresold',
                     help="Outlier thresold in microseconds")
    out_parser.add_argument('--statistic', type=str, dest='stat', default='elapsed_time',
                     help="Parameter for which thresold is applied. More useful parameters are"
                        +" elapsed_time, cpu, rows_processed, ph_reads, cr_reads, current_reads."
                        +" For the full list see the schema definition in db/arrow.py")

    waits_parser = subparsers.add_parser('waits', help='Prints summary of the wait events for '
                    +'sql_id')
    waits_parser.add_argument('--sql_id', type=str, dest='sql_id',
                     help="Comma separated list of sql_id's for which waits are displayed")
    waits_parser.add_argument('--thresold', type=str, dest='thresold',
                     help="Outlier thresold in microseconds. Minimum query response time from "
                        +"which the wait events are included")

    dbs_parser = subparsers.add_parser('db', help='Prints some statistics about the stuff in'
                        +'Parquet files and recorded trace files')

    sql_parser = subparsers.add_parser('sql', help='Summary for specific sql_id ')
    sql_parser.add_argument('--sql_id', type=str, dest='sql_id')


    args = parser.parse_args()

    s = SummaryDuckdb(args.dbdir + '/*')

    if args.action == 'summary':
        s.summary(filters)
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
        s.outliers(args.sql_id, args.thresold, args.stat)
    elif args.action == 'waits':
        s.waits(args.sql_id, args.thresold)
    elif args.action == 'db':
        s.db()
    elif args.action == 'sql':
        s.sql(args.sql_id)
