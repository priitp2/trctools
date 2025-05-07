#!/usr/bin/env python3.11

import argparse
import sys
import duckdb as d
from hdrh.histogram import HdrHistogram
import tabulate

__doc__ = """Some examples what can be done with Oracle SQL tracec using Duckdb and Parquet."""

def sqlid2pred(sql_id):
    """Turns a list of sql_id's into IN expression"""
    ids = [f"'{s}'" for s in sql_id.split(',')]
    return f"sql_id in ({','.join(ids)})"

def thresold2pred(thresold):
    """Turns (a list of) thresolds to predicates"""
    if not thresold:
        return ''
    th = thresold.split(',')
    preds = ''
    if len(th) == 2:
        preds = (f"between {th[0]} and {th[1]}")
    if len(th) == 1:
        preds = (f"> {th[0]}")
    return preds

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
    def __init__(self, dbdir, tabtype, tabsize, remove_idle):
        self.dbdir = dbdir
        self.tabtype = tabtype
        self.tabsize = tabsize
        d.sql(f"""create or replace view v_elapsed_time as
                select sql_id,
                    span_id,
                    max(tim) - min(tim) as ela,
                    first(ts order by ts) ts,
                    first(client_id) client_id
                from
                    read_parquet('{dbdir}')
                where
                    tim is not null
                    {"and cursor_id <> '#0'" if remove_idle else ""}
                group by sql_id, span_id;
              """)
        d.sql(f"""create or replace view cursor_elapsed_time as
                select cursor_id,
                    span_id,
                    max(tim) - min(tim) as ela,
                    first(ts order by ts) ts
                from
                    read_parquet('{dbdir}')
                where
                    tim is not null
                group by cursor_id, span_id;
              """)

    def summary(self, fis):
        """Prints out list of SQL queries executed and some summary statistics."""
        preds = create_preds(fis)
        filter_pred = f"""{'WHERE ' if preds else ''} {preds}"""

        query = f"""
			SELECT
                            row_number() over(order by ela.execs asc),
                            coalesce(ela.sql_id, 'NULL'),
                            coalesce(regexp_replace(substring(dbc.sql_text, 0, 30), '[\r\n]', '', 'g'), 'NULL'),
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
                        ela.execs desc
                        LIMIT {self.tabsize};
                    """
        res = d.sql(query)
        table = tabulate.tabulate(res.fetchall(), tablefmt=self.tabtype,
                headers=['#', 'sql_id', 'sql_text', 'executions', 'total(us)', 'median(us)', 'p99(us)', 'max(us)'])
        print(table)
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

    def create_hdrh(self, sql_id, fname, fis):
        preds = create_preds(fis)
        res = d.sql(f"select ela from v_elapsed_time where sql_id = '{sql_id}' "
                    +f"{'and' if preds else ''} {preds}").fetchall()
        resp_hist = HdrHistogram(1, 1000000000, 1)
        for ela in res:
            resp_hist.record_value(ela[0])
        if not fname:
            fname = f"elapsed_{sql_id}.out"
        with open(fname, 'wb') as f:
            resp_hist.output_percentile_distribution(f, 1.0)
    def outliers(self, sql_id, thresold, fis, statistic='elapsed_time'):
        preds = create_preds(fis)
        thresold_preds = thresold2pred(thresold)
        res = d.sql(f"""select
                            row_number() over(order by sum({statistic}) asc) rownum,
                            cursor_id "cursor",
                            sum({statistic}) "{statistic}",
                            first(ts) ts,
                            case when length(first(file_name)) > 30 then '<...>'||substr(first(file_name), length(first(file_name)) -35, 40) else first(file_name) end file_name,
                            first(line) "first line"
                        from
                            read_parquet('{self.dbdir}')
                        {"where" if preds else ''}
                            {preds}
                        group by
                            cursor_id, span_id
                        having sum({statistic}) {thresold_preds}
                        order by sum({statistic}) desc
                        LIMIT {self.tabsize};
                    """)
        table = tabulate.tabulate(res.fetchall(), tablefmt=self.tabtype,
                headers=['#', 'cursor', statistic, 'timestamp', 'filename', 'first line#'])
        print(table)
    def waits(self, fis, thresold):
        preds = create_preds(fis)
        inner_where = ''
        if thresold:
            inner_where = f" AND span_id in (select span_id from v_elapsed_time where ela > {thresold}) and cursor_id <> '#0'"
        res = d.sql(f"""
			SELECT
                            row_number() over(order by count(event_name) asc),
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
                                ops = 'WAIT'
                                {'AND' if preds else ''} {preds}
                                {inner_where}
                        )
                        GROUP BY
                            event_name
                        ORDER BY
                            COUNT(*) DESC
                        LIMIT {self.tabsize}
                    """)
        table = tabulate.tabulate(res.fetchall(), tablefmt=self.tabtype,
                headers=['#', 'wait', 'count', 'sum(us)', 'median(us)', 'p99(us)', 'max(us)'])
        print(table)
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
        table = tabulate.tabulate(res.fetchall(), tablefmt=self.tabtype,
                headers=['rows', 'files', "sql_id's", 'cursors', 'first_timestamp', 'last_timestamp'])
        print(table)

        res = d.sql(f"""select
                            row_number() over(order by count(*) asc),
                             case when length(first(file_name)) > 30 then '<...>'||substr(first(file_name), length(first(file_name)) -20, 40) else first(file_name) end file_name,
                            count(*) "rows",
                            date_trunc('second', min(ts)) "first timestamp",
                            date_trunc('second', max(ts)) "last timestamp",
                            date_trunc('second', max(ts)) - date_trunc('second', min(ts)) "wallclock time in file",
                            cast(round((max(tim) - min(tim))/1000000) as integer) "elapsed(s)" 
                        from read_parquet('{self.dbdir}')
                        group by file_name order by count(*) desc
                        LIMIT {self.tabsize};
                    """)
        table = tabulate.tabulate(res.fetchall(), tablefmt=self.tabtype,
                headers=['#', 'file_name', 'rows', 'first_timestamp', 'last_timestamp', 'wallclock time in file', 'elapsed(s)'])
        print(table)
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
                            group by span_id
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
        table = tabulate.tabulate(res.fetchall(), tablefmt=self.tabtype,
                headers=['statistic', 'sum', 'median(us)', 'p99(us)', 'max(us)'])
        print(table)

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
        table = tabulate.tabulate(res.fetchall(), tablefmt=self.tabtype,
                headers=['operation', 'count', 'sum of cpu(us)', 'median cpu(us)', 'p99 cpu(us)',
                            'max cpu(us)', 'sum of elapsed(us)', 'median elapsed(us)',
                            'p99 elapsed(us)', 'max elapsed(us)'])
        print(table)

if __name__ == '__main__':

    filters = {}

    parser = argparse.ArgumentParser(description='Generate summary from processed traces')
    subparsers = parser.add_subparsers(dest='action', title='Available subcommands', required=True)

    parser.add_argument('--dbdir', metavar='dbdir', type=str, required=True,
                                    help='Directory for Parquet files')
    parser.add_argument('--table-type', metavar='tabtype', type=str, required=False,
                                default='fancy_outline',
                                help="Which kind of table to generate. For example, 'jira'"
                                +" will generate table that can be copied into the trouble"
                                + " ticket. See the documentation for tabulate. Default:"
                                +" 'fancy_outline'")
    parser.add_argument('--table-size', metavar='tabsize', type=int, required=False,
                                default=30, help='Number of rows in the table. Default is 30')
    parser.add_argument('--remove-idle', metavar='remove_idle', type=bool, required=False,
                                default=True, help='Removes cursor #0 from elapsed time calculation'
                                +'(default)')

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
    hist_parser.add_argument('--start', metavar='start',
                            type=lambda x: filters.__setitem__('start', x),
                            help='Start timestamp in ISO 8601 format')
    hist_parser.add_argument('--end', metavar='end',
                            type=lambda x: filters.__setitem__('end', x),
                            help='End timestamp in ISO 8601 format')

    out_parser = subparsers.add_parser('outliers', help='Displays content of the trace files for '
                         +'the executions that took more than specified amount of time')
    out_parser.add_argument('--sql_id', type= lambda x: filters.__setitem__('sql_id', x),
                            help="Filter by comma separated list of sql_id's")
    out_parser.add_argument('--thresold', type=str, dest='thresold',
                     help="Outlier thresold in microseconds. Accepts comma-separated band of thresolds")
    out_parser.add_argument('--statistic', type=str, dest='stat', default='elapsed_time',
                     help="Parameter for which thresold is applied. More useful parameters are"
                        +" elapsed_time, cpu, rows_processed, ph_reads, cr_reads, current_reads."
                        +" For the full list see the schema definition in db/arrow.py")
    out_parser.add_argument('--start', metavar='start',
                            type=lambda x: filters.__setitem__('start', x),
                            help='Start timestamp in ISO 8601 format')
    out_parser.add_argument('--end', metavar='end',
                            type=lambda x: filters.__setitem__('end', x),
                            help='End timestamp in ISO 8601 format')

    waits_parser = subparsers.add_parser('waits', help='Prints summary of the wait events for '
                    +'sql_id')
    waits_parser.add_argument('--start', metavar='start',
                            type=lambda x: filters.__setitem__('start', x),
                            help='Start timestamp in ISO 8601 format')
    waits_parser.add_argument('--end', metavar='end',
                            type=lambda x: filters.__setitem__('end', x),
                            help='End timestamp in ISO 8601 format')
    waits_parser.add_argument('--client_id', type= lambda x: filters.__setitem__('client_id', x),
                            help='Filter by CLIENT ID')
    waits_parser.add_argument('--sql_id', type= lambda x: filters.__setitem__('sql_id', x),
                            help="Filter by comma separated list of sql_id's")
    waits_parser.add_argument('--thresold', type=str, dest='thresold',
                     help="Outlier thresold in microseconds. Minimum query response time from "
                        +"which the wait events are included")

    dbs_parser = subparsers.add_parser('db', help='Prints some statistics about the stuff in'
                        +'Parquet files and recorded trace files')

    sql_parser = subparsers.add_parser('sql', help='Summary for specific sql_id ')
    sql_parser.add_argument('--sql_id', type=str, dest='sql_id')


    args = parser.parse_args()

    s = SummaryDuckdb(args.dbdir + '/*', args.table_type, args.table_size, args.remove_idle)

    if args.action == 'summary':
        s.summary(filters)
    if args.action == 'cursor-summary':
        s.cursor_summary()
    elif args.action == 'histogram':
        if args.sql_id:
            for sqlid in args.sql_id.split(','):
                s.create_hdrh(args.sql_id, args.fname, filters)
        if args.wait_name:
            s.wait_histogram(args.wait_name, args.fname)
    elif args.action == 'outliers':
        s.outliers(args.sql_id, args.thresold, filters, args.stat)
    elif args.action == 'waits':
        s.waits(filters, args.thresold)
    elif args.action == 'db':
        s.db()
    elif args.action == 'sql':
        s.sql(args.sql_id)
