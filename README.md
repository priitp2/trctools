`trc2db` turns Oracle SQL trace (also known as event 10046) files into Parquet, or loads it into Oracle database.

 `summary.py` contains pre-canned queries and statistics about the generated Parquet file(s).

# Installation

It is tested with Python 3.11, but in principle it should work with older versions as well.

```
git clone ...
```

For trc2db.py
```
$ pip.3.11 install pyarrow oracledb
```

If you do not intend to use Oracle as a backend, then `oracledb` can be omitted.

For summary.py
```
$pip3.11 install pyarrow duckdb HdrHistogram scipy
```

# Usage

```
./trc2db.py --dbdir /home/pripii/passwd_mng3 trace0*/*
```
# summary.py

It contains some pre-canned examples what can be done in Duckdb.

```
$ ./summary.py -h
usage: summary.py [-h] [--sql_id SQL_ID] [--thresold THRESOLD] [--dbdir dbdir] [--wait_name WAIT_NAME] [--output FNAME]
                  [--test TEST_TYPE] [--dist DIST] [--dist-args DIST-ARGS]
                  {summary,histogram,outliers,waits,wait_histogram,db,norm}

Generate summary from processed traces

positional arguments:
  {summary,histogram,outliers,waits,wait_histogram,db,norm}
                        Directory for Parquet files

options:
  -h, --help            show this help message and exit
  --sql_id SQL_ID       Comma separated list of sql_id's for which summary is produced
  --thresold THRESOLD   Thresold in microsecond for which the outliers are displayed
  --dbdir dbdir         Directory for Parquet files
  --wait_name WAIT_NAME
                        Name for the wait_histogram command
  --output FNAME        Output for the wait_histogram command
  --test TEST_TYPE      For the normality test, type of the test performed. Accepted values: shapiro, anderson (Anderson-Darling),
                        kstest (Kolmogorov-Smirnov). See scipy.statistics documentation for the explanation
  --dist DIST           For normality test, specifies cdf for Kolmogorov-Smirnov, or distribution for Anderson-Darling.
  --dist-args DIST-ARGS
                        Arguments for the CDF in normality/goodness-of-fit test

```

## Available subcommands:

|   name        |   action                                                                                      |
|---------------|-----------------------------------------------------------------------------------------------|
| summary       | Prints out list of SQL queries, execution counts, median and p99 execution times, etc.        |
| histogram     | Creates response time histogram for a sql_id. Generates file `elapsed_{sql_id}.out`           |
| outliers      | Displays content of the trace files for the executions that took more than specified amount of time.|
| waits         | Prints summary of the wait events for sql_id.                                                           |
| wait_histogram| Creates histogram of the elapsed time for a specific wait event                              |
| db            | Prints some statistics about the stuff in Parquet files and recorded trace files              |
| norm          | Checks ow well the data fits the well know distributions. Defaults to the normal distribution. Use this before you start demanding averages and variance!|

### summary

![Screenshot of summary output](doc/summary.png)

### histogram

```$ ./summary.py histogram --sql_id 7dh01v2jgss7c --dbdir /home/pripii/parquet```

`--sql_id` is mandatory parameter. This creates file `elapsed_7dh01v2jgss7c.out`.

### outliers

![Screenshot of outliers output](doc/outliers.png)

# Data schema

trc2db tracks database client interactions with the database, and assigns `exec_id` to each interactions. `ops` is a type of
the database call, with some exceptions. Lines from the file header have a ops `HEADER`, and lines starting with `***` have ops
`STAR`. 

Some of the properties are renamed to something more human friendly, for example `e` to `elapsed_time`, but more esoteric
ones come as they are in the trace files.

### `event_name` and `event_raw`

In case of `WAIT`, `event_name` contains wait event name and `event_raw` unparsed text. In case of file headers, `event_name`
is the name of the header field and `event_raw` contains the value. Same goes with the lines starting with `***`. Timestamps
in those lines are persisted in `ts`.  

|      name      |    type    |                                            logical_type                                             |
|----------------|------------|-----------------------------------------------------------------------------------------------------|
| exec_id        | INT64      |                                                                                                     |
| sql_id         | BYTE_ARRAY | StringType()                                                                                        |
| cursor_id      | BYTE_ARRAY | StringType()                                                                                        |
| ops            | BYTE_ARRAY | StringType()                                                                                        |
| cpu_time       | INT64      |                                                                                                     |
| elapsed_time   | INT64      |                                                                                                     |
| ph_reads       | INT64      |                                                                                                     |
| cr_reads       | INT64      |                                                                                                     |
| current_reads  | INT64      |                                                                                                     |
| cursor_missed  | INT64      |                                                                                                     |
| rows_processed | INT64      |                                                                                                     |
| rec_call_dp    | INT64      |                                                                                                     |
| opt_goal       | INT64      |                                                                                                     |
| plh            | INT64      |                                                                                                     |
| tim            | INT64      |                                                                                                     |
| c_type         | INT64      |                                                                                                     |
| event_name     | BYTE_ARRAY | StringType()                                                                                        |
| event_raw      | BYTE_ARRAY | StringType()                                                                                        |
| file_name      | BYTE_ARRAY | StringType()                                                                                        |
| line           | INT64      |                                                                                                     |
| ts             | INT64      | TimestampType(isAdjustedToUTC=0, unit=TimeUnit(MILLIS=<null>, MICROS=MicroSeconds(), NANOS=<null>)) |
| len            | INT64      |                                                                                                     |
| uid            | INT64      |                                                                                                     |
| oct            | INT64      |                                                                                                     |
| lid            | INT64      |                                                                                                     |
| hv             | INT64      |                                                                                                     |
| ad             | BYTE_ARRAY | StringType()                                                                                        |
| rlbk           | BYTE_ARRAY | StringType()                                                                                        |
| rd_only        | BYTE_ARRAY | StringType()                                                                                        |

