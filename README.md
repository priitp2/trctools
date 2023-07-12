`trc2db` turns Oracle SQL trace (also known as event 10046) files into Parquet, or loads it into Oracle database.

 `summary.py` contains pre-canned queries and statistics about the generated Parquet file(s).

# Installation

```
git clone ...
```

For trc2db.py
```
$ pip.3.11 install pyarrow oracledb
```
For summary.py
```
$pip3.11 install pyarrow duckdb HdrHistogram scipy
```
# Usage

```
./trc2db.py --dbdir /home/pripii/passwd_mng3 trace0*/*
```

# Data schema

trc2db tracks database client interactions with the database, and assigns `exec_id` to each interactions. `ops` is a type of
the database call, with some exceptions. Lines from the file header have a ops `HEADER`, and lines starting with `***` have ops
`STAR`. 

Some of the properties are renamed to something more human friendly, for example `e` to `elapsed_time`, but more esoteric
ones come as they are in the trace files.

## `event_name` and `event_raw`

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

# summary.py

It contains some pre-canned examples what can be done in Duckdb. Available subcommands:

|   name        |   action                                                                                      |
|---------------|-----------------------------------------------------------------------------------------------|
| summary       | Prints out list of SQL queries, execution counts, median and p99 execution times, etc.        |
| histogram     | Creates response time histogram for a query                                                   |
| outliers      | Analyses wait event for the executions that took more than specified amount of time.          |
| waits         | Prints summary for the wait events.                                                           |
| wait_histogram| Creates histogram for the elapsed time for a specific wait event                              |
| db            | Prints some statistics about the stuff in PArquet files and recorded trace files              |
| norm          | Checks ow well the data fits the well know distributions. Defaults to the normal distribution.|
|               | Use this before you start demanding averages and variance!                                    |
