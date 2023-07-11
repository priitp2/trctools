`trc2db` turns Oracle SQL trace (also known as event 10046) files into Parquet, or loads it into Oracle database.
It tracle database client interactions with the database and assigns unique id's.

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

