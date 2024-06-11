#!/usr/bin/env python3.11

import argparse
import time
import trcparser
from call_tracker import CallTracker

__doc__ = """Turn Oracle SQL trace files into Parquet, or inserts them into a Oracle database,
            or sends them to the OTLP-capable tracing aggregator."""

def get_backend(args):
    """Inspects arguments and initialises suitable backend."""
    backend = None
    if args.db == 'oracle':
        print('Using backend: oracle')
        from backend.oracle import Backend
        backend = Backend()
    elif args.db == 'parquet':
        print('Using backend: arrow/parquet')
        from backend.arrow import Backend
        backend = Backend(args.dbdir, args.file_prefix)
    elif args.db == 'otlp':
        print('Using OTLP exporter')
        from backend.otlp import Backend
        backend = Backend(args.traceid)
    else:
        print('Using backend: None')
    return backend

def process_files(args):
    backend = get_backend(args)
    tracker = CallTracker(backend)

    no_files = len(args.trace_files)
    fcount = 1
    cumul_lines = 0
    start_time = time.time_ns()
    for fname in args.trace_files:
        print(f"[{fcount}/{no_files}] processing file {fname}")
        start = time.time_ns()
        lines = trcparser.process_file(tracker, fname, args.orphans)
        cumul_lines += lines
        fcount += 1
        print(f"   -> {lines} lines, {int((time.time_ns() - start)/1000000000)} seconds")

    tracker.flush()
    print(f"Processed {cumul_lines} lines in {int((time.time_ns() - start_time)/1000000000)} "
            +"seconds")

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('trace_files', metavar='files', type=str, nargs='+',
                            help='Trace files to process')
parser.add_argument('--backend', type=str, default = 'parquet', dest='db',
                    help="Persists raw data in the some kind of storage, supported"
                    +" implementations: oracle, parquet, otlp")
parser.add_argument('--dbdir', type=str, default = './', dest='dbdir',
                    help="Directory for the parquet files")
parser.add_argument('--db-file-prefix', type=str, default = 'parquet', dest='file_prefix',
                    help="Parquet file name prefix")
parser.add_argument('--traceid-parameter', type=str, default = 'CLIENT ID', dest='traceid',
                    help="Parameter that service uses to pass the trace_id to the database")
parser.add_argument('--log-orphans', type=bool, default = False, dest='orphans',
                    help="Logs lines not matched by the parser")

arguments = parser.parse_args()


if __name__ == '__main__':
    process_files(arguments)
