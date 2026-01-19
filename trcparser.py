import bz2
import collections
import datetime
from enum import Enum
import fnmatch
import gzip
import lzma
import re
from sys import exception
from traceback import print_exception
from typing import Optional
from zoneinfo import ZoneInfo
import filetype

from ops import ops_factory, Ops

__doc__ = '''Parser for the SQL trace files. '''

RES_MATCHER = re.compile(r'''^(={21})''')
PIC_MATCHER = re.compile(r'''^END OF STMT(.*)''')

# 2023-05-19T05:28:00.339263+02:00
DATE_MATCHER = re.compile(r'''^\*{3} (\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}\+\d\d:\d\d)''')
TIMEZONE_MATCHER = re.compile(r'''(?:.*)\+(\d\d:\d\d)''')

STARS_MATCHER = re.compile(r'''^\*\*\* (SESSION ID:|CLIENT ID:|SERVICE NAME:|MODULE NAME:'''
        +r'''|ACTION NAME:|CLIENT DRIVER:|CONTAINER ID:|CLIENT IP:|CONNECTION ID:)(\(.*\)) (.*)''')
CALL_MATCHER = re.compile(r'''^(PARSE|EXEC|FETCH|WAIT|CLOSE|STAT|ERROR|PARSING IN CURSOR|BINDS|PARSE ERROR) (#\d+)(:| )(.*)''')
XLOB_MATCHER = re.compile(r'''^(LOB[A-Z]+|XCTEND):* (.*)''')

FILE_HEADER_MATCHER = re.compile(r'''^(Build label|ORACLE_HOME|System name'''
                        +r'''|Node name|Release|Version|Machine|CLID|Instance name'''
                        +r'''|Instance number|Database name|Database unique name'''
                        +r'''|Database unique id'''
                        +r'''|Redo thread mounted by this instance|Oracle process number'''
                        +r'''|Unix process pid):\s+(.*)''')

def get_timestamp(instr) -> datetime.datetime:
    """Checks if input has a time zone or not, and adjusts the format accordingly."""
    tz_match = TIMEZONE_MATCHER.match(instr)
    if tz_match:
        date_format = '%Y-%m-%dT%H:%M:%S.%f%z'
    else:
        date_format = '%Y-%m-%dT%H:%M:%S.%f'
    return datetime.datetime.strptime(instr, date_format).astimezone(tz=ZoneInfo('UTC'))

def get_opener(fname):
    """Tries to guess the file type and returns corresponding open function."""
    match filetype.guess(fname):
        case None if fnmatch.fnmatch(fname, '*.lzma'):
            # Filetype does not recognize legacy lzma?
            return lzma.open
        case None:
            return open
        case ft if ft.mime == 'application/gzip':
            return gzip.open
        case ft if ft.mime == 'application/x-bzip2':
            return bz2.open
        case ft if ft.mime == 'application/x-xz':
            return lzma.open
        case other_type :
            raise RuntimeError(f"Unsupported file type {other_type}")

class ParserState(Enum):
    """Keeps track of the parser state"""
    NOC = 0 # Most of the events are single-line
    BINDS = 1 # BINDS and PARSE ERROR do not have a end marker
    PARSE_ERROR = 2
    PIC = 3 # PARSING IN CURSOR is a multi-line event with end marker

def ex_helper(line, line_count):
    """Logs errors from lower layers"""
    print(f"Got exception whole handling the line, ignoring. Offending line #{line_count}:")
    print(line)
    print_exception(exception())

def init_fmeta(file_name: str) -> collections.defaultdict():
    """fmeta contains file- or session level parameters, from line number to CLIENT_ID.
        It returns None for the keys that are not present"""

    if not file_name:
        raise ValueError('Missing file name')
    fmeta = collections.defaultdict(lambda: None)
    fmeta['FILE_NAME'] = file_name
    fmeta['LINE_COUNT'] = 0
    return fmeta

def process_file(tracker, fname, orphans=False) -> collections.defaultdict():
    """The god function. Does everything: reads the input file and parses the lines. """

    parser_state: int = ParserState.NOC
    ops: Optional[Ops] = None

    error_count: int = 0
    file_meta = init_fmeta(fname)
    opener = get_opener(fname)
    with opener(fname, 'rt', encoding='utf_8') as trace:
        for (file_meta['LINE_COUNT'], line) in enumerate(trace, 1):

            # Skip the first 3 lines
            if file_meta['LINE_COUNT'] < 4:
                continue

            # FIXME: skip lines with CR
            if len(line) < 2:
                continue

            if parser_state == ParserState.BINDS and line.startswith(' '):
                ops.add_line(line)
                continue

            if (m := CALL_MATCHER.match(line)) is not None:

                match m.group(1):
                    case 'BINDS':
                        parser_state = ParserState.BINDS
                    case 'PARSE ERROR':
                        parser_state = ParserState.PARSE_ERROR
                    case 'PARSING IN CURSOR':
                        parser_state = ParserState.PIC
                    case _ if parser_state != ParserState.NOC:
                        parser_state = ParserState.NOC
                        ops = None

                try:
                    ops = ops_factory(m.group(1), m.group(2), m.group(4), file_meta,
                                        tracker.time_tracker.get_wc)
                except (IndexError, ValueError):
                    print(f"process_file: ops = {ops}")
                    ex_helper(line, file_meta['LINE_COUNT'])
                    error_count += 1
                    continue
                try:
                    # This throws error if cursor in STAT is malformed
                    tracker.add_ops(m.group(2), ops)
                except ValueError:
                    ex_helper(line, file_meta['LINE_COUNT'])
                    error_count += 1
                    if m.group(1) in ('STAT', 'BINDS'):
                        continue
                    raise
                continue

            match parser_state:
                case ParserState.PARSE_ERROR:
                    ops.add_line(line)
                    continue
                case ParserState.PIC:
                    if (match := PIC_MATCHER.match(line)) is not None:
                        parser_state = ParserState.NOC
                    else:
                        ops.add_line(line)
                    continue


            if (m := XLOB_MATCHER.match(line)) is not None:
                try:
                    lob = ops_factory(m.group(1), None, m.group(2), file_meta,
                                    tracker.time_tracker.get_wc)
                except (IndexError, ValueError):
                    ex_helper(line, file_meta['LINE_COUNT'])
                    error_count += 1
                    continue
                tracker.db.add_ops(tracker.db.get_span_id(), None, [lob])
                continue

            # '=====================' starts new tracing span
            if (m := RES_MATCHER.match(line)) is not None:
                tracker.reset()
                continue

            if (m := DATE_MATCHER.match(line)) is not None:
                ts2 = get_timestamp(m.group(1))
                tracker.time_tracker.reset(ts2)
                dt = ops_factory('STAR', None, None, file_meta, lambda x: None,
                                    'DATETIME', ts2)
                tracker.db.add_ops(tracker.db.get_span_id(), None, [dt])
                continue

            if (m := STARS_MATCHER.match(line)) is not None:
                ts2 = get_timestamp(m.group(3))
                name = m.group(1).strip(':')
                value = m.group(2).strip('()')
                file_meta[name] = value
                tracker.time_tracker.reset(ts2)
                star = ops_factory('STAR', None, value, file_meta, lambda x: None, name, ts2)
                tracker.db.add_ops(tracker.db.get_span_id(), None, [star])
                continue

            if (m := FILE_HEADER_MATCHER.match(line)) is not None:
                header = ops_factory('HEADER', None, m.group(2), file_meta,
                                        lambda x: None, m.group(1), None)
                tracker.db.add_ops(tracker.db.get_span_id(), None, [header])
                continue

            if orphans:
                print(f"non-matching line: {line}")

    return (file_meta['LINE_COUNT'], error_count)
