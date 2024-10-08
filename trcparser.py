
import bz2
import collections
import datetime
import fnmatch
import gzip
import lzma
import re
from zoneinfo import ZoneInfo
import filetype

from ops import ops_factory

__doc__ = '''Parser for the SQL trace files. '''

RES_MATCHER = re.compile(r'''^(={21})''')
PIC_MATCHER = re.compile(r'''^END OF STMT(.*)''')
XCTEND_MATCHER = re.compile(r'''^XCTEND (.*)''')

# 2023-05-19T05:28:00.339263+02:00
DATE_MATCHER = re.compile(r'''^\*{3} (\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}\+\d\d:\d\d)''')
TIMEZONE_MATCHER = re.compile(r'''(?:.*)\+(\d\d:\d\d)''')

STARS_MATCHER = re.compile(r'''^\*\*\* (SESSION ID:|CLIENT ID:|SERVICE NAME:|MODULE NAME:'''
        +r'''|ACTION NAME:|CLIENT DRIVER:|CONTAINER ID:|CLIENT IP:|CONNECTION ID:)(\(.*\)) (.*)''')
CALL_MATCHER = re.compile(r'''^(PARSE|PARSING IN CURSOR|EXEC|FETCH|WAIT|CLOSE'''
                        +r'''|BINDS|STAT|ERROR) (#\d+)(:| )(.*)''')
LOB_MATCHER = re.compile(r'''^(LOB[A-Z]+): (.*)''')

FILE_HEADER_MATCHER = re.compile(r'''^(Build label|ORACLE_HOME|System name'''
                        +r'''|Node name|Release|Version|Machine|Instance name'''
                        +r'''|Redo thread mounted by this instance|Oracle process number'''
                        +r'''|Unix process pid):\s+(.*)''')

def get_timestamp(instr):
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

def process_file(tracker, fname, orphans=False):
    """The god function. Does everything: reads the input file and parses the lines. """

    in_binds = False
    in_pic = False
    binds = ()
    pic = None

    file_meta = collections.defaultdict(lambda: None)
    file_meta['FILE_NAME'] = fname
    file_meta['LINE_COUNT'] = 0

    opener = get_opener(fname)
    with opener(fname, 'rt', encoding='utf_8') as trace:
        for line in trace:
            file_meta['LINE_COUNT'] += 1

            # Skip the first 3 lines
            if file_meta['LINE_COUNT'] < 4:
                continue

            # FIXME: skip lines with CR
            if len(line) < 2:
                continue

            if (match := CALL_MATCHER.match(line)) is not None:
                if in_binds:
                    in_binds = False
                    binds = None

                if match.group(1) == 'BINDS':
                    in_binds = True
                    binds = ops_factory('BINDS', match.group(2), '', file_meta,
                                        tracker.time_tracker.get_wc)
                    tracker.add_ops(binds.cursor, binds)
                    continue
                if match.group(1) == 'PARSING IN CURSOR':
                    in_pic = True
                    pic = ops_factory('PIC', match.group(2), match.group(4), file_meta,
                                        tracker.time_tracker.get_wc)
                    tracker.add_pic(pic.cursor, pic)
                    continue
                ops = ops_factory(match.group(1), match.group(2), match.group(4), file_meta,
                                        tracker.time_tracker.get_wc)
                tracker.add_ops(match.group(2), ops)
                continue

            if in_binds:
                binds.add_line(line)
                continue

            if in_pic:
                if (match := PIC_MATCHER.match(line)) is not None:
                    in_pic = False
                    pic = None
                else:
                    pic.add_line(line)
                continue

            if (match := LOB_MATCHER.match(line)) is not None:
                ops = ops_factory(match.group(1), None, match.group(2), file_meta,
                                    tracker.time_tracker.get_wc)
                tracker.db.add_ops(tracker.db.get_exec_id(), None, [ops])
                continue

            if (match := XCTEND_MATCHER.match(line)) is not None:
                ops = ops_factory('XCTEND', None, match.group(1), file_meta,
                                    tracker.time_tracker.get_wc)
                tracker.db.add_ops(tracker.db.get_exec_id(), None, [ops])
                continue

            # FIXME: make this configurable
            if (match := RES_MATCHER.match(line)) is not None:
                tracker.reset()
                continue

            if (match := DATE_MATCHER.match(line)) is not None:
                ts2 = get_timestamp(match.group(1))
                tracker.time_tracker.reset(ts2)
                ops = ops_factory('STAR', None, None, file_meta, lambda x: None,
                                    'DATETIME', ts2)
                tracker.db.add_ops(tracker.db.get_exec_id(), None, [ops])
                continue

            if (match := STARS_MATCHER.match(line)) is not None:
                ts2 = get_timestamp(match.group(3))
                name = match.group(1).strip(':')
                value = match.group(2).strip('()')
                file_meta[name] = value
                tracker.time_tracker.reset(ts2)
                star = ops_factory('STAR', None, value, file_meta, lambda x: None, name, ts2)
                tracker.db.add_ops(tracker.db.get_exec_id(), None, [star])
                continue

            if (match := FILE_HEADER_MATCHER.match(line)) is not None:
                header = ops_factory('HEADER', None, match.group(2), file_meta,
                                        lambda x: None, match.group(1), None)
                tracker.db.add_ops(tracker.db.get_exec_id(), None, [header])
                continue

            if orphans:
                print(f"non-matching line: {line}")

    return file_meta['LINE_COUNT']
