import datetime
import re

from ops import ops_factory

__doc__ = '''Parser for the SQL trace files. '''

RES_MATCHER = re.compile(r'''^(={21})''')
PIC_MATCHER = re.compile(r'''^END OF STMT(.*)''')
XCTEND_MATCHER = re.compile(r'''^XCTEND (.*)''')

# 2023-05-19T05:28:00.339263+02:00
DATE_MATCHER = re.compile(r'''^\*{3} (\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}\+\d\d:\d\d)''')
#TIMEZONE_MATCHER = re.compile(r'''(?:.*)\+(\d\d:\d\d)''')

STARS_MATCHER = re.compile(r'''^\*\*\* (SESSION ID:|CLIENT ID:|SERVICE NAME:|MODULE NAME:'''
        +r'''|ACTION NAME:|CLIENT DRIVER:|CONTAINER ID:|CLIENT IP:|CONNECTION ID:)(\(.*\)) (.*)''')
CALL_MATCHER = re.compile(r'''^(PARSE|PARSING IN CURSOR|EXEC|FETCH|WAIT|CLOSE'''
                        +r'''|BINDS|STAT) (#\d+)(:| )(.*)''')
LOB_MATCHER = re.compile(r'''^(LOB[A-Z]+): (.*)''')

FILE_HEADER_MATCHER = re.compile(r'''^(Build label|ORACLE_HOME|System name'''
                        +r'''|Node name|Release|Version|Machine|Instance name'''
                        +r'''|Redo thread mounted by this instance|Oracle process number'''
                        +r'''|Unix process pid):\s+(.*)''')

def process_file(tracker, fname, orphans=False):

    line_count = 0
    in_binds = False
    in_pic = False
    binds = ()
    pic = None
    with open(fname, 'r', encoding='utf_8') as trace:
        for line in trace:
            line_count += 1

            # Skip the first 3 lines
            if line_count < 4:
                continue

            # FIXME: skip lines with CR
            if len(line) < 2:
                continue

            match = CALL_MATCHER.match(line)
            if match:
                if in_binds:
                    in_binds = False
                    binds = None

                if match.group(1) == 'BINDS':
                    in_binds = True
                    binds = ops_factory('BINDS', match.group(2), [], fname, line_count,
                                        tracker.time_tracker.get_wc)
                    tracker.add_ops(binds.cursor, binds)
                    continue
                if match.group(1) == 'PARSING IN CURSOR':
                    in_pic = True
                    pic = ops_factory('PIC', match.group(2), match.group(4), fname, line_count,
                                        tracker.time_tracker.get_wc)
                    tracker.add_pic(pic.cursor, pic)
                    continue
                ops = ops_factory(match.group(1), match.group(2), match.group(4), fname,
                                    line_count, tracker.time_tracker.get_wc)
                tracker.add_ops(match.group(2), ops)
                continue

            if in_binds:
                binds.raw.append(line)
                continue

            if in_pic:
                match = PIC_MATCHER.match(line)
                if match:
                    in_pic = False
                    pic = None
                else:
                    pic.raw.append(line)
                continue

            match = LOB_MATCHER.match(line)
            if match:
                ops = ops_factory(match.group(1), None, match.group(2), fname,line_count,
                                    tracker.time_tracker.get_wc)
                tracker.db.insert_ops(ops.to_list(tracker.db.get_exec_id(), None))
                continue

            match = XCTEND_MATCHER.match(line)
            if match:
                ops = ops_factory('XCTEND', None, match.group(1), fname, line_count,
                                    tracker.time_tracker.get_wc)
                tracker.db.insert_ops(ops.to_list(tracker.db.get_exec_id(), None))
                continue

            # FIXME: make this configurable
            match = RES_MATCHER.match(line)
            if match:
                tracker.reset()
                continue

            match = DATE_MATCHER.match(line)
            if match:
                ts2 = datetime.datetime.strptime(match.group(1), '%Y-%m-%dT%H:%M:%S.%f%z')
                tracker.time_tracker.reset(ts2)
                ops = ops_factory('STAR', None, None, fname, line_count, lambda x: None,
                                    'DATETIME', ts2)
                tracker.db.insert_ops(ops.to_list(tracker.db.get_exec_id(), None))
                continue

            match = STARS_MATCHER.match(line)
            if match:
                ts2 = datetime.datetime.strptime(match.group(3), '%Y-%m-%dT%H:%M:%S.%f%z')
                tracker.time_tracker.reset(ts2)
                star = ops_factory('STAR', None, match.group(2).strip('()'), fname, line_count,
                                    lambda x: None, match.group(1).strip(':'), ts2)
                tracker.db.insert_ops(star.to_list(tracker.db.get_exec_id(), None))
                continue

            match = FILE_HEADER_MATCHER.match(line)
            if match:
                header = ops_factory('HEADER', None, match.group(2), fname, line_count,
                                        lambda x: None, match.group(1), None)
                tracker.db.insert_ops(header.to_list(tracker.db.get_exec_id(), None))
                continue

            if orphans:
                print(f"non-matching line: {line}")

    return line_count
