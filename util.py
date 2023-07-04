import datetime
import logging
import re

from ops import Ops

class Filer:
    def __init__(self):
        self.res_matcher = re.compile(r'''^(={21})''')
        self.pic_matcher = re.compile(r'''^END OF STMT(.*)''')
        self.xctend_matcher = re.compile(r'''^XCTEND (.*)''')
        # 2023-05-19T05:28:00.339263+02:00
        self.date_matcher = re.compile(r'''^\*{3} (\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}\+\d\d:\d\d)''')
        self.timezone_matcher = re.compile(r'''(?:.*)\+(\d\d:\d\d)''')
        self.stars_matcher = re.compile(r'''^\*\*\* (SESSION ID:|CLIENT ID:|SERVICE NAME:|MODULE NAME:|ACTION NAME:|CLIENT DRIVER:|CONTAINER ID:)(\(.*\)) (.*)''')
        self.call_matcher = re.compile(r'''^(PARSE|PARSING IN CURSOR|EXEC|FETCH|WAIT|CLOSE|BINDS|STAT) (#\d+)(:| )(.*)''')
        self.file_header_matcher = re.compile(r'''^(Build label|ORACLE_HOME|System name|Node name|Release|Version|Machine|Instance name|Redo thread mounted by this instance|Oracle process number|Unix process pid):\s+(.*)''')
        self.logger = logging.getLogger(__name__)
    def process_file(self, tracker, fname):

        line_count = 0
        in_binds = False
        in_pic = False
        binds = ()
        pic = None
        self.logger.info('process_file: processing %s', fname)
        with open(fname, 'r') as trace:
            for line in trace:
                line_count += 1

                # Skip the first line
                if line_count < 4:
                    continue

                # FIXME: skip lines with CR
                if len(line) < 2:
                    continue


                match = self.call_matcher.match(line)
                if match:
                    if in_binds:
                        in_binds = False
                        ops = Ops('BINDS', binds[0], "".join(binds[3]), binds[1], binds[2])
                        tracker.add_binds(binds[0], ops)
                        #(cursor, file name, line no, [binds])
                        binds = ()

                    if match.group(1) == 'PARSING IN CURSOR':
                        in_pic = True
                        pic = Ops('PIC', match.group(2), match.group(4), fname, line_count)
                        tracker.add_pic(pic.cursor, pic)
                    if match.group(1) == 'PARSE':
                        ops = Ops('PARSE', match.group(2), match.group(4), fname, line_count)
                        tracker.add_parse(match.group(2), ops)
                    if match.group(1) == 'EXEC':
                        ops = Ops('EXEC', match.group(2), match.group(4), fname, line_count)
                        tracker.add_exec(match.group(2), ops)
                    if match.group(1) == 'FETCH':
                        ops = Ops('FETCH', match.group(2), match.group(4), fname, line_count)
                        tracker.add_fetch(match.group(2), ops)
                    if match.group(1) == 'WAIT':
                        ops = Ops('WAIT', match.group(2), match.group(4), fname, line_count)
                        tracker.add_wait(match.group(2), ops)
                    if match.group(1) == 'CLOSE':
                        ops = Ops('CLOSE', match.group(2), match.group(4), fname, line_count)
                        tracker.add_close(match.group(2), ops)
                    if match.group(1) == 'STAT':
                        ops = Ops('STAT', match.group(2), match.group(4), fname, line_count)
                        tracker.add_stat(match.group(2), ops)
                    if match.group(1) == 'BINDS':
                        in_binds = True
                        binds = (match.group(2), fname, line_count, [])
                    continue

                if in_binds:
                    binds[3].append(line)
                    continue

                if in_pic:
                    match = self.pic_matcher.match(line)
                    if match:
                        in_pic = False
                        pic = None
                    else:
                        pic.raw.append(line)
                    continue

                match = self.xctend_matcher.match(line)
                if match:
                    tracker.db.insert_ops(Ops('XCTEND', None, match.group(1), fname,
                                        line_count).to_list(tracker.db.get_exec_id(), None))
                    continue

                match = self.res_matcher.match(line)
                if match:
                    self.logger.debug('reset tracker')
                    tracker.reset()
                    continue

                match = self.date_matcher.match(line)
                if match:
                    ts2 = datetime.datetime.strptime(match.group(1), '%Y-%m-%dT%H:%M:%S.%f%z')
                    tracker.db.insert_ops(Ops('STAR', None, None, fname, line_count, 'DATETIME',
                                            ts2).to_list(tracker.db.get_exec_id(), None))
                    continue

                match = self.stars_matcher.match(line)
                if match:
                    ts2 = datetime.datetime.strptime(match.group(3), '%Y-%m-%dT%H:%M:%S.%f%z')
                    tracker.db.insert_ops(Ops('STAR', None, match.group(2).strip('()'), fname,
                                                line_count, match.group(1).strip(':'),
                                                ts2).to_list(tracker.db.get_exec_id(), None))
                    continue

                match = self.file_header_matcher.match(line)
                if match:
                    tracker.db.insert_ops(Ops('STAR', None, match.group(2), fname, line_count,
                                            match.group(1), None).to_list(tracker.db.get_exec_id(), None))
                    continue

                #print("non-matching line: {}".format(line))

        self.logger.info('process_file: %s done', fname)
        return line_count
