import re
from ops import Ops
import logging
import datetime

class Filer:
    def __init__(self):
        self.res_matcher = re.compile(r'''^(={21})''')
        # 2023-05-19T05:28:00.339263+02:00
        self.date_matcher = re.compile(r'''^\*{3} (\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}\+\d\d:\d\d)''')
        self.timezone_matcher = re.compile(r'''(?:.*)\+(\d\d:\d\d)''')
        self.stars_matcher = re.compile(r'''^\*\*\* (SESSION ID:|CLIENT ID:|SERVICE NAME:|MODULE NAME:|ACTION NAME:|CLIENT DRIVER:|CONTAINER ID:)(\(.*\)) (.*)''')
        self.call_matcher = re.compile(r'''^(PARSE|PARSING IN CURSOR|EXEC|FETCH|WAIT|CLOSE|BINDS|STAT) (#\d+)(:| )(.*)''')
        self.logger = logging.getLogger(__name__)
    def process_file(self, tr, fname, sql_ids):

        line_count = 0
        self.logger.info('process_file: processing %s', fname)
        with open(fname, 'r') as f:
            for line in f:
                line_count += 1

                match = self.call_matcher.match(line)
                if match:
                    if match.group(1) == 'PARSING IN CURSOR':
                        tr.add_parsing_in(match.group(2), match.group(4))
                    elif match.group(1) == 'PARSE':
                        last_parse = Ops('PARSE', match.group(2), match.group(4), fname, line_count)
                        tr.add_parse(match.group(2), last_parse)
                    elif match.group(1) == 'EXEC':
                        last_exec = Ops('EXEC', match.group(2), match.group(4), fname, line_count)
                        tr.add_exec(match.group(2), last_exec)
                    elif match.group(1) == 'FETCH':
                        f = Ops('FETCH', match.group(2), match.group(4), fname, line_count)
                        tr.add_fetch(match.group(2), f)
                    elif match.group(1) == 'WAIT':
                        w = Ops('WAIT', match.group(2), match.group(4), fname, line_count)
                        tr.add_wait(match.group(2), w)
                    elif match.group(1) == 'CLOSE':
                        c = Ops('CLOSE', match.group(2), match.group(4), fname, line_count)
                        tr.add_close(match.group(2), c)
                    elif match.group(1) == 'STAT':
                        s = Ops('STAT', match.group(2), match.group(4), fname, line_count)
                        tr.add_stat(match.group(2), s)
                    elif match.group(1) == 'BINDS':
                        pass
                    else:
                        print(match.group(1))
                        print("process_file: no match: {}".format(line))
                    continue

                match = self.res_matcher.match(line)
                if match:
                    self.logger.debug('reset tracker')
                    tr.reset()
                    continue

                match = self.date_matcher.match(line)
                if match:
                    ts2 = datetime.datetime.strptime(match.group(1), '%Y-%m-%dT%H:%M:%S.%f%z')
                    tr.db.insert_ops(Ops('STAR', None, None, fname, line_count, 'DATETIME', ts2).to_list(tr.db.get_exec_id(), None))
                    continue

                match = self.stars_matcher.match(line)
                if match:
                    ts2 = datetime.datetime.strptime(match.group(3), '%Y-%m-%dT%H:%M:%S.%f%z')
                    tr.db.insert_ops(Ops('STAR', None, match.group(2).strip('()'), fname, line_count, match.group(1).strip(':'), ts2).to_list(tr.db.get_exec_id(), None))
                    continue

        self.logger.info('process_file: %s done', fname)
        return line_count

