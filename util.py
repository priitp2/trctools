import re
from ops import Ops
import logging

class Filer:
    def __init__(self):
        self.res_matcher = re.compile(r'''^(={21})''')
        self.call_matcher = re.compile(r'''^(PARSE|PARSING IN CURSOR|EXEC|FETCH|WAIT|CLOSE|BINDS|STAT) (#\d+)(:| )(.*)''')
    def process_file(self, tr, fname, sql_ids):

        logger = logging.getLogger(__name__)
        line_count = 0
        logger.info('process_file: processing %s', fname)
        with open(fname, 'r') as f:
            for line in f:
                line_count += 1
                #match = re.match(r'''^(={21}|\*\*\* )''', line)
                match = self.res_matcher.match(line)
                if match:
                    logger.debug('reset tracker')
                    tr.reset()
                match = self.call_matcher.match(line)
                if match:
                    #print(match.groups())
                    if match.group(1) == 'PARSING IN CURSOR':
                        tr.add_parsing_in(match.group(2), match.group(4))
                    if match.group(1) == 'PARSE':
                        last_parse = Ops('PARSE', match.group(2), match.group(4), fname, line_count)
                        tr.add_parse(match.group(2), last_parse)
                    if match.group(1) == 'EXEC':
                        last_exec = Ops('EXEC', match.group(2), match.group(4), fname, line_count)
                        tr.add_exec(match.group(2), last_exec)
                    if match.group(1) == 'FETCH':
                        f = Ops('FETCH', match.group(2), match.group(4), fname, line_count)
                        tr.add_fetch(match.group(2), f)
                    if match.group(1) == 'WAIT':
                        w = Ops('WAIT', match.group(2), match.group(4), fname, line_count)
                        tr.add_wait(match.group(2), w)
                    if match.group(1) == 'CLOSE':
                        c = Ops('CLOSE', match.group(2), match.group(4), fname, line_count)
                        tr.add_close(match.group(2), c)
                    if match.group(1) == 'STAT':
                        s = Ops('STAT', match.group(2), match.group(4), fname, line_count)
                        tr.add_stat(match.group(2), s)
                    if match.group(1) == 'BINDS':
                        pass
        logger.info('process_file: %s done', fname)
        return line_count

