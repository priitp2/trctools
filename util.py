import re
from ops import Ops

def process_file(tr, fname, sql_ids):

    line_count = 0
    with open(fname, 'r') as f:
        for line in f:
            line_count += 1
            #match = re.match(r'''^(={21}|\*\*\* )''', line)
            match = re.match(r'''^(={21})''', line)
            if match:
                tr.reset()
            match = re.match(r'''^(PARSE|PARSING IN CURSOR|EXEC|FETCH|WAIT|CLOSE|BINDS|STAT) (#\d+)(:| )(.*)''', line)
            if match:
                #print(match.groups())
                if match.group(1) == 'PARSING IN CURSOR':
                    tr.add_parsing_in(match.group(2), match.group(4))
                if match.group(1) == 'PARSE':
                    last_parse = Ops('PARSE', match.group(2), match.group(4), fname, line_count)
                    tr.add_parse(match.group(2), last_parse)
                    #if cs:
                    #    if tr.cursors[cs.cursor] in sql_ids or len(sql_ids) == 0:
                    #        print_naughty_exec(tr, cs, fname, line_count, 'PARSE')
                if match.group(1) == 'EXEC':
                    last_exec = Ops('EXEC', match.group(2), match.group(4), fname, line_count)
                    tr.add_exec(match.group(2), last_exec)
                    #if cs:
                    #    if tr.cursors[cs.cursor] in sql_ids or len(sql_ids) == 0:
                    #        print_naughty_exec(tr, cs, fname, line_count, 'EXEC')
                if match.group(1) == 'FETCH':
                    # FIXME: fetches should be added to execs, not other way around
                    f = Ops('FETCH', match.group(2), match.group(4), fname, line_count)
                    tr.add_fetch(match.group(2), f)
                if match.group(1) == 'WAIT':
                    w = Ops('WAIT', match.group(2), match.group(4), fname, line_count)
                    tr.add_wait(match.group(2), w)
                if match.group(1) == 'CLOSE':
                    c = Ops('CLOSE', match.group(2), match.group(4), fname, line_count)
                    tr.add_close(match.group(2), c)
                    #if cs:
                    #    if tr.cursors[cs.cursor] in sql_ids or len(sql_ids) == 0:
                    #        print_naughty_exec(tr, cs, fname, line_count, 'CLOSE')

                if match.group(1) == 'STAT':
                    s = Ops('STAT', match.group(2), match.group(4), fname, line_count)
                    tr.add_stat(match.group(2), s)
                if match.group(1) == 'BINDS':
                    pass

