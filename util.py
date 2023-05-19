import re
from ops import Ops

def print_naughty_exec(tracker, cs, fname, line, trigger):
    lat = cs.merge()
    if lat.e > 1000000:
        print('----------------------------------------------')
        if lat.cursor not in tracker.cursors.keys():
            print("print_naughty_exec: missing cursor {}".format(lat.cursor))
            return
        statement = tracker.statements[tracker.cursors[lat.cursor]]
        print("sql_id = {}, cursor = {}, elapsed = {}, fetches = {}, file = {}, line = {}, triggered by {}".format(statement.sql_id, lat.cursor, lat.e, cs.fetch_count, fname, line, trigger))
        if cs.parse:
            print("    {}".format(cs.parse))
        if cs.exec:
            print("    {}".format(cs.exec))
        for i in range(0, min(10, len(cs.fetches))):
            print("     {}".format(cs.fetches[i]))
        if len(cs.fetches) > 10:
            print("     <Rest of the {} fetches>".format(len(cs.fetches)))
        for i in range(0, min(10, len(cs.waits))):
            print("     {}".format(cs.waits[i]))
        if len(cs.waits) > 10:
            print("     <Rest of the {} waits>".format(len(cs.waits)))
        if cs.close:
            print("    {}".format(cs.close))
        elapsed = cs.get_elapsed()
        if elapsed != None:
            print("    estimated elapsed time = {}".format(elapsed))

        print('----------------------------------------------')


def process_file(tr, fname):

    line_count = 0
    with open(fname, 'r') as f:
        for line in f:
            line_count += 1
            match = re.match(r'''^(PARSE|PARSING IN CURSOR|EXEC|FETCH|WAIT|CLOSE|BINDS) (#\d+)(:| )(.*)''', line)
            if match:
                #print(match.groups())
                if match.group(1) == 'PARSING IN CURSOR':
                    tr.add_parsing_in(match.group(2), match.group(4))
                if match.group(1) == 'PARSE':
                    last_parse = Ops('PARSE', match.group(2), match.group(4))
                    cs = tr.add_parse(match.group(2), last_parse)
                    if cs:
                        print_naughty_exec(tr, cs, fname, line_count, 'PARSE')
                if match.group(1) == 'EXEC':
                    last_exec = Ops('EXEC', match.group(2), match.group(4))
                    cs = tr.add_exec(match.group(2), last_exec)
                    if cs:
                        print_naughty_exec(tr, cs, fname, line_count, 'EXEC')
                if match.group(1) == 'FETCH':
                    # FIXME: fetches should be added to execs, not other way around
                    f = Ops('FETCH', match.group(2), match.group(4))
                    tr.add_fetch(match.group(2), f)
                if match.group(1) == 'WAIT':
                    w = Ops('WAIT', match.group(2), match.group(4))
                    tr.add_wait(match.group(2), w)
                if match.group(1) == 'CLOSE':
                    c = Ops('CLOSE', match.group(2), match.group(4))
                    cs = tr.add_close(match.group(2), c)
                    if cs:
                        print_naughty_exec(tr, cs, fname, line_count, 'CLOSE')

                if match.group(1) == 'BINDS':
                    pass

