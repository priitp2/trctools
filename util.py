import re

def merge_lat_objects(dest, source):
    if not source:
        return dest
    if not dest:
        dest = (None, 0, 0)
    if not isinstance(source, list):
        source = [source]
    if len(dest) < 3:
        return None
    a = dest[1]
    b = dest[2]
    for s in source:
        if len(s) < 3:
            continue
        if dest[0] != s[0]:
            raise(BaseException("merge_lat_objects: cursor mismatch: dest cursor = {}, source cursor = {}".format(dest[0], s[0])))
        a = a + s[1]
        b = b + s[2]
    return (dest[0], a, b)

def split_event(ev):
    out = {}
    for item in ev.split(','):
        key = item.split('=')
        if len(key) == 2:
            out[key[0]] = key[1]
    return out
def ops2tuple(exec_id, cursor, ops_type, params):
    if ops_type == 'CLOSE':
        return [exec_id, cursor, ops_type, params['c'], params['e'], params['dep'], params['type'], params['tim']]
    else:
        return [exec_id, cursor, ops_type, params['c'], params['e'], params['cr'], params['cu'],params['mis'], params['r'], params['dep'], params['og'], params['plh'], params['tim']]

def handle_parse(cursor, params):
    ev = split_event(params)
    return (cursor, int(ev['c']), int(ev['e']), 0, ev)

def handle_exec(cursor, params):
    ev = split_event(params)
    return (cursor, int(ev['c']), int(ev['e']), 0, ev)
#    print(statement)
#    print("handle_exec1: cursor = {}, params = {}, sql_id = {}".format(cursor, params, cursors[cursor]))

def handle_fetch(cursor, params):
    ev = split_event(params)

    lat = (cursor, int(ev['c']), int(ev['e']), ev)
    return lat

def handle_wait(cursor, params):
    #match = re.match(r""" nam=([:alnum:]+) ela = (\d+) (.*) tim=(\d+)""", params)
    wait = {}
    match = re.match(r""" nam='(.*)' ela= (\d+) (.*) tim=(\d+)""", params)
    if match:
        wait['name'] = match.group(1)
        wait['elapsed'] = match.group(2)
        wait['timestamp'] = match.group(4)
        return (cursor, 0, int(match.group(2)), wait)
    else:
        print("handle_wait: no match: cursor={}, params = ->{}<-".format(cursor, params))
        return None

def handle_close(cursor, params):
    ev = split_event(params)
    return (cursor, int(ev['c']), int(ev['e']), ev)

def print_naughty_exec(tracker, cs):
    lat = cs.merge()
    if lat[2] > 1000000:
        print('----------------------------------------------')
        if lat[0] not in tracker.cursors.keys():
            print("print_naughty_exec: missing cursor {}".format(lat[0]))
            return
        statement = tracker.statements[tracker.cursors[lat[0]]]
        print("sql_id = {}, cursor = {}, elapsed = {}, fetches = {}".format(statement.sql_id, lat[0], lat[2], cs.fetch_count))
        if cs.exec:
            print("    exec: cpu = {}, elapsed = {}, timestamp = {}".format(cs.exec[1], cs.exec[2], cs.exec[4]['tim']))
        if cs.fetch_count < cs.max_list_size:
            for f in cs.fetches:
                print("     {}".format(f))
        else:
            elapsed = merge_lat_objects((cs.cursor, 0, 0), cs.fetches)
            print("    fetches = {}, elapsed = {}".format(cs.fetch_count, elapsed[2]))
        if cs.wait_count < cs.max_list_size:
            for w in cs.waits:
                print("     {}".format(w[3]))
        else:
            elapsed = merge_lat_objects((cs.cursor, 0, 0), cs.waits)
            print("    waits = {}, elapsed = {}".format(cs.wait_count, elapsed[2]))
        elapsed = cs.get_elapsed()
        if elapsed != None:
            print("    estimated elapsed time = {}".format(elapsed))

        print('----------------------------------------------')


def process_file(tr, fname):

    print("Processing {}".format(fname))
    with open(fname, 'r') as f:
        for line in f:
            match = re.match(r'''^(PARSE|PARSING IN CURSOR|EXEC|FETCH|WAIT|CLOSE|BINDS) (#\d+)(:| )(.*)''', line)
            if match:
                #print(match.groups())
                if match.group(1) == 'PARSING IN CURSOR':
                    tr.add_parsing_in(match.group(2), match.group(4))
                if match.group(1) == 'PARSE':
                    last_parse = handle_parse(match.group(2), match.group(4))
                    cs = tr.add_parse(match.group(2), last_parse)
                    if cs:
                        print_naughty_exec(tr, cs)
                if match.group(1) == 'EXEC':
                    last_exec = handle_exec(match.group(2), match.group(4))
                    cs = tr.add_exec(match.group(2), last_exec)
                    if cs:
                        print_naughty_exec(tr, cs)
                if match.group(1) == 'FETCH':
                    # FIXME: fetches should be added to execs, not other way around
                    f = handle_fetch(match.group(2), match.group(4))
                    tr.add_fetch(match.group(2), f)
                if match.group(1) == 'WAIT':
                    w = handle_wait(match.group(2), match.group(4))
                    tr.add_wait(match.group(2), w)
                if match.group(1) == 'CLOSE':
                    c = handle_close(match.group(2), match.group(4))
                    cs = tr.add_close(match.group(2), c)
                    if cs:
                        print_naughty_exec(tr, cs)

                if match.group(1) == 'BINDS':
                    pass

