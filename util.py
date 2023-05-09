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
