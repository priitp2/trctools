def merge_lat_objects(dest, source):
    if source == None:
        return dest
    if not isinstance(source, list):
        source = [source]
    if len(dest) < 3:
        return None
    a = dest[1]
    b = dest[2]
    for s in source:
        if dest[0] != s[0]:
            raise(BaseException("merge_lat_objects: cursor mismatch: dest cursor = {}, source cursor = {}".format(dest[0], s[0])))
        a = a + s[1]
        b = b + s[2]
    return (dest[0], a, b)
