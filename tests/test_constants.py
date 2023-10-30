import collections
import datetime
import ops

__doc__ = """
Contains common constants for tests.
"""
CURSOR = '#140131077570528'
WRONG_CURSOR = '#321'
TS_CALLBACK = lambda x : datetime.datetime.today()
FMETA = collections.defaultdict(lambda: None)
FMETA['FILE_NAME'] = 'trace.trc'
FMETA['LINE_COUNT'] = 77
TRACKED_OPS = {
    'PIC':ops.ops_factory('PIC', CURSOR, "len=80 dep=0 uid=331 oct=3 lid=331 tim=7104844976089 hv=1167462720 " \
        + "ad='9d4125228' sqlid='6v48b7j2tc4a0'", FMETA, TS_CALLBACK, name='select dummy from dual'),
    'PARSE':ops.ops_factory('PARSE', CURSOR,
        'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FMETA,
        TS_CALLBACK),
    'EXEC':ops.ops_factory('EXEC', CURSOR,
        'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FMETA,
        TS_CALLBACK),
    'CLOSE':ops.ops_factory('CLOSE', CURSOR, 'c=0,e=4,dep=0,type=3,tim=5793512315335', FMETA, TS_CALLBACK),
    'STAT':ops.ops_factory('STAT', CURSOR, "id=1 cnt=1 pid=0 pos=1 obj=89434 op='TABLE ACCESS BY INDEX ROWID " \
        + "SOME_TABLE (cr=5 pr=0 pw=0 str=1 time=173 us cost=4 size=103 card=1)'", FMETA, TS_CALLBACK),
    'WAIT': ops.ops_factory('WAIT', CURSOR, " nam='db file sequential read' ela= 403 file#=414 block#=2682927" \
        + " blocks=1 obj#=89440 tim=5793512314261", FMETA, TS_CALLBACK),
    'FETCH':ops.ops_factory('FETCH', CURSOR, "c=0,e=45,p=0,cr=1,cu=0,mis=0,r=4,dep=0,og=1,plh=2725028981," \
        + "tim=5793511831594", FMETA, TS_CALLBACK)
}
UNTRACKED_OPS = {
        'STAR':ops.ops_factory('STAR', None, 'jdbcthin : 21.5.0.0.0', FMETA, None, 'CLIENT DRIVER', datetime.datetime.today()),
    'XCTEND':ops.ops_factory('XCTEND', None, 'rlbk=0, rd_only=1, tim=5793512315347', FMETA, TS_CALLBACK),
    'HEADER':ops.ops_factory('HEADER', None, 'Build label', FMETA, 'RDBMS_19.16.0.0.0DBRUR_LINUX.X64_230111'),
    'LOB':ops.ops_factory('LOBWRITE', None, "type=TEMPORARY LOB,bytes=5,c=169,e=169,p=0,cr=0,cu=7,tim=4696599871319", FMETA,
        TS_CALLBACK)
}
