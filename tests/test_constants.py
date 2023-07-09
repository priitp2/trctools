import datetime
from ops import Ops

__doc__ = """
Contains common constants for tests.
"""
CURSOR = '#140131077570528'
WRONG_CURSOR = '#321'
FNAME = 'trace.trc'
LINE = 77
TRACKED_OPS = {
    'PIC':Ops('PIC', CURSOR, "len=80 dep=0 uid=331 oct=3 lid=331 tim=7104844976089 hv=1167462720 " \
        + "ad='9d4125228' sqlid='6v48b7j2tc4a0'", FNAME, LINE, name='select dummy from dual'),
    'PARSE':Ops('PARSE', CURSOR,
        'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME,
        LINE),
    'EXEC':Ops('EXEC', CURSOR,
        'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME,
        LINE),
    'CLOSE':Ops('CLOSE', CURSOR, 'c=0,e=4,dep=0,type=3,tim=5793512315335', FNAME, LINE),
    'STAT':Ops('STAT', CURSOR, "id=1 cnt=1 pid=0 pos=1 obj=89434 op='TABLE ACCESS BY INDEX ROWID " \
        + "SOME_TABLE (cr=5 pr=0 pw=0 str=1 time=173 us cost=4 size=103 card=1)'", FNAME, LINE),
    'WAIT': Ops('WAIT', CURSOR, " nam='db file sequential read' ela= 403 file#=414 block#=2682927" \
        + " blocks=1 obj#=89440 tim=5793512314261", FNAME, LINE),
    'FETCH':Ops('FETCH', CURSOR, "c=0,e=45,p=0,cr=1,cu=0,mis=0,r=4,dep=0,og=1,plh=2725028981," \
        + "tim=5793511831594", FNAME, LINE)
}
UNTRACKED_OPS = {
    'STAR':Ops('STAR', None, 'jdbcthin : 21.5.0.0.0', FNAME, LINE, 'CLIENT DRIVER', datetime.datetime.now()),
    'XCTEND':Ops('XCTEND', None, 'rlbk=0, rd_only=1, tim=5793512315347', FNAME, LINE),
    'HEADER':Ops('HEADER', None, 'Build label', FNAME, LINE, 'RDBMS_19.16.0.0.0DBRUR_LINUX.X64_230111')
}
