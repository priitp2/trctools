from ops import Ops

"""
Contains common constants for tests.
"""
CURSOR = '#123'
WRONG_CURSOR = '#321'
FNAME = 'trace.trc'
CORRECT_OPS = {
    'PARSE':Ops('PARSE', CURSOR,
        'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME, 0),
    'EXEC':Ops('EXEC', CURSOR,
        'c=73,e=73,p=1,cr=2,cu=3,mis=4,r=5,dep=6,og=7,plh=2725028981,tim=5793511830834', FNAME, 2),
    'CLOSE':Ops('CLOSE', CURSOR, 'c=0,e=4,dep=0,type=3,tim=5793512315335', FNAME, 8),
    'STAT':Ops('STAT', CURSOR, "id=1 cnt=1 pid=0 pos=1 obj=89434 op='TABLE ACCESS BY INDEX ROWID " \
        + "SOME_TABLE (cr=5 pr=0 pw=0 str=1 time=173 us cost=4 size=103 card=1)'", FNAME, 8),
    'WAIT': Ops('WAIT', CURSOR, " nam='db file sequential read' ela= 403 file#=414 block#=2682927 " \
        + "blocks=1 obj#=89440 tim=5793512314261", FNAME, 3)
}

