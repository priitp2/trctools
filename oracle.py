import oracledb
import sys

class DB:
    def __init__(self):
        self.connection = oracledb.connect(
            user="test0",
            password='test123',
            dsn="localhost/xepdb1")

#        cursor = self.connection.cursor()
#        cursor.execute("create table if not exists rtime (id integer generated as identity primary key, sql_id varchar2(16), rt integer not null) organization index")

    def add_rows(self, sql_id, rt):
        cursor = self.connection.cursor()
        for r in rt:
            cursor.execute("insert into rtime (sql_id, rt) values(:1, :2)", [sql_id, r])

        self.connection.commit()
    def add_cursor(self, c):
        cursor = self.connection.cursor()
        cursor.execute("insert into cursors(cursor_id, statement_length, rec_depth, schema_id, command_type, priv_user_id, ts, hash_id, sqltext_addr, sql_id) values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)", [c.cursor, c.statement_length, c.rec_depth, c.schema_uid, c.command_type, c.priv_user_id, c.timestamp, c.hash_id, c.address, c.sql_id])
        self.connection.commit()
    def add_event(self, ev):
        cursor = self.connection.cursor()
        out = cursor.var(int)
        try:
            cursor.execute("insert into events(parent_id, event, cursor_id, cpu_time, elapsed_time, ph_reads, cr_reads, current_reads, cursor_missed, rows_processed, rec_call_dp, opt_goal, ts, c_type) values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14) returning id into :15", [ev['parent_id'], ev['event'], ev['cursor'], ev['c'], ev['e'], ev['p'], ev['cr'], ev['cu'], ev['mis'], ev['r'], ev['dep'], ev['og'], ev['tim'], ev['type'], out])
        except:
            print(ev)
            raise sys.exception()
        self.connection.commit()
        return out.getvalue()

    def get_exec_id(self):
        cursor = self.connection.cursor()
        out = cursor.execute('select cursor_exec_id.nextval from dual')
        return out.fetchone()
    def insert_ops(self, ops, op_close):
        if len(ops) == 0:
            return
        cursor = self.connection.cursor()
        cursor.executemany('insert into cursor_exec(id, cursor_id, ops, cpu_time, elapsed_time, ph_reads, cr_reads, current_reads, cursor_missed, rows_processed, rec_call_dp, opt_goal, ts) values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)', ops)
        if op_close:
            cursor.execute('insert into cursor_exec(id, cursor_id, ops, cpu_time, elapsed_time, rec_call_dp, c_type, ts) values(:1, :2, :3, :4, :5, :6, :7, :8)', op_close)
        self.connection.commit()

