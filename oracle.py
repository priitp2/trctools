import oracledb

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
