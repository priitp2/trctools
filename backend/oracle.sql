-- increment by should match DB.seq_batch_size
create sequence cursor_exec_id increment by 100;

CREATE TABLE dbcall (
    exec_id        INTEGER NOT NULL,
    sql_id         VARCHAR2(16),
    cursor_id      VARCHAR2(64),
    ops            VARCHAR2(12) NOT NULL,
    cpu_time       INTEGER, -- c
    elapsed_time   INTEGER, -- e
    ph_reads       INTEGER, -- p
    cr_reads       INTEGER, -- cr
    current_reads  INTEGER, -- cu
    cursor_missed  INTEGER,          -- mis
    rows_processed INTEGER,          -- r
    rec_call_dp    INTEGER,          -- dep
    opt_goal       INTEGER,          -- og
    plh	           INTEGER, 
    tim            INTEGER, -- tim
    c_type 	   INTEGER,
    wait_name	   VARCHAR2(256),
    wait_raw	   VARCHAR2(4000),
    file_name	   VARCHAR2(1000) NOT NULL,
    line	   INTEGER NOT NULL,
    ts		   TIMESTAMP,
    len		   INTEGER,
    pic_uid	   INTEGER,
    oct		   INTEGER,
    lid		   INTEGER,
    hv		   INTEGER,
    ad		   VARCHAR2(64),
    rlbk	   INTEGER,
    rd_only	   INTEGER,
    lobtype	   VARCHAR2(32),
    bytes	   INTEGER,
    sid		   VARCHAR2(128),
    client_id	   VARCHAR2(128),
    service_name   VARCHAR2(128),
    module	   VARCHAR2(128),
    action	   VARCHAR2(128),
    container_id   INTEGER,
    error_code	   INTEGER
) PCTFREE 0 ROW STORE COMPRESS ADVANCED;

create view elapsed_time as
    select
        sql_id,
	exec_id,
	max(ts) keep (dense_rank last order by ts) over (partition by sql_id, exec_id) - min(ts) keep (dense_rank first order by ts) over (partition by sql_id, exec_id) as ela
    from
	dbcall
    where
	ts is not null
    group by sql_id, exec_id;

