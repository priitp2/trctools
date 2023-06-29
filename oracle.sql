CREATE TABLE cursors (
    id integer generated as identity primary key,
    cursor_id        VARCHAR2(64) not null,
    statement_length INTEGER NOT NULL,
    rec_depth        INTEGER NOT NULL,
    schema_id        INTEGER NOT NULL,
    command_type     INTEGER NOT NULL,
    priv_user_id     INTEGER NOT NULL,
    ts               INTEGER NOT NULL,
    hash_id          INTEGER NOT NULL,
    sqltext_addr     VARCHAR2(128) NOT NULL,
    sql_id           VARCHAR2(64) NOT NULL
);

-- increment by should match DB.seq_batch_size
create sequence cursor_exec_id increment by 100;

CREATE TABLE dbcall (
    exec_id        INTEGER NOT NULL,
    sql_id         VARCHAR2(16) NOT NULL,
    cursor_id      VARCHAR2(64) NOT NULL,
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
    ts             INTEGER, -- tim
    c_type 	   INTEGER,
    wait_name	   VARCHAR2(256),
    wait_raw	   VARCHAR2(4000),
    file_name	   VARCHAR2(1000),
    line	   INTEGER
) PCTFREE 0 ROW STORE COMPRESS ADVANCED;

