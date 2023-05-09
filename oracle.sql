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

create sequence cursor_exec_id;

CREATE TABLE cursor_exec (
    id             INTEGER,
    cursor_id      VARCHAR2(64) NOT NULL,
    ops            VARCHAR2(12) NOT NULL,
    cpu_time       INTEGER NOT NULL, -- c
    elapsed_time   INTEGER NOT NULL, -- e
    ph_reads       INTEGER, -- p
    cr_reads       INTEGER, -- cr
    current_reads  INTEGER, -- cu
    cursor_missed  INTEGER,          -- mis
    rows_processed INTEGER,          -- r
    rec_call_dp    INTEGER,          -- dep
    opt_goal       INTEGER,          -- og
    ts             INTEGER NOT NULL, -- tim
    c_type integer
);

