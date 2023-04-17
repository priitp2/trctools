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

CREATE TABLE events (
    id             INTEGER
        GENERATED AS IDENTITY
    PRIMARY KEY,
    parent_id      INTEGER,
    event          VARCHAR2(12) NOT NULL,
    cursor_id      VARCHAR2(64),
    cpu_time       INTEGER NOT NULL,
    elapsed_time   INTEGER NOT NULL,
    ph_reads       INTEGER NOT NULL,
    cr_reads       INTEGER NOT NULL,
    current_reads  INTEGER NOT NULL,
    cursor_missed  INTEGER,
    rows_processed INTEGER,
    rec_call_dp    INTEGER,
    opt_goal       INTEGER,
    ts             INTEGER NOT NULL,
    c_type integer
);

