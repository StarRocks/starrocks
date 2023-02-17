create table if not exists t_r_table (
    file       STRING,
    log_type  STRING,
    name      STRING,
    version   STRING,
    log       STRING,
    sequence    INT,
    create_time DATETIME default current_timestamp
)
DISTRIBUTED BY HASH(file) BUCkETS 3
PROPERTIES (
    "replication_num" = "1"
);


