CREATE TABLE partsupp (
    ps_partkey     BIGINT NOT NULL,
    ps_suppkey     BIGINT NOT NULL,
    ps_availqty    INT NOT NULL,
    ps_supplycost  DOUBLE  NOT NULL,
    ps_comment     VARCHAR(199) NOT NULL
) ENGINE=OLAP
PRIMARY KEY(`ps_partkey`, `ps_suppkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`ps_partkey`, `ps_suppkey`) BUCKETS 12
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);
