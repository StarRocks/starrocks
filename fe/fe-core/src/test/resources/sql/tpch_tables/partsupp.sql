CREATE TABLE partsupp (
    ps_partkey      INT NOT NULL,
    ps_suppkey      INT NOT NULL,
    ps_availqty     INT NOT NULL,
    ps_supplycost   DECIMAL(15, 2) NOT NULL,
    ps_comment      VARCHAR(199) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`ps_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 24
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);
