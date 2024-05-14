CREATE TABLE `nation` (
    n_nationkey   INT(11) NOT NULL,
    n_name        VARCHAR(25) NOT NULL,
    n_regionkey   INT(11) NOT NULL,
    n_comment     VARCHAR(152) NULL
) ENGINE=OLAP
DUPLICATE KEY(`N_NATIONKEY`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);
