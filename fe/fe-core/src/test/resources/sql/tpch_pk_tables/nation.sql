CREATE TABLE `nation` (
    n_nationkey INT(11) NOT NULL,
    n_name      VARCHAR(25) NOT NULL,
    n_regionkey INT(11) NOT NULL,
    n_comment   VARCHAR(152) NULL
) ENGINE=OLAP
PRIMARY KEY(`n_nationkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`n_nationkey`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);
