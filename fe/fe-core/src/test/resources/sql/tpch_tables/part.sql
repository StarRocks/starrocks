CREATE TABLE part (
    p_partkey       INT NOT NULL,
    p_name          VARCHAR(55) NOT NULL,
    p_mfgr          VARCHAR(25) NOT NULL,
    p_brand         VARCHAR(10) NOT NULL,
    p_type          VARCHAR(25) NOT NULL,
    p_size          INT NOT NULL,
    p_container     VARCHAR(10) NOT NULL,
    p_retailprice   DECIMAL(15, 2) NOT NULL,
    p_comment       VARCHAR(23) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`p_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 24
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);
