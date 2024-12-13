CREATE TABLE customer (
    c_custkey       INT NOT NULL,
    c_name          VARCHAR(25) NOT NULL,
    c_address       VARCHAR(40) NOT NULL,
    c_nationkey     INT NOT NULL,
    c_phone         VARCHAR(15) NOT NULL,
    c_acctbal       DECIMAL(15, 2) NOT NULL,
    c_mktsegment    VARCHAR(10) NOT NULL,
    c_comment       VARCHAR(117) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 24
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);
