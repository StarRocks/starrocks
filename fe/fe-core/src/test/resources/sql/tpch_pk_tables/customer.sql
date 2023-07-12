CREATE TABLE customer (
    c_custkey     BIGINT NOT NULL,
    c_name        VARCHAR(25) NOT NULL,
    c_address     VARCHAR(40) NOT NULL,
    c_nationkey   INT NOT NULL,
    c_phone       CHAR(15) NOT NULL,
    c_acctbal     DOUBLE   NOT NULL,
    c_mktsegment  CHAR(10) NOT NULL,
    c_comment     VARCHAR(117) NOT NULL
) ENGINE=OLAP
PRIMARY KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 12
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);
