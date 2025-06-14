CREATE TABLE supplier (
    s_suppkey     BIGINT NOT NULL,
    s_name        VARCHAR(25) NOT NULL,
    s_address     VARCHAR(40) NOT NULL,
    s_nationkey   INT NOT NULL,
    s_phone       VARCHAR(15) NOT NULL,
    s_acctbal     DECIMAL(15, 2) NOT NULL,
    s_comment     VARCHAR(101) NOT NULL
) ENGINE=OLAP
PRIMARY KEY(`s_suppkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);
