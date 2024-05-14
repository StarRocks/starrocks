CREATE TABLE lineitem (
    l_shipdate      DATE NOT NULL,
    l_orderkey      INT NOT NULL,
    l_linenumber    INT not null,
    l_partkey       INT NOT NULL,
    l_suppkey       INT not null,
    l_quantity      DECIMAL(15, 2) NOT NULL,
    l_extendedprice DECIMAL(15, 2) NOT NULL,
    l_discount      DECIMAL(15, 2) NOT NULL,
    l_tax           DECIMAL(15, 2) NOT NULL,
    l_returnflag    VARCHAR(1) NOT NULL,
    l_linestatus    VARCHAR(1) NOT NULL,
    l_commitdate    DATE NOT NULL,
    l_receiptdate   DATE NOT NULL,
    l_shipinstruct  VARCHAR(25) NOT NULL,
    l_shipmode      VARCHAR(10) NOT NULL,
    l_comment       VARCHAR(44) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`l_shipdate`, `l_orderkey`)
COMMENT "OLAP"
PARTITION BY RANGE(`l_shipdate`)
(
   START ("1992-01-01") END ("1999-01-01") EVERY (INTERVAL 1 month)
)
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 64
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);
