CREATE TABLE lineitem (
    l_orderkey    BIGINT NOT NULL,
    l_partkey     BIGINT NOT NULL,
    l_suppkey     BIGINT not null,
    l_linenumber  INT not null,
    l_quantity    DOUBLE NOT NULL,
    l_extendedprice DOUBLE NOT NULL,
    l_discount    DOUBLE NOT NULL,
    l_tax         DOUBLE NOT NULL,
    l_returnflag  CHAR(1) NOT NULL,
    l_linestatus  CHAR(1) NOT NULL,
    l_shipdate    DATE NOT NULL,
    l_commitdate  DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct  CHAR(25) NOT NULL,
    l_shipmode      CHAR(10) NOT NULL,
    l_comment       VARCHAR(44) NOT NULL
) ENGINE=OLAP
PRIMARY KEY(`l_orderkey`, `l_partkey`, `l_suppkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 48
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT",
    "colocate_with" = "tpch1"
);
