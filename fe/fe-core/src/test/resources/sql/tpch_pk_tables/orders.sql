CREATE TABLE orders (
    o_orderkey       BIGINT NOT NULL,
    o_custkey        BIGINT NOT NULL,
    o_orderstatus    CHAR(1) NOT NULL,
    o_totalprice     DOUBLE NOT NULL,
    o_orderpriority  CHAR(15) NOT NULL,  
    o_orderdate      DATE NOT NULL,
    o_clerk          CHAR(15) NOT NULL, 
    o_shippriority   INT NOT NULL,
    o_comment        VARCHAR(79) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`o_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 48
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT",
    "unique_constraints" = "o_orderkey",
    "colocate_with" = "tpch1"
);
