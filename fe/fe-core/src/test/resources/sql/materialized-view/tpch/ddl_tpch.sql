DROP TABLE IF EXISTS region;
CREATE TABLE region (
                        r_regionkey  INT NOT NULL,
                        r_name       VARCHAR(25) NOT NULL,
                        r_comment    VARCHAR(152)
) ENGINE=OLAP
DUPLICATE KEY(`r_regionkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "unique_constraints" = "r_regionkey",
    "storage_format" = "DEFAULT"
);

DROP TABLE IF EXISTS part;
CREATE TABLE part (
                      p_partkey     INT NOT NULL,
                      p_name        VARCHAR(55) NOT NULL,
                      p_mfgr        VARCHAR(25) NOT NULL,
                      p_brand       VARCHAR(10) NOT NULL,
                      p_type        VARCHAR(25) NOT NULL,
                      p_size        INT NOT NULL,
                      p_container   VARCHAR(10) NOT NULL,
                      p_retailprice DECIMAL(15, 2) NOT NULL,
                      p_comment     VARCHAR(23) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`p_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 24
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "unique_constraints" = "p_partkey",
    "storage_format" = "DEFAULT"
);

DROP TABLE IF EXISTS nation;
CREATE TABLE `nation` (
                          n_nationkey INT(11) NOT NULL,
                          n_name      VARCHAR(25) NOT NULL,
                          n_regionkey INT(11) NOT NULL,
                          n_comment   VARCHAR(152) NULL
) ENGINE=OLAP
DUPLICATE KEY(`N_NATIONKEY`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "unique_constraints" = "n_nationkey",
    "foreign_key_constraints" = "(n_regionkey) references region(r_regionkey)",
    "storage_format" = "DEFAULT"
);

DROP TABLE IF EXISTS customer;
CREATE TABLE customer (
                          c_custkey     INT NOT NULL,
                          c_name        VARCHAR(25) NOT NULL,
                          c_address     VARCHAR(40) NOT NULL,
                          c_nationkey   INT NOT NULL,
                          c_phone       VARCHAR(15) NOT NULL,
                          c_acctbal     DECIMAL(15, 2) NOT NULL,
                          c_mktsegment  VARCHAR(10) NOT NULL,
                          c_comment     VARCHAR(117) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 24
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "unique_constraints" = "c_custkey",
    "foreign_key_constraints" = "(c_nationkey) references nation(n_nationkey)",
    "storage_format" = "DEFAULT"
);


DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
                        o_orderkey       BIGINT NOT NULL,
                        o_orderdate      DATE NOT NULL,
                        o_custkey        INT NOT NULL,
                        o_orderstatus    VARCHAR(1) NOT NULL,
                        o_totalprice     DECIMAL(15, 2) NOT NULL,
                        o_orderpriority  VARCHAR(15) NOT NULL,
                        o_clerk          VARCHAR(15) NOT NULL,
                        o_shippriority   INT NOT NULL,
                        o_comment        VARCHAR(79) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`o_orderkey`, `o_orderdate`)
COMMENT "OLAP"
PARTITION BY RANGE(`o_orderdate`)
(
   START ("1992-01-01") END ("1999-01-01") EVERY (INTERVAL 1 year)
)
DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "unique_constraints" = "o_orderkey",
    "foreign_key_constraints" = "(o_custkey) references customer(c_custkey)",
    "storage_format" = "DEFAULT"
);

DROP TABLE IF EXISTS supplier;
CREATE TABLE supplier (
                          s_suppkey     INT NOT NULL,
                          s_name        VARCHAR(25) NOT NULL,
                          s_address     VARCHAR(40) NOT NULL,
                          s_nationkey   INT NOT NULL,
                          s_phone       VARCHAR(15) NOT NULL,
                          s_acctbal     DECIMAL(15, 2) NOT NULL,
                          s_comment     VARCHAR(101) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`s_suppkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "unique_constraints" = "s_suppkey",
    "foreign_key_constraints" = "(s_nationkey) references nation(n_nationkey)",
    "storage_format" = "DEFAULT"
);

DROP TABLE IF EXISTS partsupp;
CREATE TABLE partsupp (
                          ps_partkey     INT NOT NULL,
                          ps_suppkey     INT NOT NULL,
                          ps_availqty    INT NOT NULL,
                          ps_supplycost  DECIMAL(15, 2) NOT NULL,
                          ps_comment     VARCHAR(199) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`ps_partkey`, `ps_suppkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 24
PROPERTIES (
    "replication_num" = "1",
    "unique_constraints" = "ps_partkey,ps_suppkey",
    "foreign_key_constraints" = "(ps_partkey) references part(p_partkey);(ps_suppkey) references supplier(s_suppkey)",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);


DROP TABLE IF EXISTS lineitem;
CREATE TABLE lineitem (
                          l_shipdate    DATE NOT NULL,
                          l_orderkey    BIGINT NOT NULL,
                          l_linenumber  INT not null,
                          l_partkey     INT NOT NULL,
                          l_suppkey     INT not null,
                          l_quantity    DECIMAL(15, 2) NOT NULL,
                          l_extendedprice  DECIMAL(15, 2) NOT NULL,
                          l_discount    DECIMAL(15, 2) NOT NULL,
                          l_tax         DECIMAL(15, 2) NOT NULL,
                          l_returnflag  VARCHAR(1) NOT NULL,
                          l_linestatus  VARCHAR(1) NOT NULL,
                          l_commitdate  DATE NOT NULL,
                          l_receiptdate DATE NOT NULL,
                          l_shipinstruct VARCHAR(25) NOT NULL,
                          l_shipmode     VARCHAR(10) NOT NULL,
                          l_comment      VARCHAR(44) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`l_shipdate`, `l_orderkey`, `l_linenumber`)
COMMENT "OLAP"
PARTITION BY RANGE(`l_shipdate`)
(
   START ("1992-01-01") END ("1999-01-01") EVERY (INTERVAL 1 year)
)
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "unique_constraints" = "l_orderkey;l_partkey;l_suppkey",
    "foreign_key_constraints" = "(l_orderkey) references orders(o_orderkey);(l_partkey,l_suppkey) references partsupp(ps_partkey, ps_suppkey)",
    "storage_format" = "default"
);