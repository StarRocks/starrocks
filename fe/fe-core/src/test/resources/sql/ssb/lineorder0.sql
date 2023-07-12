CREATE TABLE IF NOT EXISTS `lineorder0` (
    `lo_orderkey` int(11) NOT NULL COMMENT "",
    `lo_linenumber` int(11) NOT NULL COMMENT "",
    `lo_custkey` int(11) NOT NULL COMMENT "",
    `lo_partkey` int(11) NOT NULL COMMENT "",
    `lo_suppkey` int(11) NOT NULL COMMENT "",
    `lo_orderdate` int(11) NOT NULL COMMENT "",
    `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
    `lo_shippriority` int(11) NOT NULL COMMENT "",
    `lo_quantity` int(11) NOT NULL COMMENT "",
    `lo_extendedprice` int(11) NOT NULL COMMENT "",
    `lo_ordtotalprice` int(11) NOT NULL COMMENT "",
    `lo_discount` int(11) NOT NULL COMMENT "",
    `lo_revenue` int(11) NOT NULL COMMENT "",
    `lo_supplycost` int(11) NOT NULL COMMENT "",
    `lo_tax` int(11) NOT NULL COMMENT "",
    `lo_commitdate` int(11) NOT NULL COMMENT "",
    `lo_shipmode` varchar(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`lo_orderkey`)
COMMENT "OLAP"
PARTITION BY RANGE(`lo_orderdate`)
(
    PARTITION p1 VALUES [("-2147483648"), ("19930101")),
    PARTITION p2 VALUES [("19930101"), ("19940101")),
    PARTITION p3 VALUES [("19940101"), ("19950101")),
    PARTITION p4 VALUES [("19950101"), ("19960101")),
    PARTITION p5 VALUES [("19960101"), ("19970101")),
    PARTITION p6 VALUES [("19970101"), ("19980101")),
    PARTITION p7 VALUES [("19980101"), ("19990101")))
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 48
PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "groupa1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
)
