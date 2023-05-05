CREATE TABLE IF NOT EXISTS `customer` (
    `c_custkey` int(11) NOT NULL COMMENT "",
    `c_name` varchar(26) NOT NULL COMMENT "",
    `c_address` varchar(41) NOT NULL COMMENT "",
    `c_city` varchar(11) NOT NULL COMMENT "",
    `c_nation` varchar(16) NOT NULL COMMENT "",
    `c_region` varchar(13) NOT NULL COMMENT "",
    `c_phone` varchar(16) NOT NULL COMMENT "",
    `c_mktsegment` varchar(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 12
PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "groupa2",
    "in_memory" = "false",
    "unique_constraints" = "c_custkey",
    "storage_format" = "DEFAULT"
)
