 CREATE TABLE IF NOT EXISTS `supplier` (
    `s_suppkey` int(11) NOT NULL COMMENT "",
    `s_name` varchar(26) NOT NULL COMMENT "",
    `s_address` varchar(26) NOT NULL COMMENT "",
    `s_city` varchar(11) NOT NULL COMMENT "",
    `s_nation` varchar(16) NOT NULL COMMENT "",
    `s_region` varchar(13) NOT NULL COMMENT "",
    `s_phone` varchar(16) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`s_suppkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12
PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "groupa4",
    "in_memory" = "false",
    "unique_constraints" = "s_suppkey",
    "storage_format" = "DEFAULT"
)