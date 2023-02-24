CREATE TABLE IF NOT EXISTS `part` (
    `p_partkey` int(11) NOT NULL COMMENT "",
    `p_name` varchar(23) NOT NULL COMMENT "",
    `p_mfgr` varchar(7) NOT NULL COMMENT "",
    `p_category` varchar(8) NOT NULL COMMENT "",
    `p_brand` varchar(10) NOT NULL COMMENT "",
    `p_color` varchar(12) NOT NULL COMMENT "",
    `p_type` varchar(26) NOT NULL COMMENT "",
    `p_size` int(11) NOT NULL COMMENT "",
    `p_container` varchar(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`p_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 12
PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "groupa5",
    "in_memory" = "false",
    "unique_constraints" = "p_partkey",
    "storage_format" = "DEFAULT"
)