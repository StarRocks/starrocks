CREATE TABLE IF NOT EXISTS `lineitem` (
  `l_orderkey` int(11) NOT NULL COMMENT "",
  `l_partkey` int(11) NOT NULL COMMENT "",
  `l_suppkey` int(11) NOT NULL COMMENT "",
  `l_linenumber` int(11) NOT NULL COMMENT "",
  `l_quantity` decimal(15, 2) NOT NULL COMMENT "",
  `l_extendedprice` decimal(15, 2) NOT NULL COMMENT "",
  `l_discount` decimal(15, 2) NOT NULL COMMENT "",
  `l_tax` decimal(15, 2) NOT NULL COMMENT "",
  `l_returnflag` char(1) NOT NULL COMMENT "",
  `l_linestatus` char(1) NOT NULL COMMENT "",
  `l_shipdate` date NOT NULL COMMENT "",
  `l_commitdate` date NOT NULL COMMENT "",
  `l_receiptdate` date NOT NULL COMMENT "",
  `l_shipinstruct` char(25) NOT NULL COMMENT "",
  `l_shipmode` char(10) NOT NULL COMMENT "",
  `l_comment` varchar(44) NOT NULL COMMENT ""
) 
DUPLICATE KEY(`l_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 192
PROPERTIES (
"replication_num" = "3",
"colocate_with" = "group_tpch_100",
"in_memory" = "false",
"storage_format" = "V2"
);

CREATE TABLE `customer` (
  `c_custkey` int(11) NOT NULL COMMENT "",
  `c_name` varchar(25) NOT NULL COMMENT "",
  `c_address` varchar(40) NOT NULL COMMENT "",
  `c_nationkey` int(11) NOT NULL COMMENT "",
  `c_phone` char(15) NOT NULL COMMENT "",
  `c_acctbal` decimal(15, 2) NOT NULL COMMENT "",
  `c_mktsegment` char(10) NOT NULL COMMENT "",
  `c_comment` varchar(117) NOT NULL COMMENT ""
) 
DUPLICATE KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 12
PROPERTIES (
"replication_num" = "3",
"colocate_with" = "groupxcdffsisdf",
"in_memory" = "false",
"storage_format" = "V2"
);

CREATE TABLE IF NOT EXISTS `orders` (
  `o_orderkey` int(11) NOT NULL COMMENT "",
  `o_custkey` int(11) NOT NULL COMMENT "",
  `o_orderstatus` char(1) NOT NULL COMMENT "",
  `o_totalprice` decimal(15, 2) NOT NULL COMMENT "",
  `o_orderdate` date NOT NULL COMMENT "",
  `o_orderpriority` char(15) NOT NULL COMMENT "",
  `o_clerk` char(15) NOT NULL COMMENT "",
  `o_shippriority` int(11) NOT NULL COMMENT "",
  `o_comment` varchar(79) NOT NULL COMMENT ""
) 
DUPLICATE KEY(`o_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 192
PROPERTIES (
"replication_num" = "3",
"colocate_with" = "gsdaf2449s9e",
"in_memory" = "false",
"storage_format" = "V2"
);

CREATE TABLE IF NOT EXISTS `nation` (
  `n_nationkey` int(11) NOT NULL COMMENT "",
  `n_name` char(25) NOT NULL COMMENT "",
  `n_regionkey` int(11) NOT NULL COMMENT "",
  `n_comment` varchar(152) NULL COMMENT ""
) 
DUPLICATE KEY(`n_nationkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`n_nationkey`) BUCKETS 10
PROPERTIES (
"replication_num" = "3",
"in_memory" = "false",
"storage_format" = "V2"
);

CREATE TABLE IF NOT EXISTS `region` (
  `r_regionkey` int(11) NOT NULL COMMENT "",
  `r_name` char(25) NOT NULL COMMENT "",
  `r_comment` varchar(152) NULL COMMENT ""
) 
DUPLICATE KEY(`r_regionkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 10
PROPERTIES (
"replication_num" = "3",
"in_memory" = "false",
"storage_format" = "V2"
);

CREATE TABLE IF NOT EXISTS `supplier` (
  `s_suppkey` int(11) NOT NULL COMMENT "",
  `s_name` char(25) NOT NULL COMMENT "",
  `s_address` varchar(40) NOT NULL COMMENT "",
  `s_nationkey` int(11) NOT NULL COMMENT "",
  `s_phone` char(15) NOT NULL COMMENT "",
  `s_acctbal` decimal(15, 2) NOT NULL COMMENT "",
  `s_comment` varchar(101) NOT NULL COMMENT ""
) 
DUPLICATE KEY(`s_suppkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12
PROPERTIES (
"replication_num" = "3",
"colocate_with" = "grosdf12sdoq3o",
"in_memory" = "false",
"storage_format" = "V2"
);

CREATE TABLE IF NOT EXISTS `part` (
  `p_partkey` int(11) NOT NULL COMMENT "",
  `p_name` varchar(55) NOT NULL COMMENT "",
  `p_mfgr` char(25) NOT NULL COMMENT "",
  `p_brand` char(10) NOT NULL COMMENT "",
  `p_type` varchar(25) NOT NULL COMMENT "",
  `p_size` int(11) NOT NULL COMMENT "",
  `p_container` char(10) NOT NULL COMMENT "",
  `p_retailprice` decimal(15, 2) NOT NULL COMMENT "",
  `p_comment` varchar(23) NOT NULL COMMENT ""
) 
DUPLICATE KEY(`p_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 12
PROPERTIES (
"replication_num" = "3",
"colocate_with" = "grosdf123p",
"in_memory" = "false",
"storage_format" = "V2"
);

CREATE TABLE IF NOT EXISTS `partsupp` (
  `ps_partkey` int(11) NOT NULL COMMENT "",
  `ps_suppkey` int(11) NOT NULL COMMENT "",
  `ps_availqty` int(11) NOT NULL COMMENT "",
  `ps_supplycost` decimal(15, 2) NOT NULL COMMENT "",
  `ps_comment` varchar(199) NOT NULL COMMENT ""
) 
DUPLICATE KEY(`ps_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 12
PROPERTIES (
"replication_num" = "3",
"colocate_with" = "grosdf123o",
"in_memory" = "false",
"storage_format" = "V2"
);