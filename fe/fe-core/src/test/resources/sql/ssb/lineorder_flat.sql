CREATE TABLE `lineorder_flat` (
  `lo_orderdate` date NOT NULL COMMENT "",
  `lo_orderkey` int(11) NOT NULL COMMENT "",
  `lo_linenumber` tinyint(4) NOT NULL COMMENT "",
  `lo_custkey` int(11) NOT NULL COMMENT "",
  `lo_partkey` int(11) NOT NULL COMMENT "",
  `lo_suppkey` int(11) NOT NULL COMMENT "",
  `lo_orderpriority` varchar(100) NOT NULL COMMENT "",
  `lo_shippriority` tinyint(4) NOT NULL COMMENT "",
  `lo_quantity` tinyint(4) NOT NULL COMMENT "",
  `lo_extendedprice` int(11) NOT NULL COMMENT "",
  `lo_ordtotalprice` int(11) NOT NULL COMMENT "",
  `lo_discount` tinyint(4) NOT NULL COMMENT "",
  `lo_revenue` int(11) NOT NULL COMMENT "",
  `lo_supplycost` int(11) NOT NULL COMMENT "",
  `lo_tax` tinyint(4) NOT NULL COMMENT "",
  `lo_commitdate` date NOT NULL COMMENT "",
  `lo_shipmode` varchar(100) NOT NULL COMMENT "",
  `c_name` varchar(100) NOT NULL COMMENT "",
  `c_address` varchar(100) NOT NULL COMMENT "",
  `c_city` varchar(100) NOT NULL COMMENT "",
  `c_nation` varchar(100) NOT NULL COMMENT "",
  `c_region` varchar(100) NOT NULL COMMENT "",
  `c_phone` varchar(100) NOT NULL COMMENT "",
  `c_mktsegment` varchar(100) NOT NULL COMMENT "",
  `s_name` varchar(100) NOT NULL COMMENT "",
  `s_address` varchar(100) NOT NULL COMMENT "",
  `s_city` varchar(100) NOT NULL COMMENT "",
  `s_nation` varchar(100) NOT NULL COMMENT "",
  `s_region` varchar(100) NOT NULL COMMENT "",
  `s_phone` varchar(100) NOT NULL COMMENT "",
  `p_name` varchar(100) NOT NULL COMMENT "",
  `p_mfgr` varchar(100) NOT NULL COMMENT "",
  `p_category` varchar(100) NOT NULL COMMENT "",
  `p_brand` varchar(100) NOT NULL COMMENT "",
  `p_color` varchar(100) NOT NULL COMMENT "",
  `p_type` varchar(100) NOT NULL COMMENT "",
  `p_size` tinyint(4) NOT NULL COMMENT "",
  `p_container` varchar(100) NOT NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`lo_orderdate`, `lo_orderkey`)
COMMENT "olap"
PARTITION BY RANGE(`lo_orderdate`)
(PARTITION p1 VALUES [("0000-01-01"), ("1993-01-01")),
PARTITION p2 VALUES [("1993-01-01"), ("1994-01-01")),
PARTITION p3 VALUES [("1994-01-01"), ("1995-01-01")),
PARTITION p4 VALUES [("1995-01-01"), ("1996-01-01")),
PARTITION p5 VALUES [("1996-01-01"), ("1997-01-01")),
PARTITION p6 VALUES [("1997-01-01"), ("1998-01-01")),
PARTITION p7 VALUES [("1998-01-01"), ("1999-01-01")))
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 48 
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "groupxx1",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
); 
