CREATE TABLE `lineitem_monthly` (
  `l_shipdate` date NOT NULL COMMENT "",
  `l_orderkey` int(11) NOT NULL COMMENT "",
  `l_linenumber` int(11) NOT NULL COMMENT "",
  `l_partkey` int(11) NOT NULL COMMENT "",
  `l_suppkey` int(11) NOT NULL COMMENT "",
  `l_quantity` decimal(15, 2) NOT NULL COMMENT "",
  `l_extendedprice` decimal(15, 2) NOT NULL COMMENT "",
  `l_discount` decimal(15, 2) NOT NULL COMMENT "",
  `l_tax` decimal(15, 2) NOT NULL COMMENT "",
  `l_returnflag` varchar(1) NOT NULL COMMENT "",
  `l_linestatus` varchar(1) NOT NULL COMMENT "",
  `l_commitdate` date NOT NULL COMMENT "",
  `l_receiptdate` date NOT NULL COMMENT "",
  `l_shipinstruct` varchar(25) NOT NULL COMMENT "",
  `l_shipmode` varchar(10) NOT NULL COMMENT "",
  `l_comment` varchar(44) NOT NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`l_shipdate`, `l_orderkey`)
COMMENT "OLAP"
PARTITION BY date_trunc('month',l_shipdate)
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96 
PROPERTIES (
"colocate_with" = "group_tpch_100",
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
