CREATE TABLE `orders_monthly` (
  `o_orderkey` int(11) NOT NULL COMMENT "",
  `o_orderdate` date NOT NULL COMMENT "",
  `o_custkey` int(11) NOT NULL COMMENT "",
  `o_orderstatus` varchar(1) NOT NULL COMMENT "",
  `o_totalprice` decimal(15, 2) NOT NULL COMMENT "",
  `o_orderpriority` varchar(15) NOT NULL COMMENT "",
  `o_clerk` varchar(15) NOT NULL COMMENT "",
  `o_shippriority` int(11) NOT NULL COMMENT "",
  `o_comment` varchar(79) NOT NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`o_orderkey`, `o_orderdate`)
COMMENT "OLAP"
PARTITION BY date_trunc('month',o_orderdate)
DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96 
PROPERTIES (
"colocate_with" = "group_tpch_100",
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
