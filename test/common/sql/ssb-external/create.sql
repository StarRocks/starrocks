CREATE TABLE IF NOT EXISTS catalog.database.lineorder (
  `lo_orderkey` bigint(20) DEFAULT NULL,
  `lo_linenumber` int(11) DEFAULT NULL,
  `lo_custkey` int(11) DEFAULT NULL,
  `lo_partkey` int(11) DEFAULT NULL,
  `lo_suppkey` int(11) DEFAULT NULL,
  `lo_orderdate` int(11) DEFAULT NULL,
  `lo_orderpriority` varchar(16) DEFAULT NULL,
  `lo_shippriority` int(11) DEFAULT NULL,
  `lo_quantity` int(11) DEFAULT NULL,
  `lo_extendedprice` int(11) DEFAULT NULL,
  `lo_ordtotalprice` int(11) DEFAULT NULL,
  `lo_discount` int(11) DEFAULT NULL,
  `lo_revenue` int(11) DEFAULT NULL,
  `lo_supplycost` int(11) DEFAULT NULL,
  `lo_tax` int(11) DEFAULT NULL,
  `lo_commitdate` int(11) DEFAULT NULL,
  `lo_shipmode` varchar(11) DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS catalog.database.customer (
  `c_custkey` int(11) DEFAULT NULL,
  `c_name` varchar(26) DEFAULT NULL,
  `c_address` varchar(41) DEFAULT NULL,
  `c_city` varchar(11) DEFAULT NULL,
  `c_nation` varchar(16) DEFAULT NULL,
  `c_region` varchar(13) DEFAULT NULL,
  `c_phone` varchar(16) DEFAULT NULL,
  `c_mktsegment` varchar(11) DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS catalog.database.dates (
  `d_datekey` int(11) DEFAULT NULL,
  `d_date` varchar(20) DEFAULT NULL,
  `d_dayofweek` varchar(10) DEFAULT NULL,
  `d_month` varchar(11) DEFAULT NULL,
  `d_year` int(11) DEFAULT NULL,
  `d_yearmonthnum` int(11) DEFAULT NULL,
  `d_yearmonth` varchar(9) DEFAULT NULL,
  `d_daynuminweek` int(11) DEFAULT NULL,
  `d_daynuminmonth` int(11) DEFAULT NULL,
  `d_daynuminyear` int(11) DEFAULT NULL,
  `d_monthnuminyear` int(11) DEFAULT NULL,
  `d_weeknuminyear` int(11) DEFAULT NULL,
  `d_sellingseason` varchar(14) DEFAULT NULL,
  `d_lastdayinweekfl` int(11) DEFAULT NULL,
  `d_lastdayinmonthfl` int(11) DEFAULT NULL,
  `d_holidayfl` int(11) DEFAULT NULL,
  `d_weekdayfl` int(11) DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS catalog.database.part (
  `p_partkey` int(11) DEFAULT NULL,
  `p_name` varchar(23) DEFAULT NULL,
  `p_mfgr` varchar(7) DEFAULT NULL,
  `p_category` varchar(8) DEFAULT NULL,
  `p_brand` varchar(10) DEFAULT NULL,
  `p_color` varchar(12) DEFAULT NULL,
  `p_type` varchar(26) DEFAULT NULL,
  `p_size` int(11) DEFAULT NULL,
  `p_container` varchar(11) DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS catalog.database.supplier (
  `s_suppkey` int(11) DEFAULT NULL,
  `s_name` varchar(26) DEFAULT NULL,
  `s_address` varchar(26) DEFAULT NULL,
  `s_city` varchar(11) DEFAULT NULL,
  `s_nation` varchar(16) DEFAULT NULL,
  `s_region` varchar(13) DEFAULT NULL,
  `s_phone` varchar(16) DEFAULT NULL
);
