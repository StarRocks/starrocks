# 性能测试

## 测试方法

为了方便用户快速的了解StarRocks的性能指标，这里我们提供了一个标准的Star schema benchmark的测试方法和工具仅供参考。

Star schema benchmark（以下简称SSB）是学术界和工业界广泛使用的一个星型模型测试集，通过这个测试集合也可以容易的和其他OLAP产品进行性能对比。

## 测试准备

### 环境准备

* 硬件环境准备，StarRocks对机器没有严格要求，建议大于8C 32G，磁盘是SSD/SATA均可，网络建议万兆网卡。
* 集群部署参考 [集群部署](../administration/Deployment.md)
* 系统参数参考 [配置参数](../administration/Configuration.md)
* 下载ssb-poc工具集

### SSB SQL

~~~sql
--Q1.1
select sum(lo_revenue) as revenue
from lineorder join dates on lo_orderdate = d_datekey
where d_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25;
--Q1.2
select sum(lo_revenue) as revenue
from lineorder
join dates on lo_orderdate = d_datekey
where d_yearmonthnum = 199401
and lo_discount between 4 and 6
and lo_quantity between 26 and 35;
--Q1.3
select sum(lo_revenue) as revenue
from lineorder
join dates on lo_orderdate = d_datekey
where d_weeknuminyear = 6 and d_year = 1994
and lo_discount between 5 and 7
and lo_quantity between 26 and 35;
--Q2.1
select sum(lo_revenue) as lo_revenue, d_year, p_brand
from lineorder
join dates on lo_orderdate = d_datekey
join part on lo_partkey = p_partkey
join supplier on lo_suppkey = s_suppkey
where p_category = 'MFGR#12' and s_region = 'AMERICA'
group by d_year, p_brand
order by d_year, p_brand;
--Q2.2
select sum(lo_revenue) as lo_revenue, d_year, p_brand
from lineorder
join dates on lo_orderdate = d_datekey
join part on lo_partkey = p_partkey
join supplier on lo_suppkey = s_suppkey
where p_brand between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA'
group by d_year, p_brand
order by d_year, p_brand;
--Q2.3
select sum(lo_revenue) as lo_revenue, d_year, p_brand
from lineorder
join dates on lo_orderdate = d_datekey
join part on lo_partkey = p_partkey
join supplier on lo_suppkey = s_suppkey
where p_brand = 'MFGR#2239' and s_region = 'EUROPE'
group by d_year, p_brand
order by d_year, p_brand;
--Q3.1
select c_nation, s_nation, d_year, sum(lo_revenue) as lo_revenue
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
where c_region = 'ASIA' and s_region = 'ASIA'and d_year >= 1992 and d_year <= 1997
group by c_nation, s_nation, d_year
order by d_year asc, lo_revenue desc;
--Q3.2
select c_city, s_city, d_year, sum(lo_revenue) as lo_revenue
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
where c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES'
and d_year >= 1992 and d_year <= 1997
group by c_city, s_city, d_year
order by d_year asc, lo_revenue desc;
--Q3.3
select c_city, s_city, d_year, sum(lo_revenue) as lo_revenue
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
where (c_city='UNITED KI1' or c_city='UNITED KI5')
and (s_city='UNITED KI1' or s_city='UNITED KI5')
and d_year >= 1992 and d_year <= 1997
group by c_city, s_city, d_year
order by d_year asc, lo_revenue desc;
--Q3.4
select c_city, s_city, d_year, sum(lo_revenue) as lo_revenue
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
where (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and d_yearmonth = 'Dec1997'
group by c_city, s_city, d_year
order by d_year asc, lo_revenue desc;
--Q4.1
select d_year, c_nation, sum(lo_revenue) - sum(lo_supplycost) as profit
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
join part on lo_partkey = p_partkey
where c_region = 'AMERICA' and s_region = 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
group by d_year, c_nation
order by d_year, c_nation;
--Q4.2
select d_year, s_nation, p_category, sum(lo_revenue) - sum(lo_supplycost) as profit
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
join part on lo_partkey = p_partkey
where c_region = 'AMERICA'and s_region = 'AMERICA'
and (d_year = 1997 or d_year = 1998)
and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
group by d_year, s_nation, p_category
order by d_year, s_nation, p_category;
--Q4.3
select d_year, s_city, p_brand, sum(lo_revenue) - sum(lo_supplycost) as profit
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
join part on lo_partkey = p_partkey
where c_region = 'AMERICA'and s_nation = 'UNITED STATES'
and (d_year = 1997 or d_year = 1998)
and p_category = 'MFGR#14'
group by d_year, s_city, p_brand
order by d_year, s_city, p_brand;
~~~

## 测试流程

### 数据创建

#### 下载ssb-poc工具包并编译

~~~shell
make && make install
~~~

所有相关工具安装到output目录

#### 生成数据

~~~shell
bin/gen-ssb.sh 100 data_dir
~~~

#### 建表

建表中有三个注意事项，这几个选择会比较大的影响到测试结果：

1. Bucket的数量选择和分桶键的选择

    Bucket的数量是对性能影响比较大的因素之一，首先我们希望选择合理的分桶键（DISTRIBUTED BY HASH(key))，来保证数据在各个bucket中尽可能均衡，如果碰到数据倾斜严重的数据可以使用多列作为分桶键，或者采用MD5 hash以后作为分桶键，具体可以参考[分桶键选择](../table_design/Data_distribution.md#3-分桶列如何选择)，这里我们都统一使用唯一的key列。

2. 建表的数据类型

    数据类型的选择对性能测试的结果是有一定影响的，比如Decimal|String的运算一般比int|bigint要慢，所以在实际场景中我们应该尽可能准确的使用数据类型，从而达到最好的效果，比如可以使用Int|Bigint的字段就尽量避免使用String，如果是日期类型也多使用Date|Datetime以及相对应的时间日期函数，而不是用string和相关字符串操作函数来处理，针对SSB的数据集合，为了体现各个数据类型的效果，我们在多表Join的测试中都采用了Int|Bigint来处理，在lineorder_flat这个打平的宽表中，我们采用了Decimal|date等类型方便。

      >如果需要做和其他系统的对比，可以参考Kylin和Clickhouse的官方文档中推荐的SSB建表方式，对建表语句进行微调。
      >
      >[Kylin的建表方式](https://github.com/Kyligence/ssb-kylin/blob/master/hive/1_create_basic.sql)
      >
      >[Clickhouse的建表方式](https://clickhouse.tech/docs/en/getting-started/example-datasets/star-schema/)

3. 字段是否可以为空

    StarRocks的建表这里都采取的NOT NULL关键字，因为在SSB生成的标准数据集合中并没有空字段，但是对于实际的业务

针对我们三台BE的环境我们采取的建表方式如下：

~~~sql
CREATE TABLE IF NOT EXISTS `lineorder` (
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
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 96
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "group1",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);


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
"storage_format" = "DEFAULT"
);


CREATE TABLE IF NOT EXISTS `dates` (
  `d_datekey` int(11) NOT NULL COMMENT "",
  `d_date` varchar(20) NOT NULL COMMENT "",
  `d_dayofweek` varchar(10) NOT NULL COMMENT "",
  `d_month` varchar(11) NOT NULL COMMENT "",
  `d_year` int(11) NOT NULL COMMENT "",
  `d_yearmonthnum` int(11) NOT NULL COMMENT "",
  `d_yearmonth` varchar(9) NOT NULL COMMENT "",
  `d_daynuminweek` int(11) NOT NULL COMMENT "",
  `d_daynuminmonth` int(11) NOT NULL COMMENT "",
  `d_daynuminyear` int(11) NOT NULL COMMENT "",
  `d_monthnuminyear` int(11) NOT NULL COMMENT "",
  `d_weeknuminyear` int(11) NOT NULL COMMENT "",
  `d_sellingseason` varchar(14) NOT NULL COMMENT "",
  `d_lastdayinweekfl` int(11) NOT NULL COMMENT "",
  `d_lastdayinmonthfl` int(11) NOT NULL COMMENT "",
  `d_holidayfl` int(11) NOT NULL COMMENT "",
  `d_weekdayfl` int(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`d_datekey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`d_datekey`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"colocate_with" = "groupa3",
"storage_format" = "DEFAULT"
);

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
"storage_format" = "DEFAULT"
);

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
"storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS `lineorder_flat` (
  `LO_ORDERKEY` int(11) NOT NULL COMMENT "",
  `LO_ORDERDATE` date NOT NULL COMMENT "",
  `LO_LINENUMBER` tinyint(4) NOT NULL COMMENT "",
  `LO_CUSTKEY` int(11) NOT NULL COMMENT "",
  `LO_PARTKEY` int(11) NOT NULL COMMENT "",
  `LO_SUPPKEY` int(11) NOT NULL COMMENT "",
  `LO_ORDERPRIORITY` varchar(100) NOT NULL COMMENT "",
  `LO_SHIPPRIORITY` tinyint(4) NOT NULL COMMENT "",
  `LO_QUANTITY` tinyint(4) NOT NULL COMMENT "",
  `LO_EXTENDEDPRICE` int(11) NOT NULL COMMENT "",
  `LO_ORDTOTALPRICE` int(11) NOT NULL COMMENT "",
  `LO_DISCOUNT` tinyint(4) NOT NULL COMMENT "",
  `LO_REVENUE` int(11) NOT NULL COMMENT "",
  `LO_SUPPLYCOST` int(11) NOT NULL COMMENT "",
  `LO_TAX` tinyint(4) NOT NULL COMMENT "",
  `LO_COMMITDATE` date NOT NULL COMMENT "",
  `LO_SHIPMODE` varchar(100) NOT NULL COMMENT "",
  `C_NAME` varchar(100) NOT NULL COMMENT "",
  `C_ADDRESS` varchar(100) NOT NULL COMMENT "",
  `C_CITY` varchar(100) NOT NULL COMMENT "",
  `C_NATION` varchar(100) NOT NULL COMMENT "",
  `C_REGION` varchar(100) NOT NULL COMMENT "",
  `C_PHONE` varchar(100) NOT NULL COMMENT "",
  `C_MKTSEGMENT` varchar(100) NOT NULL COMMENT "",
  `S_NAME` varchar(100) NOT NULL COMMENT "",
  `S_ADDRESS` varchar(100) NOT NULL COMMENT "",
  `S_CITY` varchar(100) NOT NULL COMMENT "",
  `S_NATION` varchar(100) NOT NULL COMMENT "",
  `S_REGION` varchar(100) NOT NULL COMMENT "",
  `S_PHONE` varchar(100) NOT NULL COMMENT "",
  `P_NAME` varchar(100) NOT NULL COMMENT "",
  `P_MFGR` varchar(100) NOT NULL COMMENT "",
  `P_CATEGORY` varchar(100) NOT NULL COMMENT "",
  `P_BRAND` varchar(100) NOT NULL COMMENT "",
  `P_COLOR` varchar(100) NOT NULL COMMENT "",
  `P_TYPE` varchar(100) NOT NULL COMMENT "",
  `P_SIZE` tinyint(4) NOT NULL COMMENT "",
  `P_CONTAINER` varchar(100) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`LO_ORDERKEY`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`LO_ORDERKEY`) BUCKETS 192
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "groupxx1",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);
~~~

修改配置文件连接集群

~~~shell
# for mysql cmd
mysql_host: test1
mysql_port: 9030
mysql_user: root
mysql_password:
starrocks_db: ssb

# cluster ports
http_port: 8030
be_heartbeat_port: 9050
broker_port: 8000

...
~~~

执行脚本创建

~~~shell
bin/create_db_table.sh ddl_100
~~~

完成后我们创建了6张表，lineorder, supplier, dates, customer, part, lineorder\_flat

### 数据导入

这里我们通过stream load

~~~shell
bin/stream_load.sh data_dir
~~~

data\_dir 是9.3.1.2生成的数据目录

### 查询

测试ssb多表查询 (SQL 参见 share/ssb\_test/sql/ssb/)

~~~shell
bin/benchmark.sh -p -d ssb
~~~

生成ssb单表数据

~~~shell
bin/flat_insert.sh
~~~

测试ssb单表查询(SQL 参见 share/ssb\_test/sql/ssb-flat/)

~~~shell
bin/benchmark.sh -p -d ssb-flat
~~~
