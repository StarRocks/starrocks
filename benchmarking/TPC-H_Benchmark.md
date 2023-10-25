# TPC-H 基准测试

TPC-H 是美国交易处理效能委员会 TPC（Transaction Processing Performance Council）组织制定的用来模拟决策支持类应用的测试集。它包括一整套面向业务的 ad-hoc 查询和并发数据修改。

TPC-H 根据真实的生产运行环境来建模，模拟了一套销售系统的数据仓库。该测试共包含8张表，数据量可设定从1GB~3TB 不等。其基准测试共包含了22个查询，主要评价指标为各个查询的响应时间，即从提交查询到结果返回所需时间。

## 一、测试结论

在 TPCH 100G 规模的数据集上进行对比测试，共22个查询，结果如下：

![TPCH 100G结果](../assets/7.2.png)

StarRocks 使用本地存储查询和 Hive 外表查询两种方式进行测试。其中，StarRocks Hive 外表和 Trino 查询的是同一份数据，数据采用 ORC 格式存储，采用 zlib 格式压缩。

最终，StarRocks 本地存储查询总耗时为21s，StarRocks Hive 外表查询总耗时92s。Trino 查询总耗时307s。

## 二、测试准备

### （一）硬件环境与成本

| 机器     | 3台 阿里云主机                                  |
| -------- | ----------------------------------------------- |
| CPU      | 16 core                                         |
| 内存     | 64 GB                                           |
| 网络带宽 | 5 Gbits/s                                       |
| 磁盘     | 系统盘：ESSD云盘 40GB<br />数据盘：高效云盘 100GB |

### （二）软件环境

内核版本：Linux 3.10.0-1127.13.1.el7.x86_64

操作系统版本：CentOS Linux release 7.8.2003  

软件版本：StarRocks 2.1，Trino-357， Hive-3.1.2。

> 说明： StarRocks 的 FE 可以单独部署也可以和 BE 混合部署，对性能测试无影响。

## 三、测试数据与结果

### （一）测试数据

| 表名     | 100G 数据行数 | 100G StarRocks 导入后磁盘容量 |
| -------- | ------------- | ----------------------------- |
| customer | 1500万        | 2.4G                          |
| lineitem | 6亿           | 75G                           |
| nation   | 25            | 2.2K                          |
| orders   | 1.5亿         | 16G                           |
| part     | 2000万        | 2.4G                          |
| partsupp | 8000万        | 11.6G                         |
| region   | 5             | 389B                          |
| supplier | 100万         | 0.14G                         |

### （二）测试SQL

```SQL
--Q1
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity) as sum_qty,
  sum(l_extendedprice) as sum_base_price,
  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
  avg(l_quantity) as avg_qty,
  avg(l_extendedprice) as avg_price,
  avg(l_discount) as avg_disc,
  count(*) as count_order
from lineitem
where  l_shipdate <= date '1998-12-01'
group by  l_returnflag,  l_linestatus
order by  l_returnflag,  l_linestatus;

--Q2
select  s_acctbal, s_name,  n_name,  p_partkey,  p_mfgr,  s_address,  s_phone,  s_comment
from  part,  partsupp,  supplier,  nation,  region
where
  p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 15
  and p_type like '%BRASS'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'EUROPE'
  and ps_supplycost = (
    select  min(ps_supplycost)
    from  partsupp,  supplier,  nation,  region
    where
      p_partkey = ps_partkey
      and s_suppkey = ps_suppkey
      and s_nationkey = n_nationkey
      and n_regionkey = r_regionkey
      and r_name = 'EUROPE'
  )
order by  s_acctbal desc, n_name,  s_name,  p_partkey;

--Q3
select
  l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority
from
  customer, orders, lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by l_orderkey, o_orderdate, o_shippriority
order by revenue desc, o_orderdate;

--Q4
select  o_orderpriority,  count(*) as order_count
from  orders
where
  o_orderdate >= date '1993-07-01'
  and o_orderdate < date '1993-07-01' + interval '3' month
  and exists (
    select  * from  lineitem
    where  l_orderkey = o_orderkey and l_commitdate < l_receiptdate
  )
group by o_orderpriority
order by o_orderpriority;

--Q5
select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
from customer, orders, lineitem, supplier, nation, region
where
  c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'ASIA'
  and o_orderdate >= date '1994-01-01'
  and o_orderdate < date '1994-01-01' + interval '1' year
group by n_name
order by revenue desc;

--Q6
select
  sum(l_extendedprice * l_discount) as revenue
from
  lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;

--Q7
select supp_nation, cust_nation, l_year, sum(volume) as revenue
from (
    select
      n1.n_name as supp_nation,
      n2.n_name as cust_nation,
      extract(year from l_shipdate) as l_year,
      l_extendedprice * (1 - l_discount) as volume
    from supplier, lineitem, orders, customer, nation n1, nation n2
    where
      s_suppkey = l_suppkey
      and o_orderkey = l_orderkey
      and c_custkey = o_custkey
      and s_nationkey = n1.n_nationkey
      and c_nationkey = n2.n_nationkey
      and (
        (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
        or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
      )
      and l_shipdate between date '1995-01-01' and date '1996-12-31'
  ) as shipping
group by supp_nation, cust_nation, l_year
order by supp_nation, cust_nation, l_year;

--Q8
select
  o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
from
  (
    select
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) as volume,
      n2.n_name as nation
    from part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    where
      p_partkey = l_partkey
      and s_suppkey = l_suppkey
      and l_orderkey = o_orderkey
      and o_custkey = c_custkey
      and c_nationkey = n1.n_nationkey
      and n1.n_regionkey = r_regionkey
      and r_name = 'AMERICA'
      and s_nationkey = n2.n_nationkey
      and o_orderdate between date '1995-01-01' and date '1996-12-31'
      and p_type = 'ECONOMY ANODIZED STEEL'
  ) as all_nations
group by o_year
order by o_year;

--Q9
select nation, o_year, sum(amount) as sum_profit
from
  (
    select
      n_name as nation,
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from part, supplier, lineitem, partsupp, orders, nation
    where
      s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%green%'
  ) as profit
group by nation, o_year
order by nation, o_year desc;

--Q10
select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment
    from customer, orders, lineitem, nation
    where
      c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and o_orderdate >= date '1993-10-01'
      and o_orderdate < date '1993-12-01'
      and l_returnflag = 'R'
      and c_nationkey = n_nationkey
    group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
    order by revenue desc;

--Q11
select ps_partkey, sum(ps_supplycost * ps_availqty) as value
from partsupp, supplier, nation
where
  ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'GERMANY'
group by ps_partkey having
    sum(ps_supplycost * ps_availqty) > (
      select sum(ps_supplycost * ps_availqty) * 0.0001000000
      from partsupp, supplier, nation
      where
        ps_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'GERMANY'
    )
order by value desc;

--Q12
select
  l_shipmode,
  sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
  sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
from orders, lineitem
where
  o_orderkey = l_orderkey
  and l_shipmode in ('MAIL', 'SHIP')
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1994-01-01'
  and l_receiptdate < date '1994-01-01' + interval '1' year
group by l_shipmode
order by l_shipmode;

--Q13
select  c_count,  count(*) as custdist
from (select c_custkey,count(o_orderkey) as c_count
    from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%special%requests%'
    group by  c_custkey
) as c_orders
group by  c_count
order by custdist desc, c_count desc;

--Q14
select
  100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from lineitem, part
where
  l_partkey = p_partkey
  and l_shipdate >= date '1995-09-01'
  and l_shipdate < date '1995-09-01' + interval '1' month;

--Q15
select s_suppkey, s_name, s_address, s_phone, total_revenue
from supplier, revenue0
where
  s_suppkey = supplier_no
  and total_revenue = (
    select max(total_revenue) from revenue0
  )
order by s_suppkey;

--Q16
select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
from partsupp, part
where
  p_partkey = ps_partkey
  and p_brand <> 'Brand#45'
  and p_type not like 'MEDIUM POLISHED%'
  and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
  and ps_suppkey not in (
    select s_suppkey from supplier where s_comment like '%Customer%Complaints%'
  )
group by p_brand, p_type, p_size
order by supplier_cnt desc, p_brand, p_type, p_size;

--Q17
select sum(l_extendedprice) / 7.0 as avg_yearly
from lineitem, part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey
  );

--Q18
select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
from customer, orders, lineitem
where
  o_orderkey in (
    select l_orderkey from lineitem
    group by l_orderkey having
        sum(l_quantity) > 300
  )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
order by o_totalprice desc, o_orderdate;

--Q19
select sum(l_extendedprice* (1 - l_discount)) as revenue
from lineitem, part
where
  (
    p_partkey = l_partkey
    and p_brand = 'Brand#12'
    and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    and l_quantity >= 1 and l_quantity <= 1 + 10
    and p_size between 1 and 5
    and l_shipmode in ('AIR', 'AIR REG')
    and l_shipinstruct = 'DELIVER IN PERSON'
  )
  or
  (
    p_partkey = l_partkey
    and p_brand = 'Brand#23'
    and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    and l_quantity >= 10 and l_quantity <= 10 + 10
    and p_size between 1 and 10
    and l_shipmode in ('AIR', 'AIR REG')
    and l_shipinstruct = 'DELIVER IN PERSON'
  )
  or
  (
    p_partkey = l_partkey
    and p_brand = 'Brand#34'
    and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    and l_quantity >= 20 and l_quantity <= 20 + 10
    and p_size between 1 and 15
    and l_shipmode in ('AIR', 'AIR REG')
    and l_shipinstruct = 'DELIVER IN PERSON'
  );

--Q20
select s_name, s_address
from supplier, nation
where
  s_suppkey in (
    select ps_suppkey
    from partsupp
    where
      ps_partkey in (
        select p_partkey from part
        where p_name like 'forest%'
      )
      and ps_availqty > (
        select 0.5 * sum(l_quantity)
        from lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
      )
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by s_name;

--Q21
select s_name, count(*) as numwait
from  supplier,  lineitem l1,  orders,  nation
where
  s_suppkey = l1.l_suppkey
  and o_orderkey = l1.l_orderkey
  and o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists (
    select * from  lineitem l2
    where
      l2.l_orderkey = l1.l_orderkey
      and l2.l_suppkey <> l1.l_suppkey
  )
  and not exists (
    select * from  lineitem l3
    where
      l3.l_orderkey = l1.l_orderkey
      and l3.l_suppkey <> l1.l_suppkey
      and l3.l_receiptdate > l3.l_commitdate
  )
  and s_nationkey = n_nationkey
  and n_name = 'SAUDI ARABIA'
group by s_name
order by  numwait desc, s_name;

--Q22
select
  cntrycode,
  count(*) as numcust,
  sum(c_acctbal) as totacctbal
from
  (
    select
      substr(c_phone, 1, 2) as cntrycode,
      c_acctbal
    from
      customer
    where
      substr(c_phone, 1, 2) in
        ('13', '31', '23', '29', '30', '18', '17')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          customer
        where
          c_acctbal > 0.00
          and substr(c_phone, 1, 2) in
            ('13', '31', '23', '29', '30', '18', '17')
      )
      and not exists (
        select
          *
        from
          orders
        where
          o_custkey = c_custkey
      )
  ) as custsale
group by
  cntrycode
order by
  cntrycode;
```

### （三）测试结果

| Query | SR-native(ms) | SR-Hive external(ms) | Trino(ms) |
| ----- | ------------- | -------------------- | --------- |
| q1    | 1291          | 5349                 | 14350     |
| q2    | 257           | 940                  | 4075      |
| q3    | 690           | 2585                 | 7615      |
| q4    | 529           | 2514                 | 9385      |
| q5    | 1142          | 5844                 | 15350     |
| q6    | 115           | 2936                 | 4045      |
| q7    | 1435          | 5128                 | 8760      |
| q8    | 855           | 5989                 | 9535      |
| q9    | 3028          | 5507                 | 27310     |
| q10   | 1381          | 3654                 | 6560      |
| q11   | 265           | 691                  | 2090      |
| q12   | 387           | 3827                 | 7635      |
| q13   | 2165          | 3733                 | 14530     |
| q14   | 184           | 3824                 | 4355      |
| q15   | 354           | 7321                 | 8975      |
| q16   | 240           | 1029                 | 2265      |
| q17   | 753           | 5011                 | 46405     |
| q18   | 2909          | 7032                 | 30975     |
| q19   | 702           | 3905                 | 7315      |
| q20   | 963           | 4623                 | 12500     |
| q21   | 870           | 10016                | 58340     |
| q22   | 545           | 991                  | 5605      |
| sum   | 21060         | 92449                | 307975    |

## 四、测试流程

### （一）StarRocks 本地表测试流程

#### 1. 数据生成

首先下载 tpch-poc 工具包并编译。

```Bash
wget https://starrocks-public.oss-cn-zhangjiakou.aliyuncs.com/tpch-poc-0.1.2.zip
unzip tpch-poc-0.1.2.zip
cd tpch-poc-0.1.2
cd benchmark 
```

相关工具在`bin`目录，可以直接在主目录发起操作。

生成数据。

```Bash
# generate 100GB data under the `data_100` directory
./bin/gen_data/gen-tpch.sh 100 data_100
```

#### 2. 创建表结构

修改配置文件 conf/starrocks.conf，指定脚本操作的集群地址、用户名和密码、数据库名。

```Bash
# for mysql cmd
mysql_host: 192.168.1.1
mysql_port: 9030
mysql_user: root
mysql_password:
database: tpch

# cluster ports
http_port: 8030
be_heartbeat_port: 9050
broker_port: 8000

# parallel_fragment_exec_instance_num 设置并行度，建议是每个集群节点逻辑核数的一半, 以下以8为例
parallel_num: 8
...
```

执行脚本建表。

```Bash
# create tables for 100GB data
./bin/create_db_table.sh ddl_100
```

以下为建表语句，在上一步脚本中已经创建了表，并进行了默认分桶数配置。您可以删除该表，然后根据集群规模节点配置重新规划分桶数再进行创建，可实现更好测试效果。

```SQL
#创建表customer
drop table if exists customer;
CREATE TABLE customer (
    c_custkey     int NOT NULL,
    c_name        VARCHAR(25) NOT NULL,
    c_address     VARCHAR(40) NOT NULL,
    c_nationkey   int NOT NULL,
    c_phone       VARCHAR(15) NOT NULL,
    c_acctbal     decimal(15, 2)   NOT NULL,
    c_mktsegment  VARCHAR(10) NOT NULL,
    c_comment     VARCHAR(117) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 24
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "DEFAULT"
);

#创建表lineitem
drop table if exists lineitem;
CREATE TABLE lineitem ( 
    l_shipdate    DATE NOT NULL,
    l_orderkey    int NOT NULL,
    l_linenumber  int not null,
    l_partkey     int NOT NULL,
    l_suppkey     int not null,
    l_quantity    decimal(15, 2) NOT NULL,
    l_extendedprice  decimal(15, 2) NOT NULL,
    l_discount    decimal(15, 2) NOT NULL,
    l_tax         decimal(15, 2) NOT NULL,
    l_returnflag  VARCHAR(1) NOT NULL,
    l_linestatus  VARCHAR(1) NOT NULL,
    l_commitdate  DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct VARCHAR(25) NOT NULL,
    l_shipmode     VARCHAR(10) NOT NULL,
    l_comment      VARCHAR(44) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`l_shipdate`, `l_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "DEFAULT",
    "colocate_with" = "tpch2"
);

#创建表nation
drop table if exists nation;
CREATE TABLE `nation` (
  `n_nationkey` int(11) NOT NULL,
  `n_name`      varchar(25) NOT NULL,
  `n_regionkey` int(11) NOT NULL,
  `n_comment`   varchar(152) NULL
) ENGINE=OLAP
DUPLICATE KEY(`N_NATIONKEY`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "DEFAULT"
);

#创建表orders
drop table if exists orders;
CREATE TABLE orders  (
    o_orderkey       int NOT NULL,
    o_orderdate      DATE NOT NULL,
    o_custkey        int NOT NULL,
    o_orderstatus    VARCHAR(1) NOT NULL,
    o_totalprice     decimal(15, 2) NOT NULL,
    o_orderpriority  VARCHAR(15) NOT NULL,
    o_clerk          VARCHAR(15) NOT NULL,
    o_shippriority   int NOT NULL,
    o_comment        VARCHAR(79) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`o_orderkey`, `o_orderdate`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "DEFAULT",
    "colocate_with" = "tpch2"
);

#创建表part
drop table if exists part;
CREATE TABLE part (
    p_partkey          int NOT NULL,
    p_name        VARCHAR(55) NOT NULL,
    p_mfgr        VARCHAR(25) NOT NULL,
    p_brand       VARCHAR(10) NOT NULL,
    p_type        VARCHAR(25) NOT NULL,
    p_size        int NOT NULL,
    p_container   VARCHAR(10) NOT NULL,
    p_retailprice decimal(15, 2) NOT NULL,
    p_comment     VARCHAR(23) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`p_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 24
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "DEFAULT",
    "colocate_with" = "tpch2p"
);

#创建表partsupp
drop table if exists partsupp;
CREATE TABLE partsupp ( 
    ps_partkey          int NOT NULL,
    ps_suppkey     int NOT NULL,
    ps_availqty    int NOT NULL,
    ps_supplycost  decimal(15, 2)  NOT NULL,
    ps_comment     VARCHAR(199) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`ps_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 24
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "DEFAULT",
    "colocate_with" = "tpch2p"
);

#创建表region
drop table if exists region;
CREATE TABLE region  ( 
    r_regionkey      int NOT NULL,
    r_name       VARCHAR(25) NOT NULL,
    r_comment    VARCHAR(152)
)ENGINE=OLAP
DUPLICATE KEY(`r_regionkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "DEFAULT"
);

#创建表supplier
drop table if exists supplier;
CREATE TABLE supplier (  
    s_suppkey       int NOT NULL,
    s_name        VARCHAR(25) NOT NULL,
    s_address     VARCHAR(40) NOT NULL,
    s_nationkey   int NOT NULL,
    s_phone       VARCHAR(15) NOT NULL,
    s_acctbal     decimal(15, 2) NOT NULL,
    s_comment     VARCHAR(101) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`s_suppkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "DEFAULT"
);


drop view if exists revenue0;
create view revenue0 (supplier_no, total_revenue) as
select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
from
    lineitem
where
    l_shipdate >= date '1996-01-01'
    and l_shipdate < date '1996-01-01' + interval '3' month
group by
    l_suppkey;
```

#### 3. 数据导入

```Python
./bin/stream_load.sh data_100
```

#### 4. 收集统计信息

这一步是可选的，但是为了获得最好的测试效果，推荐全量采集统计信息，连上StarRocks集群以后执行以下命令。

```SQL
ANALYZE FULL TABLE customer;
ANALYZE FULL TABLE lineitem;
ANALYZE FULL TABLE nation;
ANALYZE FULL TABLE orders;
ANALYZE FULL TABLE part;
ANALYZE FULL TABLE partsupp;
ANALYZE FULL TABLE region;
ANALYZE FULL TABLE supplier;
```

#### 5. 数据查询

```Python
./bin/benchmark.sh -p -d tpch
```

### （二）StarRocks Hive 外表测试流程

#### 1. 创建表结构

在Hive中创建对应表结构，存储格式为 ORC。

```SQL
create database tpch_hive_orc;
use tpch_hive_orc;

--创建customer
CREATE TABLE `customer`(
  `c_custkey` int, 
  `c_name` varchar(25), 
  `c_address` varchar(40), 
  `c_nationkey` int, 
  `c_phone` varchar(15), 
  `c_acctbal` decimal(15,2), 
  `c_mktsegment` varchar(10), 
  `c_comment` varchar(117))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-49146:9000/user/hive/warehouse/tpch_hive_orc.db/customer';

--创建lineitem
CREATE TABLE `lineitem`(
  `l_orderkey` bigint, 
  `l_partkey` int, 
  `l_suppkey` int, 
  `l_linenumber` int, 
  `l_quantity` decimal(15,2), 
  `l_extendedprice` decimal(15,2), 
  `l_discount` decimal(15,2), 
  `l_tax` decimal(15,2), 
  `l_returnflag` varchar(1), 
  `l_linestatus` varchar(1), 
  `l_shipdate` date, 
  `l_commitdate` date, 
  `l_receiptdate` date, 
  `l_shipinstruct` varchar(25), 
  `l_shipmode` varchar(10), 
  `l_comment` varchar(44))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-49146:9000/user/hive/warehouse/tpch_hive_orc.db/lineitem';

--创建nation
CREATE TABLE `nation`(
  `n_nationkey` int, 
  `n_name` varchar(25), 
  `n_regionkey` int, 
  `n_comment` varchar(152))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-49146:9000/user/hive/warehouse/tpch_hive_orc.db/nation';

--创建orders
CREATE TABLE `orders`(
  `o_orderkey` bigint, 
  `o_custkey` int, 
  `o_orderstatus` varchar(1), 
  `o_totalprice` decimal(15,2), 
  `o_orderdate` date, 
  `o_orderpriority` varchar(15), 
  `o_clerk` varchar(15), 
  `o_shippriority` int, 
  `o_comment` varchar(79))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-49146:9000/user/hive/warehouse/tpch_hive_orc.db/orders';

--创建part
CREATE TABLE `part`(
  `p_partkey` int, 
  `p_name` varchar(55), 
  `p_mfgr` varchar(25), 
  `p_brand` varchar(10), 
  `p_type` varchar(25), 
  `p_size` int, 
  `p_container` varchar(10), 
  `p_retailprice` decimal(15,2), 
  `p_comment` varchar(23))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-49146:9000/user/hive/warehouse/tpch_hive_orc.db/part';

--创建partsupp
CREATE TABLE `partsupp`(
  `ps_partkey` int, 
  `ps_suppkey` int, 
  `ps_availqty` int, 
  `ps_supplycost` decimal(15,2), 
  `ps_comment` varchar(199))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-49146:9000/user/hive/warehouse/tpch_hive_orc.db/partsupp';

--创建region
CREATE TABLE `region`(
  `r_regionkey` int, 
  `r_name` varchar(25), 
  `r_comment` varchar(152))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-49146:9000/user/hive/warehouse/tpch_hive_orc.db/region';

--创建supplier
CREATE TABLE `supplier`(
  `s_suppkey` int, 
  `s_name` varchar(25), 
  `s_address` varchar(40), 
  `s_nationkey` int, 
  `s_phone` varchar(15), 
  `s_acctbal` decimal(15,2), 
  `s_comment` varchar(101))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-49146:9000/user/hive/warehouse/tpch_hive_orc.db/supplier';
```

StarRocks中的外表如下：

```SQL
create database if not exists tpch_sr;

use tpch_sr;

CREATE EXTERNAL RESOURCE "hive_test" PROPERTIES (
  "type" = "hive",
  "hive.metastore.uris" = "thrift://xxx.xxx.xxx.xxx:9083"
);
          
CREATE EXTERNAL TABLE IF NOT EXISTS nation (
    n_nationkey int,
    n_name VARCHAR(25),
    n_regionkey int,
    n_comment varchar(152))
ENGINE=hive
properties (
    "resource" = "hive_test",
    "table" = "nation",
    "database" = "tpch_hive_orc"
);
    
CREATE EXTERNAL TABLE IF NOT EXISTS customer (
    c_custkey     INTEGER,
    c_name        VARCHAR(25),
    c_address     VARCHAR(40),
    c_nationkey   INTEGER,
    c_phone       VARCHAR(15),
    c_acctbal     DECIMAL(15,2),
    c_mktsegment  VARCHAR(10),
    c_comment     VARCHAR(117))
ENGINE=hive
properties (
    "resource" = "hive_test",
    "table" = "customer",
    "database" = "tpch_hive_orc"
);
    
CREATE EXTERNAL TABLE IF NOT EXISTS lineitem (
    l_orderkey    BIGINT,
    l_partkey     INTEGER,
    l_suppkey     INTEGER,
    l_linenumber  INTEGER,
    l_quantity    DECIMAL(15,2),
    l_extendedprice  DECIMAL(15,2),
    l_discount    DECIMAL(15,2),
    l_tax         DECIMAL(15,2),
    l_returnflag  VARCHAR(1),
    l_linestatus  VARCHAR(1),
    l_shipdate    DATE,
    l_commitdate  DATE,
    l_receiptdate DATE,
    l_shipinstruct VARCHAR(25),
    l_shipmode     VARCHAR(10),
    l_comment      VARCHAR(44))
ENGINE=hive
properties (
    "resource" = "hive_test",
    "table" = "lineitem",
    "database" = "tpch_hive_orc"
);
    
CREATE EXTERNAL TABLE IF NOT EXISTS orders  ( 
    o_orderkey       BIGINT,
    o_custkey        INTEGER,
    o_orderstatus    VARCHAR(1),
    o_totalprice     DECIMAL(15,2),
    o_orderdate      DATE,
    o_orderpriority  VARCHAR(15),
    o_clerk          VARCHAR(15),
    o_shippriority   INTEGER,
    o_comment        VARCHAR(79))
ENGINE=hive
properties (
    "resource" = "hive_test",
    "table" = "orders",
    "database" = "tpch_hive_orc"
);
    
CREATE EXTERNAL TABLE IF NOT EXISTS part  ( 
    p_partkey     INTEGER,
    p_name        VARCHAR(55),
    p_mfgr        VARCHAR(25),
    p_brand       VARCHAR(10),
    p_type        VARCHAR(25),
    p_size        INTEGER,
    p_container   VARCHAR(10),
    p_retailprice DECIMAL(15,2),
    p_comment     VARCHAR(23))
ENGINE=hive
properties (
    "resource" = "hive_test",
    "table" = "part",
    "database" = "tpch_hive_orc"
);
    
CREATE EXTERNAL TABLE IF NOT EXISTS partsupp ( 
    ps_partkey     INTEGER,
    ps_suppkey     INTEGER,
    ps_availqty    INTEGER,
    ps_supplycost  DECIMAL(15,2),
    ps_comment     VARCHAR(199))
ENGINE=hive
properties (
    "resource" = "hive_test",
    "table" = "partsupp",
    "database" = "tpch_hive_orc"
);
    
CREATE EXTERNAL TABLE IF NOT EXISTS region  ( 
    r_regionkey  INTEGER,
    r_name       VARCHAR(25),
    r_comment    VARCHAR(152))
ENGINE=hive
properties (
    "resource" = "hive_test",
    "table" = "region",
    "database" = "tpch_hive_orc"
);
    
CREATE EXTERNAL TABLE IF NOT EXISTS supplier (
    s_suppkey     INTEGER,
    s_name        VARCHAR(25),
    s_address     VARCHAR(40),
    s_nationkey   INTEGER,
    s_phone       VARCHAR(15),
    s_acctbal     DECIMAL(15,2),
    s_comment     VARCHAR(101))
ENGINE=hive
properties (
    "resource" = "hive_test",
    "table" = "supplier",
    "database" = "tpch_hive_orc"
);

create view revenue0 (supplier_no, total_revenue) as
select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
from
    lineitem
where
    l_shipdate >= date '1996-01-01'
    and l_shipdate < date '1996-01-01' + interval '3' month
group by
    l_suppkey;
```

#### 2. 数据导入

(1) 在 hive 中创建 hive 外部表，外部表的存储格式为 csv，并将生成的 csv 格式测试数据上传到对应表在 hdfs 上的数据存放目录中。本例中 hive 外部表的 hdfs 的数据存储目录为：/user/tmp/csv/。

```SQL
create database tpch_hive_csv;
use tpch_hive_csv;

--创建customer外表
CREATE EXTERNAL TABLE `customer`(
  `c_custkey` int, 
  `c_name` varchar(25), 
  `c_address` varchar(40), 
  `c_nationkey` int, 
  `c_phone` varchar(15), 
  `c_acctbal` double, 
  `c_mktsegment` varchar(10), 
  `c_comment` varchar(117))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-49146:9000/user/tmp/csv/customer_csv';
 
--创建lineitem外表
CREATE EXTERNAL TABLE `lineitem`(
  `l_orderkey` int, 
  `l_partkey` int, 
  `l_suppkey` int, 
  `l_linenumber` int, 
  `l_quantity` double, 
  `l_extendedprice` double, 
  `l_discount` double, 
  `l_tax` double, 
  `l_returnflag` varchar(1), 
  `l_linestatus` varchar(1), 
  `l_shipdate` date, 
  `l_commitdate` date, 
  `l_receiptdate` date, 
  `l_shipinstruct` varchar(25), 
  `l_shipmode` varchar(10), 
  `l_comment` varchar(44))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-49146:9000/user/tmp/csv/lineitem_csv';
 

--创建nation外表
CREATE EXTERNAL TABLE `nation`(
  `n_nationkey` int, 
  `n_name` varchar(25), 
  `n_regionkey` int, 
  `n_comment` varchar(152))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-49146:9000/user/tmp/csv/nation_csv';
  
--创建orders外表
CREATE EXTERNAL TABLE `orders`(
  `o_orderkey` int, 
  `o_custkey` int, 
  `o_orderstatus` varchar(1), 
  `o_totalprice` double, 
  `o_orderdate` date, 
  `o_orderpriority` varchar(15), 
  `o_clerk` varchar(15), 
  `o_shippriority` int, 
  `o_comment` varchar(79))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-49146:9000/user/tmp/csv/orders_csv';
  
--创建part外表
CREATE EXTERNAL TABLE `part`(
  `p_partkey` int, 
  `p_name` varchar(55), 
  `p_mfgr` varchar(25), 
  `p_brand` varchar(10), 
  `p_type` varchar(25), 
  `p_size` int, 
  `p_container` varchar(10), 
  `p_retailprice` double, 
  `p_comment` varchar(23))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-49146:9000/user/tmp/csv/part_csv';
  
--创建partsupp外表
CREATE EXTERNAL TABLE `partsupp`(
  `ps_partkey` int, 
  `ps_suppkey` int, 
  `ps_availqty` int, 
  `ps_supplycost` double, 
  `ps_comment` varchar(199))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-49146:9000/user/tmp/csv/partsupp_csv';
  
--创建region外表
CREATE EXTERNAL TABLE `region`(
  `r_regionkey` int, 
  `r_name` varchar(25), 
  `r_comment` varchar(152))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-49146:9000/user/tmp/csv/region_csv';
  
--创建supplier外表
CREATE EXTERNAL TABLE `supplier`(
  `s_suppkey` int, 
  `s_name` varchar(25), 
  `s_address` varchar(40), 
  `s_nationkey` int, 
  `s_phone` varchar(15), 
  `s_acctbal` double, 
  `s_comment` varchar(101))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-49146:9000/user/tmp/csv/supplier_csv';
```

(2) 将存储格式为 CSV 的 Hive 外部表的数据转移到存储格式为 ORC 的 Hive 表中，以便后续在 StarRocks 和 Trino 中创建存储格式为 ORC 的 Hive 外表进行测试。

```SQL
use tpch_hive_csv；
insert into tpch_hive_orc.customer  select * from customer;
insert into tpch_hive_orc.lineitem  select * from lineitem;
insert into tpch_hive_orc.nation  select * from nation;
insert into tpch_hive_orc.orders  select * from orders;
insert into tpch_hive_orc.part  select * from part;
insert into tpch_hive_orc.partsupp  select * from partsupp;
insert into tpch_hive_orc.region  select * from region;
insert into tpch_hive_orc.supplier  select * from supplier;
```

#### 3. 数据查询

```SQL
use tpch_sr;
--查看当前会话系统变量。
show variables;
--设置并行度为8。
set parallel_fragment_exec_instance_num = 8;

--Q1
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity) as sum_qty,
  sum(l_extendedprice) as sum_base_price,
  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
  avg(l_quantity) as avg_qty,
  avg(l_extendedprice) as avg_price,
  avg(l_discount) as avg_disc,
  count(*) as count_order
from lineitem
where  l_shipdate <= date '1998-12-01'
group by  l_returnflag,  l_linestatus
order by  l_returnflag,  l_linestatus;

--Q2
select  s_acctbal, s_name,  n_name,  p_partkey,  p_mfgr,  s_address,  s_phone,  s_comment
from  part,  partsupp,  supplier,  nation,  region
where
  p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 15
  and p_type like '%BRASS'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'EUROPE'
  and ps_supplycost = (
    select  min(ps_supplycost)
    from  partsupp,  supplier,  nation,  region
    where
      p_partkey = ps_partkey
      and s_suppkey = ps_suppkey
      and s_nationkey = n_nationkey
      and n_regionkey = r_regionkey
      and r_name = 'EUROPE'
  )
order by  s_acctbal desc, n_name,  s_name,  p_partkey;

--Q3
select
  l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority
from
  customer, orders, lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by l_orderkey, o_orderdate, o_shippriority
order by revenue desc, o_orderdate;

--Q4
select  o_orderpriority,  count(*) as order_count
from  orders
where
  o_orderdate >= date '1993-07-01'
  and o_orderdate < date '1993-07-01' + interval '3' month
  and exists (
    select  * from  lineitem
    where  l_orderkey = o_orderkey and l_commitdate < l_receiptdate
  )
group by o_orderpriority
order by o_orderpriority;

--Q5
select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
from customer, orders, lineitem, supplier, nation, region
where
  c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'ASIA'
  and o_orderdate >= date '1994-01-01'
  and o_orderdate < date '1994-01-01' + interval '1' year
group by n_name
order by revenue desc;

--Q6
select
  sum(l_extendedprice * l_discount) as revenue
from
  lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;

--Q7
select supp_nation, cust_nation, l_year, sum(volume) as revenue
from (
    select
      n1.n_name as supp_nation,
      n2.n_name as cust_nation,
      extract(year from l_shipdate) as l_year,
      l_extendedprice * (1 - l_discount) as volume
    from supplier, lineitem, orders, customer, nation n1, nation n2
    where
      s_suppkey = l_suppkey
      and o_orderkey = l_orderkey
      and c_custkey = o_custkey
      and s_nationkey = n1.n_nationkey
      and c_nationkey = n2.n_nationkey
      and (
        (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
        or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
      )
      and l_shipdate between date '1995-01-01' and date '1996-12-31'
  ) as shipping
group by supp_nation, cust_nation, l_year
order by supp_nation, cust_nation, l_year;

--Q8
select
  o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
from
  (
    select
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) as volume,
      n2.n_name as nation
    from part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    where
      p_partkey = l_partkey
      and s_suppkey = l_suppkey
      and l_orderkey = o_orderkey
      and o_custkey = c_custkey
      and c_nationkey = n1.n_nationkey
      and n1.n_regionkey = r_regionkey
      and r_name = 'AMERICA'
      and s_nationkey = n2.n_nationkey
      and o_orderdate between date '1995-01-01' and date '1996-12-31'
      and p_type = 'ECONOMY ANODIZED STEEL'
  ) as all_nations
group by o_year
order by o_year;

--Q9
select nation, o_year, sum(amount) as sum_profit
from
  (
    select
      n_name as nation,
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from part, supplier, lineitem, partsupp, orders, nation
    where
      s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%green%'
  ) as profit
group by nation, o_year
order by nation, o_year desc;

--Q10
select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment
    from customer, orders, lineitem, nation
    where
      c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and o_orderdate >= date '1993-10-01'
      and o_orderdate < date '1993-12-01'
      and l_returnflag = 'R'
      and c_nationkey = n_nationkey
    group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
    order by revenue desc;

--Q11
select ps_partkey, sum(ps_supplycost * ps_availqty) as value
from partsupp, supplier, nation
where
  ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'GERMANY'
group by ps_partkey having
    sum(ps_supplycost * ps_availqty) > (
      select sum(ps_supplycost * ps_availqty) * 0.0001000000
      from partsupp, supplier, nation
      where
        ps_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'GERMANY'
    )
order by value desc;

--Q12
select
  l_shipmode,
  sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
  sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
from orders, lineitem
where
  o_orderkey = l_orderkey
  and l_shipmode in ('MAIL', 'SHIP')
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1994-01-01'
  and l_receiptdate < date '1994-01-01' + interval '1' year
group by l_shipmode
order by l_shipmode;

--Q13
select  c_count,  count(*) as custdist
from (select c_custkey,count(o_orderkey) as c_count
    from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%special%requests%'
    group by  c_custkey
) as c_orders
group by  c_count
order by custdist desc, c_count desc;

--Q14
select
  100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from lineitem, part
where
  l_partkey = p_partkey
  and l_shipdate >= date '1995-09-01'
  and l_shipdate < date '1995-09-01' + interval '1' month;

--Q15
select s_suppkey, s_name, s_address, s_phone, total_revenue
from supplier, revenue0
where
  s_suppkey = supplier_no
  and total_revenue = (
    select max(total_revenue) from revenue0
  )
order by s_suppkey;

--Q16
select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
from partsupp, part
where
  p_partkey = ps_partkey
  and p_brand <> 'Brand#45'
  and p_type not like 'MEDIUM POLISHED%'
  and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
  and ps_suppkey not in (
    select s_suppkey from supplier where s_comment like '%Customer%Complaints%'
  )
group by p_brand, p_type, p_size
order by supplier_cnt desc, p_brand, p_type, p_size;

--Q17
select sum(l_extendedprice) / 7.0 as avg_yearly
from lineitem, part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey
  );

--Q18
select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
from customer, orders, lineitem
where
  o_orderkey in (
    select l_orderkey from lineitem
    group by l_orderkey having
        sum(l_quantity) > 300
  )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
order by o_totalprice desc, o_orderdate;

--Q19
select sum(l_extendedprice* (1 - l_discount)) as revenue
from lineitem, part
where
  (
    p_partkey = l_partkey
    and p_brand = 'Brand#12'
    and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    and l_quantity >= 1 and l_quantity <= 1 + 10
    and p_size between 1 and 5
    and l_shipmode in ('AIR', 'AIR REG')
    and l_shipinstruct = 'DELIVER IN PERSON'
  )
  or
  (
    p_partkey = l_partkey
    and p_brand = 'Brand#23'
    and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    and l_quantity >= 10 and l_quantity <= 10 + 10
    and p_size between 1 and 10
    and l_shipmode in ('AIR', 'AIR REG')
    and l_shipinstruct = 'DELIVER IN PERSON'
  )
  or
  (
    p_partkey = l_partkey
    and p_brand = 'Brand#34'
    and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    and l_quantity >= 20 and l_quantity <= 20 + 10
    and p_size between 1 and 15
    and l_shipmode in ('AIR', 'AIR REG')
    and l_shipinstruct = 'DELIVER IN PERSON'
  );

--Q20
select s_name, s_address
from supplier, nation
where
  s_suppkey in (
    select ps_suppkey
    from partsupp
    where
      ps_partkey in (
        select p_partkey from part
        where p_name like 'forest%'
      )
      and ps_availqty > (
        select 0.5 * sum(l_quantity)
        from lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
      )
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by s_name;

--Q21
select s_name, count(*) as numwait
from  supplier,  lineitem l1,  orders,  nation
where
  s_suppkey = l1.l_suppkey
  and o_orderkey = l1.l_orderkey
  and o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists (
    select      *     from    lineitem l2
    where
      l2.l_orderkey = l1.l_orderkey
      and l2.l_suppkey <> l1.l_suppkey
  )
  and not exists (
    select      *       from    lineitem l3
    where
      l3.l_orderkey = l1.l_orderkey
      and l3.l_suppkey <> l1.l_suppkey
      and l3.l_receiptdate > l3.l_commitdate
  )
  and s_nationkey = n_nationkey
  and n_name = 'SAUDI ARABIA'
group by s_name
order by  numwait desc, s_name;


--Q22
select
  cntrycode,
  count(*) as numcust,
  sum(c_acctbal) as totacctbal
from
  (
    select
      substr(c_phone, 1, 2) as cntrycode,
      c_acctbal
    from
      customer
    where
      substr(c_phone, 1, 2) in
        ('13', '31', '23', '29', '30', '18', '17')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          customer
        where
          c_acctbal > 0.00
          and substr(c_phone, 1, 2) in
            ('13', '31', '23', '29', '30', '18', '17')
      )
      and not exists (
        select
          *
        from
          orders
        where
          o_custkey = c_custkey
      )
  ) as custsale
group by
  cntrycode
order by
  cntrycode;
```
