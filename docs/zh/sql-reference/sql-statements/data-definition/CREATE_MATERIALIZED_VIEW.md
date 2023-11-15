# CREATE MATERIALIZED VIEW

## 功能

创建物化视图。创建物化视图是一个异步的操作。CREATE MATERIALIZED VIEW 命令执行成功即代表创建物化视图的任务提交成功。您可以通过 [SHOW ALTER MATERIALIZED VIEW](../data-manipulation/SHOW_ALTER_MATERIALIZED_VIEW.md) 命令查看当前数据库中物化视图的构建状态。关于物化视图适用的场景请参考 [物化视图](../../../using_starrocks/Materialized_view.md)。

> **注意**
>
> 只有拥有基表所在数据库 `CREATE_PRIV` 权限的用户才可以创建物化视图。

StarRocks 自 v2.4 起支持多表物化视图。多表物化视图与先前版本中的单表物化视图区别主要体现在以下方面：

|                  | **支持异步及手动刷新** | **支持聚合列** | **支持修改分区分片** | **JOIN、WHERE、GROUP BY 子句** |
| ---------------- | ------------------- | ------------ | ------------------ | ------------------------------ |
| **单表物化视图** | 否（v2.3 及之前版本物化视图仅支持同步刷新方式） | 否  | 否 | 否 |
| **多表物化视图** | 是 | 是  | 是 | 是   |

## 语法

```SQL
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [database.]mv_name
[distribution_desc]
[REFRESH refresh_scheme_desc]
[partition_expression]
[COMMENT ""]
[PROPERTIES ("key"="value", ...)]
AS (query)
```

## 参数

**mv_name**（必填）

物化视图的名称。命名要求如下：

- 必须由字母（a-z 或 A-Z）、数字（0-9）或下划线（_）组成，且只能以字母开头。
- 总长度不能超过 64 个字符。

> **注意**
>
> 同一张基表可以创建多个物化视图，但同一数据库内的物化视图名称不可重复。

**query**（必填）

创建物化视图的查询语句，其结果即为物化视图中的数据。语法如下：

```SQL
SELECT select_expr[, select_expr ...]
[GROUP BY column_name[, column_name ...]]
[ORDER BY column_name[, column_name ...]]
```

- select_expr（必填）

  查询语句中所有的列，也即物化视图的 schema 中所有的列。该参数支持如下值：

  - 单列或聚合列：形如 `SELECT a, b, c FROM table_a` （适用于创建单表物化视图）或 `SELECT table_a.a, table_a.b, table_b.d,`（仅适用于 2.4 版本以上创建多表物化视图），其中 `a`、`b`、`c` 和 `d` 为基表的列名。如果您没有为物化视图指定列名，那么物化视图的列名也为 `a`、`b`、`c` 和 `d`。
  - 表达式：形如 `SELECT a+1 AS x, b+2 AS y, c*c AS z FROM table_a`，其中 `a+1`、`b+2` 和 `c*c` 为包含基表的列名的表达式，`x`、`y` 和 `z` 为物化视图的列名。

  > **注意**
  >
  > - 如果查询语句中的列不是简单列，则必须在物化视图中指定新列名。
  > - 该参数至少需包含一个单列，且所有涉及到的列，均只能出现一次。

- GROUP BY（选填）

  物化视图的分组列。如不指定该参数，则默认不对数据进行分组。

- ORDER BY（选填）

  物化视图的排序列。

  - 排序列的声明顺序必须和 `select_expr` 中列声明顺序一致。
  - 如果不指定排序列，则系统根据规则自动补充排序列。 如果物化视图是聚合类型，则所有的分组列自动补充为排序列。 如果物化视图是非聚合类型，则前 36 个字节自动补充为排序列。如果自动补充的排序个数小于 3 个，则前三个作为排序列。
  - 如果查询语句中包含分组列，则排序列必须和分组列一致。

**distribution_desc**（建立异步多表物化视图时为**必填**）

物化视图的分桶方式，形如 `DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]`。

**refresh_scheme_desc**（选填）

物化视图的刷新方式。该参数支持如下值：

- `ASYNC`：异步的刷新方式。您可以为该方式指定刷新开始时间、刷新间隔或者导入触发刷新方式。刷新间隔仅支持：`DAY`、`HOUR`、`MINUTE` 以及 `SECOND`。
- `MANUAL`：手动的刷新方式。
- 如果不指定该参数，则默认使用 MANUAL 方式。

**partition_expression**（选填）

物化视图的分区表达式。目前仅支持在创建物化视图时使用一个分区表达式。该参数支持如下值：

- 列：形如 `PARTITION BY dt`，表示按照 `dt` 列进行分区。
- date_trunc 函数：形如 PARTITION BY date_trunc("MONTH", 'dt')，表示将 `dt` 列截断至以月为单位进行分区。date_trunc 函数支持截断的单位包括 `YEAR`、`MONTH`、`DAY`、`HOUR` 以及 `MINUTE`。
- 如不指定该参数，则默认物化视图为无分区。

**COMMENT**（选填）

物化视图的注释。

**PROPERTIES**（选填）

物化视图的属性。

- `replication_num`：创建物化视图副本数量。
- `storage_medium`：存储介质类型。

### 支持数据类型

- 基于 StarRocks 内部数据目录（Default Catalog）创建的异步物化视图支持以下数据类型：

  - DATE
  - DATETIME
  - CHAR
  - VARCHAR
  - BOOLEAN
  - TINYINT
  - SMALLINT
  - INT
  - BIGINT
  - LARGEINT
  - FLOAT
  - DOUBLE
  - DECIMAL
  - ARRAY
  - JSON
  - BITMAP
  - HLL
  - PERCENTILE

> **说明**
>
> 自 v2.4.5 起支持 BITMAP、HLL 以及 PERCENTILE。

### 聚合函数匹配关系

使用物化视图搜索时，原始查询语句将会被自动改写并用于查询物化视图中保存的中间结果。下表展示了原始查询聚合函数和构建物化视图用到的聚合函数的匹配关系。您可以根据您的业务场景选择对应的聚合函数构建物化视图。

| **原始查询聚合函数**                                   | **物化视图构建聚合函数** |
| ------------------------------------------------------ | ------------------------ |
| sum                                                    | sum                      |
| min                                                    | min                      |
| max                                                    | max                      |
| count                                                  | count                    |
| bitmap_union, bitmap_union_count, count(distinct)      | bitmap_union             |
| hll_raw_agg, hll_union_agg, ndv, approx_count_distinct | hll_union                |

## 注意事项

- 当前版本暂时不支持同时创建多个物化视图。仅当当前创建任务完成时，方可执行下一个创建任务。

- 关于同步物化视图：

  - 同步物化视图仅支持单列聚合函数，不支持形如 `sum(a+b)` 的查询语句。
  - 同步物化视图仅支持对同一列数据使用一种聚合函数，不支持形如 `select sum(a), min(a) from table` 的查询语句。
  - 同步物化视图中使用聚合函数需要与 GROUP BY 语句一起使用，且 SELECT 的列中至少包含一个分组列。
  - 同步物化视图创建语句不支持 JOIN、WHERE 以及 GROUP BY 的 HAVING 子句。
  - 使用 ALTER TABLE DROP COLUMN 删除基表中特定列时，需要保证该基表所有同步物化视图中不包含被删除列，否则无法进行删除操作。如果必须删除该列，则需要将所有包含该列的同步物化视图删除，然后进行删除列操作。
  - 为一张表创建过多的同步物化视图会影响导入的效率。导入数据时，同步物化视图和基表数据将同步更新，如果一张基表包含 n 个物化视图，向基表导入数据时，其导入效率大约等同于导入 n 张表，数据导入的速度会变慢。

## 示例

以下示例基于下列基表。

```SQL
CREATE TABLE `lineorder` (
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
PARTITION BY RANGE(`lo_orderdate`)
(PARTITION p1 VALUES [("-2147483648"), ("19930101")),
PARTITION p2 VALUES [("19930101"), ("19940101")),
PARTITION p3 VALUES [("19940101"), ("19950101")),
PARTITION p4 VALUES [("19950101"), ("19960101")),
PARTITION p5 VALUES [("19960101"), ("19970101")),
PARTITION p6 VALUES [("19970101"), ("19980101")),
PARTITION p7 VALUES [("19980101"), ("19990101")))
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 48;

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
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 12;

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
DISTRIBUTED BY HASH(`d_datekey`) BUCKETS 1;

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
DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12;

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
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 12;

create table orders ( 
    dt date NOT NULL, 
    order_id bigint NOT NULL, 
    user_id int NOT NULL, 
    merchant_id int NOT NULL, 
    good_id int NOT NULL, 
    good_name string NOT NULL, 
    price int NOT NULL, 
    cnt int NOT NULL, 
    revenue int NOT NULL, 
    state tinyint NOT NULL 
) 
PRIMARY KEY (dt, order_id) 
PARTITION BY RANGE(`dt`) 
( PARTITION p20210820 VALUES [('2021-08-20'), ('2021-08-21')), 
PARTITION p20210821 VALUES [('2021-08-21'), ('2021-08-22')) ) 
DISTRIBUTED BY HASH(order_id) BUCKETS 4 
PROPERTIES (
    "replication_num" = "3", 
    "enable_persistent_index" = "true"
);
```

示例一：从源表创建非分区物化视图

```SQL
CREATE MATERIALIZED VIEW lo_mv1
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 10
REFRESH ASYNC
AS
select
    lo_orderkey, 
    lo_custkey, 
    sum(lo_quantity) as total_quantity, 
    sum(lo_revenue) as total_revenue, 
    count(lo_shipmode) as shipmode_count
from lineorder 
group by lo_orderkey, lo_custkey 
order by lo_orderkey;
```

示例二：从源表创建分区物化视图

```SQL
CREATE MATERIALIZED VIEW lo_mv2
PARTITION BY `lo_orderdate`
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 10
REFRESH ASYNC START('2023-07-01 10:00:00') EVERY (interval 1 day)
AS
select
    lo_orderkey,
    lo_orderdate,
    lo_custkey, 
    sum(lo_quantity) as total_quantity, 
    sum(lo_revenue) as total_revenue, 
    count(lo_shipmode) as shipmode_count
from lineorder 
group by lo_orderkey, lo_orderdate, lo_custkey
order by lo_orderkey;

# 使用 date_trunc 函数将 `dt` 列截断至以月为单位进行分区。
CREATE MATERIALIZED VIEW order_mv1
PARTITION BY date_trunc('month', `dt`)
DISTRIBUTED BY HASH(`order_id`) BUCKETS 10
REFRESH ASYNC START('2023-07-01 10:00:00') EVERY (interval 1 day)
AS
select
    dt,
    order_id,
    user_id,
    sum(cnt) as total_cnt,
    sum(revenue) as total_revenue, 
    count(state) as state_count
from orders
group by dt, order_id, user_id;
```

示例三：创建多表物化视图

```SQL
CREATE MATERIALIZED VIEW flat_lineorder
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 48
REFRESH MANUAL
AS
SELECT
    l.LO_ORDERKEY AS LO_ORDERKEY,
    l.LO_LINENUMBER AS LO_LINENUMBER,
    l.LO_CUSTKEY AS LO_CUSTKEY,
    l.LO_PARTKEY AS LO_PARTKEY,
    l.LO_SUPPKEY AS LO_SUPPKEY,
    l.LO_ORDERDATE AS LO_ORDERDATE,
    l.LO_ORDERPRIORITY AS LO_ORDERPRIORITY,
    l.LO_SHIPPRIORITY AS LO_SHIPPRIORITY,
    l.LO_QUANTITY AS LO_QUANTITY,
    l.LO_EXTENDEDPRICE AS LO_EXTENDEDPRICE,
    l.LO_ORDTOTALPRICE AS LO_ORDTOTALPRICE,
    l.LO_DISCOUNT AS LO_DISCOUNT,
    l.LO_REVENUE AS LO_REVENUE,
    l.LO_SUPPLYCOST AS LO_SUPPLYCOST,
    l.LO_TAX AS LO_TAX,
    l.LO_COMMITDATE AS LO_COMMITDATE,
    l.LO_SHIPMODE AS LO_SHIPMODE,
    c.C_NAME AS C_NAME,
    c.C_ADDRESS AS C_ADDRESS,
    c.C_CITY AS C_CITY,
    c.C_NATION AS C_NATION,
    c.C_REGION AS C_REGION,
    c.C_PHONE AS C_PHONE,
    c.C_MKTSEGMENT AS C_MKTSEGMENT,
    s.S_NAME AS S_NAME,
    s.S_ADDRESS AS S_ADDRESS,
    s.S_CITY AS S_CITY,
    s.S_NATION AS S_NATION,
    s.S_REGION AS S_REGION,
    s.S_PHONE AS S_PHONE,
    p.P_NAME AS P_NAME,
    p.P_MFGR AS P_MFGR,
    p.P_CATEGORY AS P_CATEGORY,
    p.P_BRAND AS P_BRAND,
    p.P_COLOR AS P_COLOR,
    p.P_TYPE AS P_TYPE,
    p.P_SIZE AS P_SIZE,
    p.P_CONTAINER AS P_CONTAINER FROM lineorder AS l 
INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY
INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY
INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY;
```

示例四：创建单表物化视图

基表结构为：

```sql
mysql> desc duplicate_table;
+-------+--------+------+------+---------+-------+
| Field | Type   | Null | Key  | Default | Extra |
+-------+--------+------+------+---------+-------+
| k1    | INT    | Yes  | true | N/A     |       |
| k2    | INT    | Yes  | true | N/A     |       |
| k3    | BIGINT | Yes  | true | N/A     |       |
| k4    | BIGINT | Yes  | true | N/A     |       |
+-------+--------+------+------+---------+-------+
```

1. 创建一个仅包含原始表 （k1，k2）列的物化视图。

    ```sql
    create materialized view k1_k2 as
    select k1, k2 from duplicate_table;
    ```

    物化视图的 schema 如下图，物化视图仅包含两列 k1、k2 且不带任何聚合。

    ```sql
    +-----------------+-------+--------+------+------+---------+-------+
    | IndexName       | Field | Type   | Null | Key  | Default | Extra |
    +-----------------+-------+--------+------+------+---------+-------+
    | k1_k2           | k1    | INT    | Yes  | true | N/A     |       |
    |                 | k2    | INT    | Yes  | true | N/A     |       |
    +-----------------+-------+--------+------+------+---------+-------+
    ```

2. 创建一个以 k2 为排序列的物化视图。

    ```sql
    create materialized view k2_order as
    select k2, k1 from duplicate_table order by k2;
    ```

    物化视图的 schema 如下图，物化视图仅包含两列 k2、k1，其中 k2 列为排序列，不带任何聚合。

    ```sql
    +-----------------+-------+--------+------+-------+---------+-------+
    | IndexName       | Field | Type   | Null | Key   | Default | Extra |
    +-----------------+-------+--------+------+-------+---------+-------+
    | k2_order        | k2    | INT    | Yes  | true  | N/A     |       |
    |                 | k1    | INT    | Yes  | false | N/A     | NONE  |
    +-----------------+-------+--------+------+-------+---------+-------+
    ```

3. 创建一个以 k1，k2 分组，k3 列为 SUM 聚合的物化视图。

    ```sql
    create materialized view k1_k2_sumk3 as
    select k1, k2, sum(k3) from duplicate_table group by k1, k2;
    ```

    物化视图的 schema 如下图，物化视图包含两列 k1、k2，sum(k3) 其中 k1、k2 为分组列，sum(k3) 为根据 k1、k2 分组后的 k3 列的求和值。

    由于物化视图没有声明排序列，且物化视图带聚合数据，系统默认补充分组列 k1、k2 为排序列。

    ```sql
    +-----------------+-------+--------+------+-------+---------+-------+
    | IndexName       | Field | Type   | Null | Key   | Default | Extra |
    +-----------------+-------+--------+------+-------+---------+-------+
    | k1_k2_sumk3     | k1    | INT    | Yes  | true  | N/A     |       |
    |                 | k2    | INT    | Yes  | true  | N/A     |       |
    |                 | k3    | BIGINT | Yes  | false | N/A     | SUM   |
    +-----------------+-------+--------+------+-------+---------+-------+
    ```

4. 创建一个去除重复行的物化视图。

    ```sql
    create materialized view deduplicate as
    select k1, k2, k3, k4 from duplicate_table group by k1, k2, k3, k4;
    ```

    物化视图 schema 如下图，物化视图包含 k1、k2、k3、k4 列，且不存在重复行。

    ```sql
    +-----------------+-------+--------+------+-------+---------+-------+
    | IndexName       | Field | Type   | Null | Key   | Default | Extra |
    +-----------------+-------+--------+------+-------+---------+-------+
    | deduplicate     | k1    | INT    | Yes  | true  | N/A     |       |
    |                 | k2    | INT    | Yes  | true  | N/A     |       |
    |                 | k3    | BIGINT | Yes  | true  | N/A     |       |
    |                 | k4    | BIGINT | Yes  | true  | N/A     |       |
    +-----------------+-------+--------+------+-------+---------+-------+

    ```

5. 创建一个不声明排序列的非聚合型物化视图。

    all_type_table 的 schema 如下：

    ```sql
    +-------+--------------+------+-------+---------+-------+
    | Field | Type         | Null | Key   | Default | Extra |
    +-------+--------------+------+-------+---------+-------+
    | k1    | TINYINT      | Yes  | true  | N/A     |       |
    | k2    | SMALLINT     | Yes  | true  | N/A     |       |
    | k3    | INT          | Yes  | true  | N/A     |       |
    | k4    | BIGINT       | Yes  | true  | N/A     |       |
    | k5    | DECIMAL(9,0) | Yes  | true  | N/A     |       |
    | k6    | DOUBLE       | Yes  | false | N/A     | NONE  |
    | k7    | VARCHAR(20)  | Yes  | false | N/A     | NONE  |
    +-------+--------------+------+-------+---------+-------+
    ```

    物化视图包含 k3、k4、k5、k6、k7 列，且不声明排序列，则创建语句如下：

    ```sql
    create materialized view mv_1 as
    select k3, k4, k5, k6, k7 from all_type_table;
    ```

    系统默认补充的排序列为 k3、k4、k5 三列。这三列类型的字节数之和为 4(INT) + 8(BIGINT) + 16(DECIMAL) = 28 < 36。所以补充的是这三列作为排序列。
    物化视图的 schema 如下，可以看到其中 k3、k4、k5 列的 key 字段为 true，也就是排序列。k6、k7 列的 key 字段为 false，也就是非排序列。

    ```sql
    +----------------+-------+--------------+------+-------+---------+-------+
    | IndexName      | Field | Type         | Null | Key   | Default | Extra |
    +----------------+-------+--------------+------+-------+---------+-------+
    | mv_1           | k3    | INT          | Yes  | true  | N/A     |       |
    |                | k4    | BIGINT       | Yes  | true  | N/A     |       |
    |                | k5    | DECIMAL(9,0) | Yes  | true  | N/A     |       |
    |                | k6    | DOUBLE       | Yes  | false | N/A     | NONE  |
    |                | k7    | VARCHAR(20)  | Yes  | false | N/A     | NONE  |
    +----------------+-------+--------------+------+-------+---------+-------+
    ```
