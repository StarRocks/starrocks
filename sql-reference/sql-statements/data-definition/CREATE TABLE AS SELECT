# CREATE TABLE AS SELECT

## Description  

CREATE TABLE AS SELECT（简称 CTAS）可以查询原表，并基于查询结果，创建一个新表并且导入数据。

## Syntax

```SQL
CREATE TABLE [IF NOT EXISTS] [database.]table_name

[(column_name [, column_name2, ...]]

[COMMENT "table comment"]

[partition_desc]

[distribution_desc]

[PROPERTIES ("key"="value", ...)]

AS SELECT query

  [ ... ]
```

## Parameters

### 建表部分

| 参数             | 说明                                                         |
| ---------------- | ------------------------------------------------------------ |
| column_name      | 列名。您无需传入列类型，StarRocks 会自动选择合适的类型，并将 FLOAT 或 DOUBLE 转换为 DECIMAL(38,9)，CHAR、VARCHAR、STRING 转换为 VARCHAR(1048576)。 |
| COMMENT          | 表注释。                                                     |
| partition_desc   | 分区方式。更多说明，请参见 [StarRocks 数据分布](https://docs.starrocks.com/zh-cn/main/table_design/Data_distribution#starrocks数据分布)。如果不填写，则默认为无分区。 |
| distribution_des | 分桶方式。更多说明，请参见 [StarRocks 数据分布](https://docs.starrocks.com/zh-cn/main/table_design/Data_distribution#starrocks数据分布)。如果不填写，则默认分桶键为CBO统计信息中最高基数的列，分桶数量为10。如果CBO中没有相关统计信息，则默认分桶键为第一列。 |
| properties       | 新表的附带属性。更多说明，请参见 [PROPERTIES](https://docs.starrocks.com/zh-cn/main/sql-reference/sql-statements/data-definition/CREATE TABLE#syntax)。目前CTA仅支持创建ENGINE类型为OLAP的表。 |

### 查询部分

支持`AS SELECT query` 直接指定具体列，比如 `as select a, b, c from table_a;` ，则新建表的列名为a，b， c 。

支持`AS SELECT query` 使用表达式，并且建议您为新表的列设置具有业务意义的别名，便于后续识别，比如`select a+1 as x, b+2 as y, c*c as z from table_a;`，设置新列名为x，y，z。

## Usage Notes

- 仅支持 ENGINE 类型为 OLAP ；数据模型为 Duplicate Key，排序键为前三列（数据类型的存储空间不能超过36字节）。
- 暂不支持设置索引。
- 暂无法对 CTAS 提供事务保证。
- 创建成功后，您需要手动授予用户权限。

## Examples

示例一：复制原表 order，创建一个新表 order_new。

```SQL
create table order_new as select * from order;
```

示例二：根据原表 order 的列 k1、k2 和 k3，创建一个新表 order_new，并指定列名为 a、b 和 c。

> 指定的列数需要与 AS SELECT *query* 的列数保持一致。

```SQL
create table order_new a, b, c as select k1, k2, k3 from order;
```

```SQL
create table order_new as select k1 as a, k2 as b, k3 as c from order;
```

示例三：AS SELECT *query*使用表达式，根据表达式结果，创建一个新表，并重新指定列名。

> 建议您为新表的列名设置具有业务意义的别名，便于后续识别。

```SQL
--根据原表 employee 的列 salary 计算出最大值，并根据结果，创建一个新表 employee_new 并指定新列名为 salary_new 。

create table employee_new as select max(salary) as salary_max from employee;

--查询新表 employee_new 。

select * from employee_new;

+------------+
| salary_max |
+------------+
|   10000    |
+------------+
```

示例四：根据三张原表 lineorder、customer、supplier 和 part，创建一个新表 lineorder_flat，并且调整分区和分桶方式。

```SQL
CREATE TABLE lineorder_flat

PARTITION BY RANGE(`LO_ORDERDATE`)(

START ("1993-01-01") END ("1999-01-01") EVERY (INTERVAL 1 YEAR)

)

DISTRIBUTED BY HASH(`LO_ORDERKEY`) BUCKETS 120 

AS SELECT

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

    p.P_CONTAINER AS P_CONTAINER

FROM lineorder AS l

INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY

INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY

INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY;
```
