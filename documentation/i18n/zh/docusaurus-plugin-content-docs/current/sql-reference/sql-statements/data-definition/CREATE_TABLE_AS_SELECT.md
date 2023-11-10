# CREATE TABLE AS SELECT

## 功能

CREATE TABLE AS SELECT（简称 CTAS）语句可用于同步或异步查询原表并基于查询结果创建新表，然后将查询结果插入到新表中。

您可以通过 [SUBMIT TASK](../data-manipulation/SUBMIT_TASK.md) 创建异步 CTAS 任务。

## 语法

- 同步查询原表并基于查询结果创建新表，然后将查询结果插入到新表中。

  ```SQL
  CREATE TABLE [IF NOT EXISTS] [database.]table_name
  [(column_name [, column_name2, ...]]
  [key_desc]
  [COMMENT "table comment"]
  [partition_desc]
  [distribution_desc]
  [PROPERTIES ("key"="value", ...)]AS SELECT query
  [ ... ]
  ```

- 异步查询原表并基于查询结果创建新表，然后将查询结果插入到新表中。

  ```SQL
  SUBMIT [/*+ SET_VAR(key=value) */] TASK [[database.]<task_name>]AS
  CREATE TABLE [IF NOT EXISTS] [database.]table_name
  [(column_name [, column_name2, ...]]
  [key_desc]
  [COMMENT "table comment"]
  [partition_desc]
  [distribution_desc]
  [PROPERTIES ("key"="value", ...)]AS SELECT query
  [ ... ]
  ```

## 参数说明

| **参数**          | **必填** | **描述**                                                     |
| ----------------- | -------- | ------------------------------------------------------------ |
| column_name       | 是       | 新表的列名。您无需指定列类型。StarRocks 会自动选择合适的列类型，并将 FLOAT 和 DOUBLE 转换为 DECIMAL(38,9)；将 CHAR、VARCHAR 和 STRING 转换为 VARCHAR(65533)。 |
| key_desc          | 否       | 语法是 `key_type (<col_name1> [, <col_name2>, ...])`。<br />**参数**：<ul><li>`key_type`：新表的 Key 类型。有效值：`DUPLICATE KEY` 和 `PRIMARY KEY`。默认值：`DUPLICATE KEY`。</li><li> `col_name`：组成 Key 的列。</li></ul>|
| COMMENT           | 否       | 新表注释。                                                   |
| partition_desc    | 否       | 新表的分区方式。如不指定该参数，则默认新表为无分区。更多有关分区的设置，参见 CREATE TABLE。 |
| distribution_desc | 否       | 新表的分桶方式。如不指定该参数，则默认新表的分桶列为使用 CBO 优化器采集的统计信息中基数最高的列，且分桶数量默认为 10。如果 CBO 优化器没有采集基数信息，则默认新表的第一列为分桶列。更多有关分桶的设置，参见 CREATE TABLE。 |
| Properties        | 否       | 新表的属性。                                                 |
| AS SELECT query   | 是       | 查询结果。该参数支持如下值： 列。比如 `... AS SELECT a, b, c FROM table_a;`，其中 `a`、`b` 和 `c` 为原表的列名。如果您没有为新表指定列名，那么新表的列名也为 `a`、`b` 和 `c`。 表达式。比如 `... AS SELECT a+1 AS x, b+2 AS y, c*c AS z FROM table_a;`，其中 `a+1`、`b+2` 和 `c*c` 为原表的列名，`x`、`y` 和 `z` 为新表的列名。 说明： 新表的列数需要与 `AS SELECT query` 中指定的列数保持一致。 建议您为新表的列设置具有业务意义的列名，便于后续识别使用。 |

## 注意事项

- 使用 CTAS 语句创建的新表需满足如下条件：
  - `ENGINE` 类型为 `OLAP`。

  - 表类型默认为明细表，您也可以在 `key_desc` 中指定为主键表。

  - 排序列为前三列且这三列类型的存储空间不能超过 36 个字节。

- CTAS 语句不支持为新表设置索引。

- 如果 CTAS 语句由于 FE 重启或其他原因执行失败，可能会发生如下情况：
  - 新表创建成功，但表中没有数据。

  - 新表创建失败。

- 新表创建后，如果存在多种方式（比如 Insert Into）将数据插入到新表中，那么最先执行完插入操作的即最先将数据插入到新表中。

- 新表创建成功后，您需要手动授予用户对该表的权限。参见 [表权限](../../../administration/privilege_item.md#表权限-table) 和 [GRANT](../account-management/GRANT.md)。
- 当异步查询原表并基于查询结果创建新表时，如果不指定 Task 名称，那么 StarRocks 会自动生成一个 Task 名称。

## 示例

示例一：同步查询原表 `order` 并根据查询结果创建新表 `order_new`，然后将查询结果插入到新表中。

```SQL
CREATE TABLE order_new
AS SELECT * FROM order;
```

示例二：同步查询原表 `order`中的 `k1`、`k2` 和 `k3` 列并根据查询结果创建新表 `order_new`，然后将查询结果插入到新表中，并指定新表中列的名称为 `a`、`b` 和 `c`。

```SQL
CREATE TABLE order_new (a, b, c)
AS SELECT k1, k2, k3 FROM order;
```

或

```SQL
CREATE TABLE order_new
AS SELECT k1 AS a, k2 AS b, k3 AS c FROM order;
```

示例三：同步查询原表 `employee`中 `salary` 列的最大值并根据查询结果创建新表 `employee_new` ，然后将查询结果插入到新表中，并指定新表中列名为 `salary_new`。

```SQL
CREATE TABLE employee_new
AS SELECT MAX(salary) AS salary_max FROM employee;
```

插入完成后，查询新表 `employee_new`。

```SQL
SELECT * FROM employee_new;

+------------+
| salary_max |
+------------+
|   10000    |
+------------+
```

示例四：使用 CTAS 创建一张主键表。需要注意的是，主键表中的数据行数可能会比查询结果中的数据行数少。这是因为主键表只存储具有相同主键的一组数据行中最新的一条数据行。

```SQL
CREATE TABLE employee_new
PRIMARY KEY(order_id)
AS SELECT
    order_list.order_id,
    sum(goods.price) as total
FROM order_list INNER JOIN goods ON goods.item_id1 = order_list.item_id2
GROUP BY order_id;
```

示例五：同步查询四张原表 `lineorder`、`customer`、`supplier` 和 `part` 并根据查询结果创建新表 `lineorder_flat`，然后将查询结果插入到新表中，并指定新表的分区和分桶方式。

```SQL
CREATE TABLE lineorder_flat
PARTITION BY RANGE (`LO_ORDERDATE`)
(
    START ("1993-01-01") END ("1999-01-01") EVERY (INTERVAL 1 YEAR)
)
DISTRIBUTED BY HASH (`LO_ORDERKEY`) AS SELECT
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

示例六：异步查询原表 `order_detail` 并根据查询结果创建新表 `order_statistics`，然后将查询结果插入到新表中。

```Plain_Text
SUBMIT TASK AS CREATE TABLE order_statistics AS SELECT COUNT(*) as count FROM order_detail;

+-------------------------------------------+-----------+
| TaskName                                  | Status    |
+-------------------------------------------+-----------+
| ctas-df6f7930-e7c9-11ec-abac-bab8ee315bf2 | SUBMITTED |
+-------------------------------------------+-----------+
```

查询 Task 的信息。

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;

-- Task 信息如下

TASK_NAME: ctas-df6f7930-e7c9-11ec-abac-bab8ee315bf2
CREATE_TIME: 2022-06-14 14:07:06
SCHEDULE: MANUAL
DATABASE: default_cluster:test
DEFINITION: CREATE TABLE order_statistics AS SELECT COUNT(*) as cnt FROM order_detail
EXPIRE_TIME: 2022-06-17 14:07:06
```

查询 TaskRun 的状态。

```SQL
SELECT * FROM INFORMATION_SCHEMA.task_runs;

--TaskRun 的状态如下

QUERY_ID: 37bd2b63-eba8-11ec-8d41-bab8ee315bf2
TASK_NAME: ctas-df6f7930-e7c9-11ec-abac-bab8ee315bf2
CREATE_TIME: 2022-06-14 14:07:06
FINISH_TIME: 2022-06-14 14:07:07
STATE: SUCCESS
DATABASE: 
DEFINITION: CREATE TABLE order_statistics AS SELECT COUNT(*) as cnt FROM order_detail
EXPIRE_TIME: 2022-06-17 14:07:06
ERROR_CODE: 0
ERROR_MESSAGE: NULL
```

当 TaskRun 的状态为 `SUCCESS` 时即可查询新表。

```SQL
SELECT * FROM order_statistics;
```
