---
displayed_sidebar: docs
keywords: ['CTAS']
---

# CREATE TABLE AS SELECT

## Description

You can use the CREATE TABLE AS SELECT (CTAS) statement to synchronously or asynchronously query a table and create a new table based on the query result, and then insert the query result into the new table.

You can submit an asynchronous CTAS task using [SUBMIT TASK](../loading_unloading/ETL/SUBMIT_TASK.md).

## Syntax

- Synchronously query a table and create a new table based on the query result, and then insert the query result into the new table.

  ```SQL
  CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [database.]table_name
  [column_name1 [, column_name2, ...]]
  [index_definition1 [, index_definition2, ...]]
  [key_desc]
  [COMMENT "table comment"]
  [partition_desc]
  [distribution_desc]
  [ORDER BY (column_name1 [, column_name2, ...])]
  [PROPERTIES ("key"="value", ...)]
  AS SELECT query
  [ ... ]
  ```

- Asynchronously query a table and create a new table based on the query result, and then insert the query result into the new table.

  ```SQL
  SUBMIT [/*+ SET_VAR(key=value) */] TASK [[database.]<task_name>]AS
  CREATE TABLE [IF NOT EXISTS] [database.]table_name
  [column_name1 [, column_name2, ...]]
  [index_definition1 [, index_definition2, ...]]
  [key_desc]
  [COMMENT "table comment"]
  [partition_desc]
  [distribution_desc]
  [ORDER BY (column_name1 [, column_name2, ...])]
  [PROPERTIES ("key"="value", ...)] AS SELECT query
  [ ... ]
  ```

## Parameters

| **Parameter**     | **Required** | **Description**                                              |
| ----------------- | ------------ | ------------------------------------------------------------ |
| TEMPORARY         | No           | Creates a temporary table. From v3.3.1, StarRocks supports creating temporary tables in the Default Catalog. For more information, see [Temporary Table](../../../table_design/StarRocks_table_design.md#temporary-table). Currently, StarRocks does not support creating temporary tables with asynchronous tasks by using SUBMIT TASK. |
| column_name       | No          | The name of a column in the new table. You do not need to specify the data type for the column. StarRocks automatically specifies an appropriate data type for the column. StarRocks converts FLOAT and DOUBLE data into DECIMAL(38,9) data. StarRocks also converts CHAR, VARCHAR, and STRING data into VARCHAR(65533) data. |
| index_definition| No          | Since v3.1.8, a bitmap index can be created for the new table. The syntax is `INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] COMMENT 'xxxxxx'`. For more information about parameter descriptions and usage notes, see [Bitmap indexes](../../../table_design/indexes/Bitmap_index.md). |
| key_desc          | No           | The syntax is `key_type ( <col_name1> [, <col_name2> , ...])`.<br />**Parameters**:<ul><li>`key_type`: [the key type of the new table](../../../table_design/table_types/table_types.md). Valid values: `DUPLICATE KEY` and `PRIMARY KEY`. Default value: `DUPLICATE KEY`.</li><li> `col_name`: the column to form the key.</li></ul> |
| COMMENT           | No           | The comment of the new table.                                |
| partition_desc    | No           | The partitioning method of the new table. By default, if you do not specify this parameter, the new table has no partition. For more information about partitioning, see [CREATE TABLE](./CREATE_TABLE.md#partition_desc). |
| distribution_desc | No           | The bucketing method of the new table. If you do not specify this parameter, the bucket column defaults to the column with the highest cardinality collected by the cost-based optimizer (CBO). The number of buckets defaults to 10. If the CBO does not collect information about the cardinality, the bucket column defaults to the first column in the new table. For more information about bucketing, see [CREATE TABLE](./CREATE_TABLE.md#distribution_desc). |
| ORDER BY | No | Since v3.1.8, a sort key can be specified for the new table if the new table is a Primary Key table. The sort key can be a combination of any columns. A Primary Key table is a table for which `PRIMARY KEY (xxx)` is specified at table creation.|
| Properties        | No           | The properties of the new table.                             |
| AS SELECT query   | Yes          | The query result.  You can specify columns in `... AS SELECT query`, for example, `... AS SELECT a, b, c FROM table_a;`. In this example, `a`, `b`, and `c` indicates the column names of the table that is queried. If you do not specify the column names of the new table, the column names of the new table are also `a`, `b`, and `c`. You can specify expressions in `... AS SELECT query`, for example, `... AS SELECT a+1 AS x, b+2 AS y, c*c AS z FROM table_a;`. In this example, `a+1`, `b+2`, and `c*c` indicates the column names of the table that is queried, and `x`, `y`, and `z` indicates the column names of the new table. Note:  The number of columns in the new table need to be the same as the number of the columns specified in the SELECT statement . We recommend that you use column names that are easy to identify. |

## Usage notes

- The CTAS statement can only create a new table that meets the following requirements:
  - `ENGINE` is `OLAP`.

  - The table is a Duplicate Key table by default. You can also specify it as a Primary Key table in `key_desc`.

  - The sort keys are the first three columns, and the storage space of the data types of these three columns does not exceed 36 bytes.

- The CTAS statement does not support setting indexes for a newly created table.

- If the CTAS statement fails to execute due to reasons, such as an FE restart, one of the following issues may occur:
  - A new table is created successfully but does not contain data.

  - A new table fails to be created.

- After a new table is created, if multiple methods (such as INSERT INTO) are used to insert data into the new table, the method that completes the INSERT operation first will insert its data into the new table.

- After a new table is created, you need to manually grant permissions on this table to users.

- If you do not specify a name for a Task when you asynchronously query a table and create a new table based on the query result, StarRocks automatically generates a name for the Task.

## Examples

Example 1: Synchronously query a table `order` and create a new table `order_new` based on the query result, and then insert the query result into the new table.

```SQL
CREATE TABLE order_new
AS SELECT * FROM order;
```

Example 2: Synchronously query the `k1`, `k2`, and `k3` columns in the table `order` and create a new table `order_new` based on the query result, and then insert the query result into the new table. Additionally, set the column names of the new table to `a`, `b`, and `c`.

```SQL
CREATE TABLE order_new (a, b, c)
AS SELECT k1, k2, k3 FROM order;
```

or

```SQL
CREATE TABLE order_new
AS SELECT k1 AS a, k2 AS b, k3 AS c FROM order;
```

Example 3: Synchronously query the largest value of the `salary` column in the table `employee` and create a new table `employee_new` based on the query result, and then insert the query result into the new table. Additionally, set the column name of the new table to `salary_max`.

```SQL
CREATE TABLE employee_new
AS SELECT MAX(salary) AS salary_max FROM employee;
```

After data is inserted, query the new table.

```SQL
SELECT * FROM employee_new;

+------------+
| salary_max |
+------------+
|   10000    |
+------------+
```

Example 4: Synchronously query the `customer_id` and `first_name` columns in the table `customers` and create a new table `customers_new` based on the query result, and then insert the query result into the new table. Additionally, set the column names of the new table to `customer_id_new` and `first_name_new`. Also, build a bitmap index for the column `customer_id_new` in the new table.

```SQL
CREATE TABLE customers_new 
(   customer_id_new,
    first_name_new,
    INDEX idx_bitmap_customer_id (customer_id_new) USING BITMAP
) 
AS SELECT customer_id,first_name FROM customers;
```

Example 5: Synchronously query a table `customers` and create a new table `customers_new` based on the query result, and then insert the query result into the new table. Additionally, specify the new table as a Primary Key table, and specify its sort key as `first_name` and `last_name`.

```SQL
CREATE TABLE customers_pk
PRIMARY KEY (customer_id)
ORDER BY (first_name,last_name)
AS SELECT  * FROM customers;
```

Example 6: Use CTAS to create a Primary Key table. Note that the number of data rows in the Primary Key table may be less than that in the query result. It is because the [Primary Key](../../../table_design/table_types/primary_key_table.md) table only stores the most recent data row among a group of rows that have the same primary key.

```SQL
CREATE TABLE employee_new
PRIMARY KEY(order_id)
AS SELECT
    order_list.order_id,
    sum(goods.price) as total
FROM order_list INNER JOIN goods ON goods.item_id1 = order_list.item_id2
GROUP BY order_id;
```

Example 7: Synchronously query four tables, including `lineorder`, `customer`, `supplier`, and `part` and create a new table `lineorder_flat` based on the query result, and then insert the query result to the new table. Additionally, specify the partitioning method and bucketing method for the new table.

```SQL
CREATE TABLE lineorder_flat
PARTITION BY RANGE(`LO_ORDERDATE`)
(
    START ("1993-01-01") END ("1999-01-01") EVERY (INTERVAL 1 YEAR)
)
DISTRIBUTED BY HASH(`LO_ORDERKEY`) AS SELECT
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

Example 8: Asynchronously query the table `order_detail` and create a new table `order_statistics` based on the query result, and then insert the query result into the new table.

```plaintext
SUBMIT TASK AS CREATE TABLE order_statistics AS SELECT COUNT(*) as count FROM order_detail;

+-------------------------------------------+-----------+
| TaskName                                  | Status    |
+-------------------------------------------+-----------+
| ctas-df6f7930-e7c9-11ec-abac-bab8ee315bf2 | SUBMITTED |
+-------------------------------------------+-----------+
```

Check information of the Task.

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;

-- Information of the Task

TASK_NAME: ctas-df6f7930-e7c9-11ec-abac-bab8ee315bf2
CREATE_TIME: 2022-06-14 14:07:06
SCHEDULE: MANUAL
DATABASE: default_cluster:test
DEFINITION: CREATE TABLE order_statistics AS SELECT COUNT(*) as cnt FROM order_detail
EXPIRE_TIME: 2022-06-17 14:07:06
```

Check the state of the TaskRun.

```SQL
SELECT * FROM INFORMATION_SCHEMA.task_runs;

-- State of the TaskRun

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

Query the new table when the state of the TaskRun is `SUCCESS`.

```SQL
SELECT * FROM order_statistics;
```
