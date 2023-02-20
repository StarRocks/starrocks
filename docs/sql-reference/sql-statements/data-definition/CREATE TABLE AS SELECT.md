# CREATE TABLE AS SELECT

## Description

You can use the CREATE TABLE AS SELECT (CTAS) statement to synchronously or asynchronously query a table and create a new table based on the query result, and then insert the query result into the new table.

## Syntax

- Synchronously query a table and create a new table based on the query result, and then insert the query result into the new table.

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

- Asynchronously query a table and create a new table based on the query result, and then insert the query result into the new table.

  ```SQL
  SUBMIT [/*+ SET_VAR(key=value) */] TASK [[database.]<task_name>]AS
  CREATE TABLE [IF NOT EXISTS] [database.]table_name
  [(column_name [, column_name2, ...]]
  [COMMENT "table comment"]
  [partition_desc]
  [distribution_desc]
  [PROPERTIES ("key"="value", ...)]AS SELECT query
  [ ... ]
  ```

  The preceding syntax creates a Task, which is a template for storing a task that executes the CTAS statement. You can check the information of the Task by using the following syntax.

  ```SQL
  SELECT * FROM INFORMATION_SCHEMA.tasks;
  ```

  After you run the Task, a TaskRun is generated accordingly. A TaskRun indicates a task that executes the CTAS statement. A TaskRun has the following four states:

  - `PENDING`: The task waits to be run.
  - `RUNNING`: The task is running.
  - `FAILED`: The task failed.
  - `SUCCESS`: The task runs successfully.

  You can check the state of a TaskRun by using the following syntax.

  ```SQL
  SELECT * FROM INFORMATION_SCHEMA.task_runs;
  ```

## Parameters

### Parameters of the CTAS statement

| **Parameter**     | **Required** | **Description**                                              |
| ----------------- | ------------ | ------------------------------------------------------------ |
| column_name       | Yes          | The name of a column in the new table. You do not need to specify the data type for the column. StarRocks automatically specifies an appropriate data type for the column . StarRocks converts FLOAT and DOUBLE data into DECIMAL(38,9) data. StarRocks also converts CHAR, VARCHAR, and STRING data into VARCHAR(65533) data. |
| COMMENT           | No           | The comment of the new table.                                |
| partition_desc    | No           | The partitioning method of the new table. If you do not specify this parameter, by default, the new table has no partition. For more information about partitioning, see CREATE TABLE. |
| distribution_desc | No           | The bucketing method of the new table. If you do not specify this parameter, the bucket column defaults to the column with the highest cardinality collected by the cost-based optimizer (CBO). The number of buckets defaults to 10. If the CBO does not collect information about the cardinality, the bucket column defaults to the first column in the new table. For more information about bucketing, see CREATE TABLE. |
| Properties        | No           | The properties of the new table.                             |
| AS SELECT query   | Yes          | The query result.  You can specify columns in `... AS SELECT query`, for example, `... AS SELECT a, b, c FROM table_a;`. In this example, `a`, `b`, and `c` indicates the column names of the table that is queried. If you do not specify the column names of the new table, the column names of the new table are also `a`, `b`, and `c`. You can specify expressions in `... AS SELECT query`, for example, `... AS SELECT a+1 AS x, b+2 AS y, c*c AS z FROM table_a;`. In this example, `a+1`, `b+2`, and `c*c` indicates the column names of the table that is queried, and `x`, `y`, and `z` indicates the column names of the new table. Note:  The number of columns in the new table need to be the same as the number of the columns specified in the SELECT statement . We recommend that you use column names that are easy to identify. |

### Parameters of frontends (FEs)

If you asynchronously query a table and create a new table based on the query result, you also need to configure the following parameters.

| **Parameter**              | **Default value** | **Description**                                              |
| -------------------------- | ----------------- | ------------------------------------------------------------ |
| task_ttl_second            | 259200            | The period during which a Task is valid. Unit: seconds. Tasks that exceed the validity period are deleted. |
| task_check_interval_second | 14400             | The time interval to delete invalid Tasks. Unit: seconds.    |
| task_runs_ttl_second       | 259200            | The period during which a TaskRun is valid. Unit: seconds. TaskRuns that exceed the validity period are deleted automatically. Additionally, TaskRuns in the `FAILED` and `SUCCESS` states are also deleted automatically. |
| task_runs_concurrency      | 20                | The maximum number of TaskRuns that can be run in parallel.  |
| task_runs_queue_length     | 500               | The maximum number of TaskRuns that are pending for running. If the number exceeds the default value, the incoming tasks will be suspended. |

## Usage notes

- The CTAS statement can only create a new table that meets the following requirements:
  - `ENGINE` is `OLAP`.

  - The data model is the Duplicate Key model.

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

Example 4: Synchronously query four tables, including `lineorder`, `customer`, `supplier`, and `part` and create a new table `lineorder_flat` based on the query result, and then insert the query result to the new table. Additionally, specify the partitioning method and bucketing method for the new table.

```SQL
CREATE TABLE lineorder_flat
PARTITION BY RANGE(`LO_ORDERDATE`)
(
    START ("1993-01-01") END ("1999-01-01") EVERY (INTERVAL 1 YEAR)
)
DISTRIBUTED BY HASH(`LO_ORDERKEY`) BUCKETS 120 AS SELECT
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

Example 5: Asynchronously query the table `order_detail` and create a new table `order_statistics` based on the query result, and then insert the query result into the new table.

```Plain%20Text
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
