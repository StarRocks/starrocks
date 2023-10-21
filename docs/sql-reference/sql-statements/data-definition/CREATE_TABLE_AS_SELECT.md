# CREATE TABLE AS SELECT

## Description

CREATE TABLE AS SELECT (CTAS) is used to create a new table based on the query results of other tables.

## Syntax

~~~SQL
CREATE TABLE [IF NOT EXISTS] [database.]table_name

[(column_name [, column_name2, ...]]

[COMMENT "table comment"]

[partition_desc]

[distribution_desc]

[PROPERTIES ("key"="value", ...)]

AS SELECT query

[ ... ]
~~~

## Parameters

### Table creation

| Parameter         | Description                                                  |
| ----------------- | ------------------------------------------------------------ |
| column_name       | The column name. You do not need to pass in the column data type. StarRocks automatically selects an appropriate data type for each column and converts FLOAT and DOUBLE data into DECIMAL (38,9), and converts CHAR, VARCHAR, and STRING data into VARCHAR (65533). |
| COMMENT           | The table comment.                                           |
| partition_desc    | The partitioning method. For more information, see [partition_desc](https://docs.starrocks.com/zh-cn/main/sql-reference/sql-statements/data-definition/CREATE_TABLE/#syntax). An empty value indicates that the table has no partition. |
| distribution_desc | The bucketing method. For more information, see [distribution_desc](https://docs.starrocks.com/zh-cn/main/sql-reference/sql-statements/data-definition/CREATE_TABLE/#syntax). An empty value indicates that the bucket key is the column that has the highest cardinality in the cost-based optimizer (CBO) statistical information and the number of buckets is 10. If the CBO does not contain related statistical information, the bucket key is the first column by default. |
| properties        | The properties of the new table. For more information, see [PROPERTIES](https://docs.starrocks.com/zh-cn/main/sql-reference/sql-statements/data-definition/CREATE_TABLE#syntax). Currently, CTAS can only be used to create a table whose ENGINE is OLAP. |

### Query

- You can specify columns in `... AS SELECT query`, for example, `... AS SELECT a, b, c FROM table_a;`. In this case, the column names of the new table are a, b, and c.
- You can use expressions in `... AS SELECT query`. We also recommend that you specify aliases that are easy to identify for columns of the new table.

## Usage notes

- CTAS can only be used to create a table whose `ENGINE` is OLAP, the data model is the duplicate key model, and the sort keys are the first three columns (the storage space for each data type cannot exceed 36 bytes).

- Currently, CTAS does not support indexes.

- Currently, transaction guarantee is not provided for CTAS. If the CTAS statement fails (due to reasons such as an FE restart), the following issues may occur:
  - A new table may have been created and is not deleted.

- A new table may have been created. If another import job other than CTAS (such as INSERT) is also used to import data into the new table, the data successfully imported for the first time is regarded as the first version of the data.

- After the table is created, you must manually grant permissions on this table to users.

## Examples

Example 1: Create a table `order_new` based on the table `order`.

~~~SQL
CREATE TABLE order_new

AS SELECT * FROM order;
~~~

Example 2: Create a table `order_new` based on columns k1, k2, and k3 in the table `order`, and specify column names as a, b, and c.

> The number of specified columns must be the same as the number in `... AS SELECT query`.

~~~SQL
CREATE TABLE order_new a, b, c

AS SELECT k1, k2, k3 FROM order;
CREATE TABLE order_new

AS SELECT k1 AS a, k2 AS b, k3 AS c FROM order;
~~~

Example 3: Use an expression in `... AS SELECT query` and create a table based on the result of the expression and specify column names for the new table.

> We recommend that you specify column names that are easy to identify for the new table.

~~~SQL
--Calculate the largest salary value in the table employee. Create a table employee_new based on the result and set the column name of the new table to salary_new.

CREATE TABLE employee_new

AS SELECT MAX(salary) AS salary_max FROM employee;

 

--Query data in the employee_new table.

SELECT * FROM employee_new;

+------------+

| salary_max |

+------------+

|   10000    |

+------------+
~~~

Example 4: Create a table `lineorder_flat` based on four tables (`lineorder`, `customer`, `supplier`, and `part`), and adjust the partitioning and bucketing methods.

~~~SQL
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
~~~
