---
displayed_sidebar: "English"
---

# ARRAY

ARRAY, as an extended type of database, is supported in various database systems such as PostgreSQL, ClickHouse, and Snowflake. ARRAY is widely used in scenarios such as A/B tests, user tag analysis, and user profiling. StarRocks supports multidimensional array nesting, array slicing, comparison, and filtering.

## Define ARRAY columns

You can define an ARRAY column when you create a table.

~~~SQL
-- Define a one-dimensional array.
ARRAY<type>

-- Define a nested array.
ARRAY<ARRAY<type>>

-- Define an array column as NOT NULL.
ARRAY<type> NOT NULL
~~~

`type` specifies the data types of elements in an array. StarRocks supports the following element types: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, VARCHAR, CHAR, DATETIME, DATE, JSON, ARRAY (since v3.1), MAP (since v3.1), and STRUCT (since v3.1).

Elements in an array are nullable by default, for example, `[null, 1 ,2]`. You cannot specify elements in an array as NOT NULL. However, you can specify an ARRAY column as NOT NULL when you create a table, such as the third example in the following code snippet.

Examples:

~~~SQL
-- Define c1 as a one-dimensional array whose element type is INT.
create table t0(
  c0 INT,
  c1 ARRAY<INT>
)
duplicate key(c0)
distributed by hash(c0);

-- Define c1 as an nested array whose element type is VARCHAR.
create table t1(
  c0 INT,
  c1 ARRAY<ARRAY<VARCHAR(10)>>
)
duplicate key(c0)
distributed by hash(c0);

-- Define c1 as a NOT NULL array column.
create table t2(
  c0 INT,
  c1 ARRAY<INT> NOT NULL
)
duplicate key(c0)
distributed by hash(c0);
~~~

## Limits

The following limits apply when you create ARRAY columns in StarRocks tables:

- In versions earlier than v2.1, you can create ARRAY columns only in Duplicate Key tables. From v2.1 onwards, you can also create ARRAY columns in other types of tables (Primary Key, Unique Key, Aggregate). Note that in an Aggregate table, you can create an ARRAY column only when the function used to aggregate data in that column is replace() or replace_if_not_null(). For more information, see [Aggregate table](../../../table_design/table_types/aggregate_table.md).
- ARRAY columns cannot be used as key columns.
- ARRAY columns cannot be used as partition keys (included in PARTITION BY) or bucketing keys (included in DISTRIBUTED BY).
- DECIMAL V3 is not supported in ARRAY.
- An array can have a maximum of 14-level nesting.

## Construct arrays in SQL

Arrays can be constructed in SQL using brackets `[]`, with each array element separated by a comma (`,`).

~~~Plain Text
mysql> select [1, 2, 3] as numbers;

+---------+
| numbers |
+---------+
| [1,2,3] |
+---------+

mysql> select ["apple", "orange", "pear"] as fruit;

+---------------------------+
| fruit                     |
+---------------------------+
| ["apple","orange","pear"] |
+---------------------------+

mysql> select [true, false] as booleans;

+----------+
| booleans |
+----------+
| [1,0]    |
+----------+
~~~

StarRocks automatically infers data types if an array consists of elements of multiple types:

~~~Plain Text
mysql> select [1, 1.2] as floats;
+---------+
| floats  |
+---------+
| [1.0,1.2] |
+---------+

mysql> select [12, "100"];

+--------------+
| [12,'100']   |
+--------------+
| ["12","100"] |
+--------------+
~~~

You can use pointed brackets (`<>`) to show the declared array type.

~~~Plain Text
mysql> select ARRAY<float>[1, 2];

+-----------------------+
| ARRAY<float>[1.0,2.0] |
+-----------------------+
| [1,2]                 |
+-----------------------+

mysql> select ARRAY<INT>["12", "100"];

+------------------------+
| ARRAY<int(11)>[12,100] |
+------------------------+
| [12,100]               |
+------------------------+
~~~

NULLs can be included in the element.

~~~Plain Text
mysql> select [1, NULL];

+----------+
| [1,NULL] |
+----------+
| [1,null] |
+----------+
~~~

For an empty array, you can use pointed brackets to show the declared type, or you can write \[\] directly for StarRocks to infer the type based on the context. If StarRocks cannot infer the type, it will report an error.

~~~Plain Text
mysql> select [];

+------+
| []   |
+------+
| []   |
+------+

mysql> select ARRAY<VARCHAR(10)>[];

+----------------------------------+
| ARRAY<unknown type: NULL_TYPE>[] |
+----------------------------------+
| []                               |
+----------------------------------+

mysql> select array_append([], 10);

+----------------------+
| array_append([], 10) |
+----------------------+
| [10]                 |
+----------------------+
~~~

## Load Array data

StarRocks supports loading Array data in three ways:

- INSERT INTO is suitable for loading small-scale data for testing.
- Broker Load is suitable for loading ORC or Parquet files with large-scale data.
- Stream Load and Routine Load are suitable for loading CSV files with large-scale data.

### Use INSERT INTO to load arrays

You can use INSERT INTO to load small-scale data column by column, or perform ETL on data before loading the data.

  ~~~SQL
create table t0(
  c0 INT,
  c1 ARRAY<INT>
)
duplicate key(c0)
distributed by hash(c0);

INSERT INTO t0 VALUES(1, [1,2,3]);
~~~

### Use Broker Load to load arrays from ORC or Parquet files

  The array type in StarRocks corresponds to the list structure in ORC and Parquet files, which eliminates the need for you to specify different data types in StarRocks. For more information about data loading, see [Broker load](../data-manipulation/BROKER_LOAD.md).

### Use Stream Load or Routine Load to load CSV-formatted arrays

  Arrays in CSV files are separated with comma by default. You can use [Stream Load](../../../loading/StreamLoad.md#load-csv-data) or [Routine Load](../../../loading/RoutineLoad.md#load-csv-format-data) to load CSV text files or CSV data in Kafka.

## Query ARRAY data

You can access elements in an array using `[]` and subscripts, starting from `1`.

~~~Plain Text
mysql> select [1,2,3][1];

+------------+
| [1,2,3][1] |
+------------+
|          1 |
+------------+
1 row in set (0.00 sec)
~~~

If the subscript is 0 or a negative number, **no error is reported and NULL is returned**.

~~~Plain Text
mysql> select [1,2,3][0];

+------------+
| [1,2,3][0] |
+------------+
|       NULL |
+------------+
1 row in set (0.01 sec)
~~~

If the subscript exceeds the length of the array (the number of elements in the array), **NULL will be returned**.

~~~Plain Text
mysql> select [1,2,3][4];

+------------+
| [1,2,3][4] |
+------------+
|       NULL |
+------------+
1 row in set (0.01 sec)
~~~

For multidimensional arrays, the elements can be accessed **recursively**.

~~~Plain Text
mysql(ARRAY)> select [[1,2],[3,4]][2];

+------------------+
| [[1,2],[3,4]][2] |
+------------------+
| [3,4]            |
+------------------+
1 row in set (0.00 sec)

mysql> select [[1,2],[3,4]][2][1];

+---------------------+
| [[1,2],[3,4]][2][1] |
+---------------------+
|                   3 |
+---------------------+
1 row in set (0.01 sec)
~~~
