# ARRAY

## Background

Arrays, as an extended type of database, are supported in PG, ClickHouse, Snowflake and other systems. They can be widely used in scenarios such as A/B tests, user tag analysis, and crowd profiling. StarRocks supports multidimensional array nesting, array slicing, comparison, and filtering.

## Array usage

### Array definition

The following is an example of defining an array column in StarRocks:

~~~SQL
-- One-dimensional arrays
create table t0(
  c0 INT,
  c1 `ARRAY<INT>`
)
duplicate key(c0);

-- Define nested arrays
create table t1(
  c0 INT,
  c1 `ARRAY<ARRAY<VARCHAR(10)>>`
)
duplicate key(c0);
~~~

As above, the array column is defined in the form of `ARRAY` and its `TYPE` has  a default value of `nullable`. Currently, StarRocks does not support specifying `TYPE` as `NOT NULL`, but you can define the array itself as `NOT NULL`.

~~~SQL
create table t2(
  c0 INT,
  c1 `ARRAY<INT>` NOT NULL
)
duplicate key(c0)
~~~

The array type has the following restrictions:

* The array columns can be only defined in duplicate tables
* Array columns cannot be used as key columns (may be supported later)
* Array columns cannot be used as distribution columns
* Array columns cannot be used as partition columns

### Construct arrays in SQL

Arrays can be constructed in SQL using brackets ("[" and "]"), with each array element separated by a comma (",")

~~~SQL
select [1, 2, 3] as numbers;
select ["apple", "orange", "pear"] as fruit;
select [true, false] as booleans;
~~~

When array elements have different types, StarRocks will automatically derive the appropriate type (supertype)

~~~SQL
select [1, 1.2] as floats;
select [12, "100"]; -- The result is ["12", "100"].
~~~

You can use pointed brackets (`<>`) to show the declared array type.

~~~SQL
select `ARRAY<float>`[1, 2];
select `ARRAY<INT>`["12", "100"]; -- The result is [12, 100].
~~~

NULL can be included in the element

~~~SQL
select [1, NULL];
~~~

For empty arrays, you can use pointed brackets to show the declared type, or you can write \[\] directly for StarRocks to infer the type based on the context. If StarRocks can’t infer the type, it will report an error.

~~~SQL
select [];
select `ARRAY<VARCHAR(10)>`[];
select array_append([], 10);
~~~

### Array import

There are three ways to write array values to StarRocks. Insert into is suitable for small-scale data testing. ORC Parquet inport and CSV import are suitable for large-scale data import.

* **INSERT INTO**

  ~~~SQL
  create table t0(c0 INT, c1 `ARRAY<INT>`)duplicate key(c0);
  INSERT INTO t0 VALUES(1, [1,2,3]);
  ~~~

* **Import from ORC Parquet file**

  The array type in StarRocks corresponds to the list structure in ORC/Parquet format; no additional specification is needed. Currently ORC list structure can be imported directly.

* **Import from CSV file**

  The array of CSV files is separated by comma by default. You can use stream load or routine load to import CSV files or CSV format data in Kafka.

### Array element access

Access an element of an array using `[ ]` and subscripts, starting with `1`.

~~~Plain Text
mysql> select [1,2,3][1];

+------------+
| [1,2,3][1] |
+------------+
|          1 |
+------------+
1 row in set (0.00 sec)
~~~

If the subscript is 0 or a negative number, **no error will be reported and NULL will be returned**

~~~Plain Text
mysql> select [1,2,3][0];

+------------+
| [1,2,3][0] |
+------------+
|       NULL |
+------------+
1 row in set (0.01 sec)
~~~

If the subscript exceeds the size of the array, **NULL will be returned**.

~~~Plain Text
mysql> select [1,2,3][4];

+------------+
| [1,2,3][4] |
+------------+
|       NULL |
+------------+
1 row in set (0.01 sec)
~~~

For multidimensional arrays, the internal elements can be accessed **recursively**.

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
