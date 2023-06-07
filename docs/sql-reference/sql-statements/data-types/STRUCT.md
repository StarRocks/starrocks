# Struct

## Background

Struct, as an extended type of database, are supported in Trino, ClickHouse and other systems. They are contains a set of named fields, each field has a type and a name. Structs are widely used to express complex data types. Note there are no duplicate field names in a struct.

## Struct usage

### Struct definition

`Struct<NAME, TYPE>`

`NAME` column name, it's same as the column name in the `CREATE TABLE` statement.
`TYPE` can be any supported types, and it's natively nullable.

The following is an example of defining a struct column in StarRocks:

~~~SQL
-- One-dimensional struct
create table t0(
  c0 INT,
  c1 STRUCT<a INT, b INT>
)
duplicate key(c0);

-- Define complex structs
create table t1(
  c0 INT,
  c1 STRUCT<a INT, b STRUCT<c INT, d INT>, c MAP<INT, INT>, d ARRAY<INT>>
)
duplicate key(c0);

-- Define not-null nullable structs
create table t2(
  c0 INT,
  c1 STRUCT<a INT, b INT> NOT NULL
)
duplicate key(c0);
~~~

The struct type has the following restrictions:

* Struct columns cannot be used as key columns (maybe supported later)
* Struct columns cannot be used as distribution columns
* Struct columns cannot be used as partition columns
* Struct columns only support REPLACE in Aggregate Table Model

### Construct structs in SQL

Struct can be constructed in SQL using function `row`, `named_struct`, `struct` 

~~~SQL
select row(1, 2, 3, 4) as numbers; -- The result is {'col1': 1, 'col2': 2, 'col3': 3, 'col4': 4}
select struct(1, 2, 3, 4) as numbers; -- The result is {'col1': 1, 'col2': 2, 'col3': 3, 'col4': 4}
select named_struct('a', 1, 'b', 2, 'c', 3, 'd', 4) as numbers; -- The result is {'a': 1, 'b': 2, 'c': 3, 'd': 4}
~~~

`row`, `struct` support un-named struct, StarRocks will automatically generate column names, like `col1`, `col2`...
`named_struct` support a named struct, the parameters must be a Name/Value pairs, and the number of parameters must be even.

StarRocks will automatically determine the type of the struct.

### Struct import

There are two ways to write struct values to StarRocks. Insert into is suitable for small-scale data testing. ORC or Parquet import is suitable for large-scale data import. NOTE StarRocks will automatically cast the data type to the corresponding struct type.

* **INSERT INTO**

  ~~~SQL
  create table t0(c0 INT, c1 STRUCT<a INT, b INT>)duplicate key(c0);
  INSERT INTO t0 VALUES(1, row(1, 1));
  ~~~

* **Import from ORC Parquet file**

  The struct type in StarRocks corresponds to the nest columns structure in ORC/Parquet format; no additional specification is needed. 


### Struct element access

Access a subfield of a struct using `.`, or using `[subfield-index]`

~~~Plain Text
mysql> select named_struct('a', 1, 'b', 2, 'c', 3, 'd', 4).a;

+------------------------------------------------+
| named_struct('a', 1, 'b', 2, 'c', 3, 'd', 4).a |
+------------------------------------------------+
| 1                                              |
+------------------------------------------------+

mysql> select row(1, 2, 3, 4).col1;
+-----------------------+
| row(1, 2, 3, 4).col1  |
+-----------------------+
| 1                     |
+-----------------------+

mysql> select row(1, 2, 3, 4)[2];
+---------------------+
| row(1, 2, 3, 4)[2]  |
+---------------------+
| 2                   |
+---------------------+
~~~
