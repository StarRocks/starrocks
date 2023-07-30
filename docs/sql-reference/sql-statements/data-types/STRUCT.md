# STRUCT

## Description

STRUCT is widely used to express complex data types. It represents a collection of elements (also called fields) with different data types, for example, `<a INT, b STRING>`.

Field names in a struct must be unique. Fields can be of primitive data types (such as numeric, string, or date) or complex data types (such as ARRAY or MAP).

A field within a struct can also be another STRUCT, ARRAY, or MAP, which allows you to create nested data structures, for example, `STRUCT<a INT, b STRUCT<c INT, d INT>, c MAP<INT, INT>, d ARRAY<INT>>`.

The STRUCT data type is supported from v3.1 onwards. In v3.1, you can define STRUCT columns when you create a StarRocks table, load STRUCT data into that table, and query MAP data.

From v2.5 onwards, StarRocks supports querying complex data types MAP and STRUCT from data lakes. You can use external catalogs provided by StarRocks to query MAP and STRUCT data from Apache Hive™, Apache Hudi, and Apache Iceberg. You can only query data from ORC and Parquet files. For more information about how to use external catalogs to query external data sources, see [Overview of catalogs](../../../data_source/catalog/catalog_overview.md) and topics related to the required catalog type.

## Syntax

```Haskell
STRUCT<name, type>
```

- `name`: the field name, same as the column name defined in the CREATE TABLE statement.
- `type`: the field type. It can be of any supported type.

## Define a STRUCT column in StarRocks

You can define a STRUCT column when you create a table and load STRUCT data into this column.

```SQL
-- Define a one-dimensional struct.
CREATE TABLE t0(
  c0 INT,
  c1 STRUCT<a INT, b INT>
)
DUPLICATE KEY(c0);

-- Define a complex struct.
CREATE TABLE t1(
  c0 INT,
  c1 STRUCT<a INT, b STRUCT<c INT, d INT>, c MAP<INT, INT>, d ARRAY<INT>>
)
DUPLICATE KEY(c0);

-- Define a NOT NULL struct.
CREATE TABLE t2(
  c0 INT,
  c1 STRUCT<a INT, b INT> NOT NULL
)
DUPLICATE KEY(c0);
```

Columns with the STRUCT type have the following restrictions:

- Cannot be used as key columns in a table. They can only be used as value columns.
- Cannot be used as partition key columns (following PARTITION BY) in a table.
- Cannot be used as bucketing columns (following DISTRIBUTED BY) in a table.
- Only supports the replace() function when used as a value column in an [Aggregate table](../../../table_design/table_types/aggregate_table.md).

## Construct structs in SQL

STRUCT can be constructed in SQL using the following functions: [row, struct](../../sql-functions/struct-functions/row.md), and [named_struct](../../sql-functions/struct-functions/named_struct.md). struct() is the alias of row().

- `row` and `struct` support unnamed struct. You do not need to specify the field names. StarRocks automatically generates column names, like `col1`, `col2`...
- `named_struct` supports named struct. The expressions of names and values must be in pairs.

StarRocks automatically determines the type of the struct based on the input values.

```SQL
select row(1, 2, 3, 4) as numbers; -- Return {"col1":1,"col2":2,"col3":3,"col4":4}.
select row(1, 2, null, 4) as numbers; -- Return {"col1":1,"col2":2,"col3":null,"col4":4}.
select row(null) as nulls; -- Return {"col1":null}.
select struct(1, 2, 3, 4) as numbers; -- Return {"col1":1,"col2":2,"col3":3,"col4":4}.
select named_struct('a', 1, 'b', 2, 'c', 3, 'd', 4) as numbers; -- Return {"a":1,"b":2,"c":3,"d":4}.
```

## Load STRUCT data

You can load STRUCT data into StarRocks using two methods: [INSERT INTO](../../../loading/InsertInto.md), and [ORC/Parquet loading](../data-manipulation/BROKER%20LOAD.md).

Note that StarRocks automatically casts the data type into the corresponding STRUCT type.

### INSERT INTO

```SQL
CREATE TABLE t0(
  c0 INT,
  c1 STRUCT<a INT, b INT>
)
DUPLICATE KEY(c0);

INSERT INTO t0 VALUES(1, row(1, 1));

SELECT * FROM t0;
+------+---------------+
| c0   | c1            |
+------+---------------+
|    1 | {"a":1,"b":1} |
+------+---------------+
```

### Load STRUCT data from ORC/Parquet files

The STRUCT data type in StarRocks corresponds to the nested columns structure in ORC or Parquet format. No additional specification is needed. You can load STRUCT data from ORC or Parquet files by following the instructions in [ORC/Parquet loading](../data-manipulation/BROKER%20LOAD.md).

## Access STRUCT fields

To query a subfield of a struct, you can use the dot (`.`) operator to query a value by its field name, or use `[]` to call a value by its index.

```Plain Text
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

mysql> select row(2, 4, 6, 8)[2];
+--------------------+
| row(2, 4, 6, 8)[2] |
+--------------------+
|                  4 |
+--------------------+

mysql> select row(map{'a':1}, 2, 3, 4)[1];
+-----------------------------+
| row(map{'a':1}, 2, 3, 4)[1] |
+-----------------------------+
| {"a":1}                     |
+-----------------------------+
```
