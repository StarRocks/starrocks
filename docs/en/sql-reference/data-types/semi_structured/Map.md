---
displayed_sidebar: docs
---

# MAP

## Description

MAP is a complex data type that stores a set of key-value pairs, for example, `{a:1, b:2, c:3}`. Keys in a map must be unique. A nested map can contain up to 14 levels of nesting.

The MAP data type is supported from v3.1 onwards. In v3.1, you can define MAP columns when you create a StarRocks table, load MAP data into that table, and query MAP data.

From v2.5 onwards, StarRocks supports querying complex data types MAP and STRUCT from data lakes. You can use external catalogs provided by StarRocks to query MAP and STRUCT data from Apache Hive™, Apache Hudi, and Apache Iceberg. You can only query data from ORC and Parquet files. For more information about how to use external catalogs to query external data sources, see [Overview of catalogs](../../../data_source/catalog/catalog_overview.md) and topics related to the required catalog type.

## Syntax

```Haskell
MAP<key_type,value_type>
```

- `key_type`: the data type of the key. The key must be of a primitive type supported by StarRocks, such as numeric, string, or date. It cannot be of the HLL, JSON, ARRAY, MAP, BITMAP, or STRUCT type.
- `value_type`: the data type of the value. The value can be of any supported type.

Keys and values are **natively nullable**.

## Define a MAP column in StarRocks

You can define a MAP column when you create a table and load MAP data into this column.

```SQL
-- Define a one-dimensional map.
CREATE TABLE t0(
  c0 INT,
  c1 MAP<INT,INT>
)
DUPLICATE KEY(c0);

-- Define a nested map.
CREATE TABLE t1(
  c0 INT,
  c1 MAP<DATE, MAP<VARCHAR(10), INT>>
)
DUPLICATE KEY(c0);

-- Define a NOT NULL map.
CREATE TABLE t2(
  c0 INT,
  c1 MAP<INT,DATETIME> NOT NULL
)
DUPLICATE KEY(c0);
```

Columns with the MAP type have the following restrictions:

- Cannot be used as key columns in a table. They can only be used as value columns.
- Cannot be used as partition key columns (the columns following PARTITION BY) in a table.
- Cannot be used as bucketing columns (the columns following DISTRIBUTED BY) in a table.

## Construct maps in SQL

Map can be constructed in SQL using the following two syntaxes:

- `map{key_expr:value_expr, ...}`: Map elements are separated by a comma (`,`), and keys and values are separated by a colon (`:`), for example, `map{a:1, b:2, c:3}`.

- `map(key_expr, value_expr ...)`: The expressions of keys and values must be in pairs, for example, `map(a,1,b,2,c,3)`.

StarRocks can derive the data types of keys and values from all the input keys and values.

```SQL
select map{1:1, 2:2, 3:3} as numbers;
select map(1,1,2,2,3,3) as numbers; -- Return {1:1,2:2,3:3}.
select map{1:"apple", 2:"orange", 3:"pear"} as fruit;
select map(1, "apple", 2, "orange", 3, "pear") as fruit; -- Return {1:"apple",2:"orange",3:"pear"}.
select map{true:map{3.13:"abc"}, false:map{}} as nest;
select map(true, map(3.13, "abc"), false, map{}) as nest; -- Return {1:{3.13:"abc"},0:{}}.
```

If the keys or values have different types, StarRocks automatically derives the appropriate type (supertype).

```SQL
select map{1:2.2, 1.2:21} as floats_floats; -- Return {1.0:2.2,1.2:21.0}.
select map{12:"a", "100":1, NULL:NULL} as string_string; -- Return {"12":"a","100":"1",null:null}.
```

You can also define the data type using `<>` when you construct a map. The input keys or values must be able to cast into the specified types.

```SQL
select map<FLOAT,INT>{1:2}; -- Return {1:2}.
select map<INT,INT>{"12": "100"}; -- Return {12:100}.
```

Keys and values are nullable.

```SQL
select map{1:NULL};
```

Construct empty maps.

```SQL
select map{} as empty_map;
select map() as empty_map; -- Return {}.
```

## Load MAP data into StarRocks

You can load map data into StarRocks using two methods: [INSERT INTO](../../../loading/InsertInto.md), and [ORC/Parquet loading](../../sql-statements/loading_unloading/BROKER_LOAD.md).

Note that StarRocks will remove duplicate keys of each map when loading MAP data.

### INSERT INTO

```SQL
  CREATE TABLE t0(
    c0 INT,
    c1 MAP<INT,INT>
  )
  DUPLICATE KEY(c0);

  INSERT INTO t0 VALUES(1, map{1:2,3:NULL});
```

### Load MAP data from ORC and Parquet files

The MAP data type in StarRocks corresponds to the map structure in ORC or Parquet format. No additional specification is needed. You can load MAP data from ORC or Parquet files by following the instructions in [ORC/Parquet loading](../../sql-statements/loading_unloading/BROKER_LOAD.md).

## Access MAP data

Example 1: Query MAP column `c1` from table `t0`.

```Plain Text
mysql> select c1 from t0;
+--------------+
| c1           |
+--------------+
| {1:2,3:null} |
+--------------+
```

Example 2: Use the `[ ]` operator to retrieve values from a map by key, or use the `element_at(any_map, any_key)` function.

The following example queries the value corresponding to key `1`.

```Plain Text
mysql> select map{1:2,3:NULL}[1];
+-----------------------+
| map(1, 2, 3, NULL)[1] |
+-----------------------+
|                     2 |
+-----------------------+

mysql> select element_at(map{1:2,3:NULL},1);
+--------------------+
| map{1:2,3:NULL}[1] |
+--------------------+
|                  2 |
+--------------------+
```

If the key does not exist in the map, `NULL` is returned.

The following example queries the value corresponding to key 2, which does not exist.

```Plain Text
mysql> select map{1:2,3:NULL}[2];
+-----------------------+
| map(1, 2, 3, NULL)[2] |
+-----------------------+
|                  NULL |
+-----------------------+
```

Example 3: Query multidimensional maps **recursively**.

The following example first queries the value corresponding to key `1`, which is `map{2:1}` and then recursively queries the value corresponding to key `2` in `map{2:1}`.

```Plain Text
mysql> select map{1:map{2:1},3:NULL}[1][2];

+----------------------------------+
| map(1, map(2, 1), 3, NULL)[1][2] |
+----------------------------------+
|                                1 |
+----------------------------------+
```

## References

- [Map functions](../../sql-functions/map-functions/map_values.md)
- [element_at](../../sql-functions/array-functions/element_at.md)
