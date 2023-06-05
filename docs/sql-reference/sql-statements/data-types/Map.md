# MAP

## Background

Maps, as an extended type of database, are supported in PG, ClickHouse, Snowflake and other systems. They are widely used to express key-value pairs. Note there are no duplicate keys in a map.

## Map usage

### Map definition

`MAP<KEY-TYPE,VALUE-TYPE>`

`KEY-TYPE` should be base types in StarRocks, such as numeric types, string types and date-related types, excepting HLL, JSON, ARRAY, MAP, BITMAP, STRUCT.
`VALUE-TYPE` can be any supported types. `KEY-TYPE` and `VALUE-TYPE` are natively nullable.

The following is an example of defining a map column in StarRocks:

~~~SQL
-- One-dimensional map
create table t0(
  c0 INT,
  c1 `MAP<INT,INT>`
)
duplicate key(c0);

-- Define nested maps
create table t1(
  c0 INT,
  c1 `MAP<DATE, MAP<VARCHAR(10), INT>>`
)
duplicate key(c0);

-- Define not-null nullable maps
create table t2(
  c0 INT,
  c1 `MAP<INT,DATETIME>` NOT NULL
)
duplicate key(c0)
~~~

The map type has the following restrictions:

* Map columns cannot be used as key columns (maybe supported later)
* Map columns cannot be used as distribution columns
* Map columns cannot be used as partition columns

### Construct maps in SQL

Map can be constructed in SQL using `map{key_expr:value_expr, ...}`, with each map element separated by a comma (","), keys and values are separated by a colon (":");

Or alternatively constructing maps using `MAP(key_expr, value_expr ...)`, the expressions of keys and values should be in pairs. 

The keys' type and values' type are derived from all input keys and values, respectively.

~~~SQL
select map{1:1, 2:2, 3:3} as numbers;
select map(1,1,2,2,3,3) as numbers; -- The result is {{1:1,2:2,3:3}
select map{1:"apple", 2:"orange", 3:"pear"} as fruit;
select map(1, "apple", 2, "orange", 3, "pear") as fruit; -- The result is {1:"apple",2:"orange",3:"pear"}
select map{true:map{3.13:"abc"}, false:map{}} as nest;
select map(true, map(3.13, "abc"), false, map{}) as nest; -- The result is {1:{3.13:"abc"},0:{}}
~~~

When map'keys or values have different types, StarRocks will automatically derive the appropriate type (supertype)

~~~SQL
select map{1:2.2, 1.2:21} as floats_floats; -- The result is {1.0:2.2,1.2:21.0}
select map{12:"a", "100":1, NULL:NULL} as string_string; -- The result is {"12":"a","100":"1",null:null}
~~~

You can specifically define the maps'type when using `{}` to construct maps, the input keys or values should be able to respectively cast to the specified types.

~~~SQL
select `MAP<FLOAT,INT>`{1:2}; -- The result is {1.0:2}
select `MAP<INT:INT>`{"12": "100"}; -- The result is {12:100}.
~~~

NULL can be included in the key/value elements.

~~~SQL
select map{1:NULL};
~~~

Construct empty maps

~~~SQL
select map{} as empty_map;
select map() as empty_map; --The result is {}
~~~

### Map import

There are two ways to write map values to StarRocks. Insert into is suitable for small-scale data testing. ORC Parquet import is suitable for large-scale data import. NOTE StarRocks will remove duplicate keys of each map when importing maps.

* **INSERT INTO**

  ~~~SQL
  create table t0(c0 INT, c1 `MAP<INT:INT>`)duplicate key(c0);
  INSERT INTO t0 VALUES(1, map{1:2,3:NULL});
  ~~~

* **Import from ORC Parquet file**

  The map type in StarRocks corresponds to the map structure in ORC/Parquet format; no additional specification is needed. 


### Map element access

Access a key-value pair of a map using `[ ]` with a specific key, or using `element_at(any_map, any_key)`.

~~~Plain Text
mysql> select map{1:2,3:NULL}[1];

+-----------------------+
| map(1, 2, 3, NULL)[1] |
+-----------------------+
|                     2 |
+-----------------------+
~~~

If the key does not exist in the map, returning `NULL`.

~~~Plain Text
mysql> select map{1:2,3:NULL}[2];

+-----------------------+
| map(1, 2, 3, NULL)[2] |
+-----------------------+
|                  NULL |
+-----------------------+
~~~

For multidimensional maps, the internal maps can be accessed **recursively**.

~~~Plain Text
mysql> select map{1:map{2:1},3:NULL}[1][2];

+----------------------------------+
| map(1, map(2, 1), 3, NULL)[1][2] |
+----------------------------------+
|                                1 |
+----------------------------------+
~~~
