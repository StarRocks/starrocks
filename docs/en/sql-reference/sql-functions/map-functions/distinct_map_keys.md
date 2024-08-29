---
displayed_sidebar: docs
---

# distinct_map_keys

## Description

Removes duplicate keys from a map, because keys in a map must be unique in terms of semantics. This function keeps only the last value for identical keys, called LAST WIN. This function is used when querying MAP data from external tables if there are duplicate keys in maps. StarRocks internal tables natively remove duplicate keys in maps.

This function is supported from v3.1 onwards.

## Syntax

```Haskell
distinct_map_keys(any_map)
```

## Parameters

`any_map`: the MAP value from which you want to remove duplicate keys.

## Return value

Returns a new map without duplicate keys in each map.

If the input is NULL, NULL is returned.

## Examples

Example 1: Simple usage.

```plain
select distinct_map_keys(map{"a":1,"a":2});
+-------------------------------------+
| distinct_map_keys(map{'a':1,'a':2}) |
+-------------------------------------+
| {"a":2}                             |
+-------------------------------------+
```

Example 2: Query MAP data from external tables and remove duplicate keys from the `col_map` column.

```plain
select distinct_map_keys(col_map) as unique, col_map from external_table;
+---------------+---------------+
|      unique   | col_map       |
+---------------+---------------+
|       {"c":2} | {"c":1,"c":2} |
|           NULL|          NULL |
| {"e":4,"d":5} | {"e":4,"d":5} |
+---------------+---------------+
3 rows in set (0.05 sec)
```
