---
displayed_sidebar: "English"
---

# transform_values

## Description

Transforms values in a map using a [Lambda expression](../Lambda_expression.md) and produces a new value for each entry in the map.

This function is supported from v3.1 onwards.

## Syntax

```Haskell
MAP transform_values(lambda_func, any_map)
```

`lambda_func` can also be placed after `any_map`:

```Haskell
MAP transform_values(any_map, lambda_func)
```

## Parameters

- `any_map`: the Map.

- `lambda_func`: the Lambda expression you want to apply to `any_map`.

## Return value

Returns a map value where the data types of values are determined by the result of the Lambda expression and the data types of keys are the same as keys in `any_map`.

If any input parameter is NULL, NULL is returned.

If a key or value in the original map is NULL, NULL is processed as a normal value.

The Lambda expression must have two parameters. The first parameter represents the key. The second parameter represents the value.

## Examples

The following example uses [map_from_arrays](map_from_arrays.md) to generate a map value `{1:"ab",3:"cdd",2:null,null:"abc"}`. Then the Lambda expression is applied to each value of the map. The first example changes the value of each key-value pair to 1. The second example changes the value of each key-value pair to null.

```SQL
mysql> select transform_values((k,v)->1, col_map) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+----------------------------------------+
| transform_values((k, v) -> 1, col_map) |
+----------------------------------------+
| {1:1,3:1,2:1,null:1}                   |
+----------------------------------------+
1 row in set (0.02 sec)

mysql> select transform_values((k,v)->null, col_map) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+--------------------------------------------+
| transform_values((k, v) -> NULL, col_map)  |
+--------------------------------------------+
| {1:null,3:null,2:null,null:null} |
+--------------------------------------------+
1 row in set (0.01 sec)
```
