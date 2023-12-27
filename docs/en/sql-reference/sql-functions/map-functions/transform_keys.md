---
displayed_sidebar: "English"
---

# transform_keys

## Description

Transforms keys in a map using a [Lambda expression](../Lambda_expression.md) and produces a new key for each entry in the map.

This function is supported from v3.1 onwards.

## Syntax

```Haskell
MAP transform_keys(lambda_func, any_map)
```

`lambda_func` can also be placed after `any_map`:

```Haskell
MAP transform_keys(any_map, lambda_func)
```

## Parameters

- `any_map`: the Map.

- `lambda_func`: the Lambda expression you want to apply to `any_map`.

## Return value

Returns a map value where the data types of keys are determined by the result of the Lambda expression and the data types of values are the same as values in `any_map`.

If any input parameter is NULL, NULL is returned.

If a key or value in the original map is NULL, NULL is processed as a normal value.

The Lambda expression must have two parameters. The first parameter represents the key. The second parameter represents the value.

## Examples

The following example uses [map_from_arrays](map_from_arrays.md) to generate a map value `{1:"ab",3:"cdd",2:null,null:"abc"}`. Then the Lambda expression is applied to each key to increment the key by 1.

```SQL
mysql> select transform_keys((k,v)->(k+1), col_map) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+------------------------------------------+
| transform_keys((k, v) -> k + 1, col_map) |
+------------------------------------------+
| {2:"ab",4:"cdd",3:null,null:"abc"}       |
+------------------------------------------+
```
