---
displayed_sidebar: "English"
---

# map_apply

## Description

Applies a [Lambda expression](../Lambda_expression.md) to the keys and values of the original Map and generates a new Map. This function is supported from v3.0.

## Syntax

```Haskell
MAP map_apply(lambda_func, any_map)
```

## Parameters

- `lambda_func`: the Lambda expression.

- `any_map`: the map to which the Lambda expression is applied.

## Return value

Returns a map value. The data types of keys and values in the result map are determined by the result of the Lambda expression.

If any input parameter is NULL, NULL is returned.

If a key or value in the original map is NULL, NULL is processed as a normal value.

The Lambda expression must have two parameters. The first parameter represents the key. The second parameter represents the value.

## Examples

The following example uses [map_from_arrays()](map_from_arrays.md) to generate a map value `{1:"ab",3:"cd"}`. Then the Lambda expression increments each key by 1 and calculates the length of each value. For example, the length of "ab" is 2.

```SQL
mysql> select map_apply((k,v)->(k+1,length(v)), col_map)
from (select map_from_arrays([1,3],["ab","cd"]) as col_map)A;
+--------------------------------------------------+
| map_apply((k, v) -> (k + 1, length(v)), col_map) |
+--------------------------------------------------+
| {2:2,4:2}                                        |
+--------------------------------------------------+
1 row in set (0.01 sec)

mysql> select map_apply((k,v)->(k+1,length(v)), col_map)
from (select map_from_arrays(null,null) as col_map)A;
+--------------------------------------------------+
| map_apply((k, v) -> (k + 1, length(v)), col_map) |
+--------------------------------------------------+
| NULL                                             |
+--------------------------------------------------+
1 row in set (0.02 sec)

mysql> select map_apply((k,v)->(k+1,length(v)), col_map)
from (select map_from_arrays([1,null],["ab","cd"]) as col_map)A;
+--------------------------------------------------+
| map_apply((k, v) -> (k + 1, length(v)), col_map) |
+--------------------------------------------------+
| NULL                                             |
+--------------------------------------------------+
```
