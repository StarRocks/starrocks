---
displayed_sidebar: docs
---

# map_filter

## Description

Filters key-value pairs in a map by applying a Boolean array or a [Lambda expression](../Lambda_expression.md) to each key-value pair. The pair that evaluates to `true` is returned.

This function is supported from v3.1 onwards.

## Syntax

```Haskell
MAP map_filter(any_map, array<boolean>)
MAP map_filter(lambda_func, any_map)
```

- `map_filter(any_map, array<boolean>)`

  Evaluates key-value pairs in `any_map` one by one against `array<boolean>` and returns key-value pairs that evaluate to `true`.

- `map_filter(lambda_func, any_map)`

  Applies `lambda_func` to the key-value pairs in `any_map` one by one and returns key-value pairs whose result is `true`.

## Parameters

- `any_map`: the map value.

- `array<boolean>`: the Boolean array used to evaluate the map value.

- `lambda_func`: the Lambda expression used to evaluate the map value.

## Return value

Returns a map whose data type is the same as `any_map`.

If `any_map` is NULL, NULL is returned. If `array<boolean>` is null, an empty map is returned.

If a key or value in the map value is NULL, NULL is processed as a normal value.

The Lambda expression must have two parameters. The first parameter represents the key. The second parameter represents the value.

## Examples

### Use `array<boolean>`

The following example uses [map_from_arrays()](map_from_arrays.md) to generate a map value `{1:"ab",3:"cdd",2:null,null:"abc"}`. Then each key-value pair is evaluated against `array<boolean>` and the pair whose result is `true` is returned.

```SQL
mysql> select map_filter(col_map, array<boolean>[0,0,0,1,1]) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+----------------------------------------------------+
| map_filter(col_map, ARRAY<BOOLEAN>[0, 0, 0, 1, 1]) |
+----------------------------------------------------+
| {null:"abc"}                                       |
+----------------------------------------------------+
1 row in set (0.02 sec)

mysql> select map_filter(null, array<boolean>[0,0,0,1,1]);
+-------------------------------------------------+
| map_filter(NULL, ARRAY<BOOLEAN>[0, 0, 0, 1, 1]) |
+-------------------------------------------------+
| NULL                                            |
+-------------------------------------------------+
1 row in set (0.02 sec)

mysql> select map_filter(col_map, null) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+---------------------------+
| map_filter(col_map, NULL) |
+---------------------------+
| {}                        |
+---------------------------+
1 row in set (0.01 sec)
```

### Use Lambda expression

The following example uses map_from_arrays() to generate a map value `{1:"ab",3:"cdd",2:null,null:"abc"}`. Then each key-value pair is evaluated against the Lambda expression and the key-value pair whose value is not null is returned.

```SQL

mysql> select map_filter((k,v) -> v is not null,col_map) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+------------------------------------------------+
| map_filter((k,v) -> v is not null, col_map)    |
+------------------------------------------------+
| {1:"ab",3:"cdd",null:'abc'}                        |
+------------------------------------------------+
1 row in set (0.02 sec)
```
