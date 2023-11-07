# ISNOTNULL

## Description

Checks whether the value is not `NULL`, returns `1` if it is not `NULL`, and returns `0` if it is `NULL`.

## Syntax

```Haskell
ISNOTNULL(v)
```

## Parameters

- `v`: the value to check. All date types is supported.

## Return value

Returns 1 if it is not `NULL`, and returns 0 if it is `NULL`.

## Examples

```plain text
MYSQL > SELECT c1, isnotnull(c1) FROM t1;
+------+--------------+
| c1   | `c1` IS NULL |
+------+--------------+
| NULL |            0 |
|    1 |            1 |
+------+--------------+
```
