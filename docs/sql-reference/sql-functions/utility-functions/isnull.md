# ISNULL

## Description

Checks whether the value is `NULL`, returns `1` if it is `NULL`, and returns `0` if it is not `NULL`.

## Syntax

```Haskell
ISNULL(v)
```

## Parameters

- `v`: the value to check. All date typs is supported.

## Return value

Returns 1 if it is `NULL`, and returns 0 if it is not `NULL`.

## Example

```plain text
MYSQL > SELECT c2, isnull(c2) FROM t1;
+------+--------------+
| c2   | `c2` IS NULL |
+------+--------------+
| NULL |            1 |
|    1 |            0 |
+------+--------------+
```
