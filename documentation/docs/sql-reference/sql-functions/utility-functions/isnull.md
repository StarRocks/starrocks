# ISNULL

## Description

Checks whether the value is `NULL`, returns `1` if it is `NULL`, and returns `0` if it is not `NULL`.

## Syntax

```Haskell
ISNULL(v)
```

## Parameters

- `v`: the value to check. All date types is supported.

## Return value

Returns 1 if it is `NULL`, and returns 0 if it is not `NULL`.

## Example

```plain text
MYSQL > SELECT c1, isnull(c1) FROM t1;
+------+--------------+
| c1   | `c1` IS NULL |
+------+--------------+
| NULL |            1 |
|    1 |            0 |
+------+--------------+
```
