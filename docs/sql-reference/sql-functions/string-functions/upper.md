# UPPER

## Description

Converts a string to upper-case.

## Syntax

```haskell
upper(str)
```

## Parameters

<<<<<<< HEAD
- `str`：The string that needs to be converted. If `str` is not a string, implicit cast will be performed first.
=======
- `str`: the string to convert. If `str` is not a string type, it will try implicit cast first.
>>>>>>> 92ca83f56 ([Doc] trim and create table (#16982))

## Return value

<<<<<<< HEAD
Returns upper-case string.
=======
Return an upper-case string.
>>>>>>> 92ca83f56 ([Doc] trim and create table (#16982))

## Examples

```plaintext
MySQL [test]> select C_String, upper(C_String) from ex_iceberg_tbl;
+-------------------+-------------------+
| C_String          | upper(C_String)   |
+-------------------+-------------------+
| Hello, StarRocks! | HELLO, STARROCKS! |
| Hello, World!     | HELLO, WORLD!     |
+-------------------+-------------------+
```
