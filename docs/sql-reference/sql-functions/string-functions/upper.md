# UPPER

## Description

This function converts a string to upper-case.

## Syntax

```haskell
upper(str)
```

## Parameters

- `str`：The string that need to be converted. If `str` is not a string type, it will try implicit cast first.

## Return values

Return upper-case string.

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
