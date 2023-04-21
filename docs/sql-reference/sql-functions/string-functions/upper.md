# UPPER

## Description

Converts a string to upper-case.

## Syntax

```haskell
upper(str)
```

## Parameters

- `str`: The string that needs to be converted. If `str` is not a string, implicit cast will be performed first.

## Return value

Returns upper-case string.

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
