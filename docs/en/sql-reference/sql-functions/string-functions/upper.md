---
displayed_sidebar: docs
---

# upper

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

Converts a string to upper-case.

## Syntax

```haskell
upper(str)
```

## Parameters

- `str`: the string to convert. If `str` is not a string type, it will try implicit cast first.

## Return values

Return an upper-case string.

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
