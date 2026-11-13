---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Converts a string to upper-case."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# upper



Converts a string to upper-case.

## Syntax

```plaintext
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
