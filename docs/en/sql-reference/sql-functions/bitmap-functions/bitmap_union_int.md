---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Count the number of different values ​​in columns of type TINYINT, SMALLINT and INT, return the sum of COUNT (DISTINCT expr) same."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitmap_union_int



Count the number of different values ​​in columns of type TINYINT, SMALLINT and INT, return the sum of COUNT (DISTINCT expr) same.

## Syntax

```plaintext
BIGINT bitmap_union_int(expr)
```

### Parameters

`expr`: column expression. The supported column type is TINYINT, SMALLINT and INT.

## Return value

Returns a value of the BIGINT type.

## Examples

```Plaintext
mysql> select bitmap_union_int(k1) from tbl1;
+------------------------+
| bitmap_union_int(`k1`) |
+------------------------+
|                      2 |
+------------------------+
```
