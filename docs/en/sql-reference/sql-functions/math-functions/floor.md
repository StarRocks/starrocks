---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the largest integer that is not more than x."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# floor, dfloor



Returns the largest integer that is not more than `x`.

## Syntax

```SQL
FLOOR(x);
```

## Parameters

`x`: DOUBLE is supported.

## Return value

Returns a value of the BIGINT data type.

## Examples

```Plaintext
mysql> select floor(3.14);
+-------------+
| floor(3.14) |
+-------------+
|           3 |
+-------------+
1 row in set (0.01 sec)
```
