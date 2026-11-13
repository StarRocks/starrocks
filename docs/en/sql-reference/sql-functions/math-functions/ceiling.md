---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns values from the input arg rounded to the nearest equal or larger integer."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# ceiling



Returns values from the input `arg` rounded to the nearest equal or larger integer.

## Syntax

```Shell
ceiling(arg)
```

## Parameter

`arg` supports the DOUBLE data type.

## Return value

Returns a value of the BIGINT data type.

## Examples

```Plain
mysql> select ceiling(3.14);
+---------------+
| ceiling(3.14) |
+---------------+
|             4 |
+---------------+
1 row in set (0.00 sec)
```
