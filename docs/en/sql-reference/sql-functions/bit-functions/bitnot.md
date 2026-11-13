---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the bitwise negation of a numeric expression."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitnot



Returns the bitwise negation of a numeric expression.

## Syntax

```plaintext
BITNOT(x);
```

## Parameters

`x`: This expression must evaluate to any of the following data types: TINYINT, SMALLINT, INT, BIGINT, LARGEINT.

## Return value

The return value has the same type as `x`. If any value is NULL, the result is NULL.

## Examples

```Plain Text
mysql> select bitnot(3);
+-----------+
| bitnot(3) |
+-----------+
|        -4 |
+-----------+
```
