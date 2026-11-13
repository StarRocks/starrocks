---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the bitwise AND of two numeric expressions."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitand



Returns the bitwise AND of two numeric expressions.

## Syntax

```plaintext
BITAND(x,y);
```

## Parameters

- `x`: This expression must evaluate to any of the following data types: TINYINT, SMALLINT, INT, BIGINT, LARGEINT.

- `y`: This expression must evaluate to any of the following data types: TINYINT, SMALLINT, INT, BIGINT, LARGEINT.

> `x` and `y` must agree in data type.

## Return value

The return value has the same type as `x`. If any value is NULL, the result is NULL.

## Examples

```Plain Text
mysql> select bitand(3,0);
+--------------+
| bitand(3, 0) |
+--------------+
|            0 |
+--------------+
1 row in set (0.01 sec)
```
