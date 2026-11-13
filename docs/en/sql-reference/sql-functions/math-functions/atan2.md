---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the arc tangent of x divided by y, that is, the arc tangent of x/y."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# atan2



Returns the arc tangent of `x` divided by `y`, that is, the arc tangent of `x/y`. The signs of the two parameters are used to determine the quadrant of the result.

The return value is in the range of [-π, π].

## Syntax

```plaintext
ATAN2(x,y);
```

## Parameters

`x`: The supported data type is DOUBLE.

`y`: The supported data type is DOUBLE.

## Return value

Returns a value of the DOUBLE type. Returns NULL if `x` or `y` is NULL.

## Examples

```Plain Text
mysql> select atan2(-0.8,2);
+---------------------+
| atan2(-0.8, 2)      |
+---------------------+
| -0.3805063771123649 |
+---------------------+
1 row in set (0.01 sec)
```
