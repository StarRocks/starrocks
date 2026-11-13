---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Converts the angle in radians x to degrees."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# degrees



Converts the angle in radians `x` to degrees.

## Syntax

```SQL
DEGREES(x);
```

## Parameters

`x`: the angle in radians. DOUBLE is supported.

## Return value

Returns a value of the DOUBLE data type.

## Examples

```Plaintext
mysql> select degrees(3.1415926535898);
+--------------------------+
| degrees(3.1415926535898) |
+--------------------------+
|        180.0000000000004 |
+--------------------------+
1 row in set (0.07 sec)
```
