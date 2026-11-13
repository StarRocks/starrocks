---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Computes the arc tangent of the argument."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# atan



Computes the arc tangent of the argument.

## Syntax

```plaintext
DOUBLE atan(DOUBLE arg)
```

### Parameters

`arg`: You can specify only a numeric value. This function converts the numeric value into a DOUBLE value before it computes the arc tangent of the value.

## Return value

Returns a value of the DOUBLE data type.

## Usage notes

If you specify a non-numeric value, this function returns `NULL`.

## Examples

```Plain
mysql> select atan(1);
+--------------------+
| atan(1)            |
+--------------------+
| 0.7853981633974483 |
+--------------------+

mysql> select atan(0);
+---------+
| atan(0) |
+---------+
|       0 |
+---------+

mysql> select atan(-1);
+---------------------+
| atan(-1)            |
+---------------------+
| -0.7853981633974483 |
+---------------------+

mysql> select atan("");
+----------+
| atan('') |
+----------+
|     NULL |
+----------+
```

## keyword

ATAN
