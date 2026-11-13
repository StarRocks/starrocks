---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Computes the cosine of the argument."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# cos



Computes the cosine of the argument.

## Syntax

```plaintext
DOUBLE cos(DOUBLE arg)
```

### Parameters

`arg`: You can specify only a numeric value. This function converts the numeric value into a DOUBLE value before it computes the cosine of the value.

## Return value

Returns a value of the DOUBLE data type.

## Usage notes

If you specify a non-numeric value, this function returns `NULL`.

## Examples

```Plain
mysql> select cos(-1);
+--------------------+
| cos(-1)            |
+--------------------+
| 0.5403023058681398 |
+--------------------+

mysql> select cos(0);
+--------+
| cos(0) |
+--------+
|      1 |
+--------+

mysql> select cos(1);
+--------------------+
| cos(1)             |
+--------------------+
| 0.5403023058681398 |
+--------------------+

mysql> select cos("");
+---------+
| cos('') |
+---------+
|    NULL |
+---------+
```

## keyword

COS
