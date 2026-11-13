---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Calculates the base 10 logarithm of a number."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# log10, dlog10



Calculates the base 10 logarithm of a number.

## Syntax

```SQL
log10(arg)
```

## Parameters

- `arg`: The value whose logarithm you want to calculate. Only DOUBLE data type is supported.

> **NOTE**
>
> StarRocks returns NULL if `arg` is specified as a negative or 0.

## Return value

Returns a value of the DOUBLE data type.

## Example

Example 1: Calculate the base 10 logarithm of 100.

```Plain
select log10(100);
+------------+
| log10(100) |
+------------+
|          2 |
+------------+
1 row in set (0.02 sec)
```
