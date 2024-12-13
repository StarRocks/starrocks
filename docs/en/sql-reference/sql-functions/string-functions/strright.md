---
displayed_sidebar: docs
---

# strright

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

This function extracts a number of characters from a string with specified length (starting from right). The unit for length: utf-8 character.
Note: This function is also named as [right](right.md).

## Syntax

```SQL
VARCHAR strright(VARCHAR str,INT len)
```

## Examples

```SQL
MySQL > select strright("Hello starrocks",9);
+--------------------------------+
| strright('Hello starrocks', 9) |
+--------------------------------+
| starrocks                      |
+--------------------------------+
```

## keyword

STRRIGHT
