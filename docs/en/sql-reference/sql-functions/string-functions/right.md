---
displayed_sidebar: docs
---

# right

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

This function returns a specified length of characters from the right side of a given string. Length unit: utf8 character.
Note: This function is also named as [strright](strright.md).

## Syntax

```SQL
VARCHAR right(VARCHAR str,INT len)
```

## Examples

```SQL
MySQL > select right("Hello starrocks",9);
+-----------------------------+
| right('Hello starrocks', 9) |
+-----------------------------+
| starrocks                   |
+-----------------------------+
```

## keyword

RIGHT
