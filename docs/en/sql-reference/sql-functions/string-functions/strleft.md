---
displayed_sidebar: docs
---

# strleft

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

This function extracts a number of characters from a string with specified length (starting from left). The unit for length: utf8 character.
Note: This function is also named as [left](left.md).

## Syntax

```SQL
VARCHAR strleft(VARCHAR str,INT len)
```

## Examples

```SQL
MySQL > select strleft("Hello starrocks",5);
+-------------------------------+
| strleft('Hello starrocks', 5) |
+-------------------------------+
| Hello                         |
+-------------------------------+
```

## keyword

STRLEFT
