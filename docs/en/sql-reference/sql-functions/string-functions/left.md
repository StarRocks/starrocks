---
displayed_sidebar: docs
---

# left

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

This function returns a specified number of characters from the left side of a given string. The unit for length: utf8 character.
Note: This function is also named as [strleft](strleft.md).

## Syntax

```SQL
VARCHAR left(VARCHAR str,INT len)
```

## Examples

```SQL
MySQL > select left("Hello starrocks",5);
+----------------------------+
| left('Hello starrocks', 5) |
+----------------------------+
| Hello                      |
+----------------------------+
```

## keyword

LEFT
