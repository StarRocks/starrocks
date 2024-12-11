---
displayed_sidebar: docs
---

# left

<<<<<<< HEAD
## Description
=======

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

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
