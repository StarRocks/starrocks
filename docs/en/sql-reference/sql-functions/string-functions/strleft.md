---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns a specified number of characters from the left of a string; alias of left."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# strleft



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
