---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns a specified number of characters from the right of a string; alias of right."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# strright



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
