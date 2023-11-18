---
displayed_sidebar: "English"
---

# strright

## Description

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
