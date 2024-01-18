---
displayed_sidebar: "English"
---

# right

## Description

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
