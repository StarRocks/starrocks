---
displayed_sidebar: "English"
---

# concat

## Description

This function combines multiple strings. If any of the parameter value is NULL, it will return NULL.

## Syntax

```Haskell
VARCHAR concat(VARCHAR,...)
```

## Examples

```Plain Text
MySQL > select concat("a", "b");
+------------------+
| concat('a', 'b') |
+------------------+
| ab               |
+------------------+

MySQL > select concat("a", "b", "c");
+-----------------------+
| concat('a', 'b', 'c') |
+-----------------------+
| abc                   |
+-----------------------+

MySQL > select concat("a", null, "c");
+------------------------+
| concat('a', NULL, 'c') |
+------------------------+
| NULL                   |
+------------------------+
```

## keyword

CONCAT
