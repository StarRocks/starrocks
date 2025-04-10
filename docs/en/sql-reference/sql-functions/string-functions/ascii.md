---
displayed_sidebar: docs
---

# ascii

## Description

This function returns the ascii value of the leftmost character of a given string.

## Syntax

```Haskell
INT ascii(VARCHAR str)
```

## Examples

```Plain Text
MySQL > select ascii('1');
+------------+
| ascii('1') |
+------------+
|         49 |
+------------+

MySQL > select ascii('234');
+--------------+
| ascii('234') |
+--------------+
|           50 |
+--------------+
```

## keyword

ASCII
