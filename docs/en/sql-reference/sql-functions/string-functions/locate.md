---
displayed_sidebar: docs
description: "Returns the 1-based character position of a substring within a string."
---

# locate



This function is used for finding the location of a substring in a string (starting counting from 1 and measured in characters). If the third argument pos is specified, it will start to find positions of substr in strings below pos. If str is not found, it will return 0.

## Syntax

```Haskell
INT locate(VARCHAR substr, VARCHAR str[, INT pos])
```

## Examples

```Plain Text
MySQL > SELECT LOCATE('bar', 'foobarbar');
+----------------------------+
| locate('bar', 'foobarbar') |
+----------------------------+
|                          4 |
+----------------------------+

MySQL > SELECT LOCATE('xbar', 'foobar');
+--------------------------+
| locate('xbar', 'foobar') |
+--------------------------+
|                        0 |
+--------------------------+

MySQL > SELECT LOCATE('bar', 'foobarbar', 5);
+-------------------------------+
| locate('bar', 'foobarbar', 5) |
+-------------------------------+
|                             7 |
+-------------------------------+
```

## keyword

LOCATE
