---
displayed_sidebar: "English"
---

# concat_ws

## Description

This function uses the first argument sep as the separator which combines the second with the rest to form a string. If the separator is NULL, the result is NULL. concat_ws does not skip empty strings, but it will skip NULL values.

## Syntax

```Haskell
VARCHAR concat_ws(VARCHAR sep, VARCHAR str,...)
```

## Examples

```Plain Text
MySQL > select concat_ws("or", "d", "is");
+----------------------------+
| concat_ws('or', 'd', 'is') |
+----------------------------+
| starrocks                      |
+----------------------------+

MySQL > select concat_ws(NULL, "d", "is");
+----------------------------+
| concat_ws(NULL, 'd', 'is') |
+----------------------------+
| NULL                       |
+----------------------------+

MySQL > select concat_ws("or", "d", NULL,"is");
+---------------------------------+
| concat_ws("or", "d", NULL,"is") |
+---------------------------------+
| starrocks                           |
+---------------------------------+
```

## keyword

CONCAT_WS,CONCAT,WS
