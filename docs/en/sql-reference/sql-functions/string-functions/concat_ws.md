---
displayed_sidebar: docs
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
MySQL > select concat_ws("Rock", "Star", "s");
+--------------------------------+
| concat_ws('Rock', 'Star', 's') |
+--------------------------------+
| StarRocks                      |
+--------------------------------+

MySQL > select concat_ws(NULL, "Star", "s");
+------------------------------+
| concat_ws(NULL, 'Star', 's') |
+------------------------------+
| NULL                         |
+------------------------------+
1 row in set (0.01 sec)

MySQL > StarRocks > select concat_ws("Rock", "Star", NULL, "s");
+--------------------------------------+
| concat_ws('Rock', 'Star', NULL, 's') |
+--------------------------------------+
| StarRocks                            |
+--------------------------------------+
1 row in set (0.04 sec)
```

## keyword

CONCAT_WS,CONCAT,WS
