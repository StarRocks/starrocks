---
displayed_sidebar: docs
---

# concat

この関数は複数の文字列を結合します。パラメータの値が NULL の場合、NULL を返します。

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