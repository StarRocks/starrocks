---
displayed_sidebar: docs
---

# ascii

この関数は、指定された文字列の最左の文字の ASCII 値を返します。

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