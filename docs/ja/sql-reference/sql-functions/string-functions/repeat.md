---
displayed_sidebar: docs
---

# repeat

この関数は、`str` を `count` の回数だけ繰り返します。`count` が 1 未満の場合、空の文字列を返します。`str` または `count` が NULL の場合、NULL を返します。

## Syntax

```Haskell
VARCHAR repeat(VARCHAR str, INT count)
```

## Examples

```Plain Text
MySQL > SELECT repeat("a", 3);
+----------------+
| repeat('a', 3) |
+----------------+
| aaa            |
+----------------+

MySQL > SELECT repeat("a", -1);
+-----------------+
| repeat('a', -1) |
+-----------------+
|                 |
+-----------------+
```

## keyword

REPEAT,