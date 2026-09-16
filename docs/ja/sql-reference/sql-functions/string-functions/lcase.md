---
displayed_sidebar: docs
description: "文字列を小文字に変換します。lcase関数と同じです。"
---

# lcase

この関数は文字列を小文字に変換します。lower 関数と同様です。

## Syntax

```Haskell
VARCHAR lcase(VARCHAR str)
```

## Examples

```Plain Text
mysql> SELECT lcase("AbC123");
+-----------------+
|lcase('AbC123')  |
+-----------------+
|abc123           |
+-----------------+
```

## keyword

LCASE