---
displayed_sidebar: docs
---

# lcase

## 説明

この関数は文字列を小文字に変換します。関数 lower と同様です。

## 構文

```Haskell
VARCHAR lcase(VARCHAR str)
```

## 例

```Plain Text
mysql> SELECT lcase("AbC123");
+-----------------+
|lcase('AbC123')  |
+-----------------+
|abc123           |
+-----------------+
```

## キーワード

LCASE