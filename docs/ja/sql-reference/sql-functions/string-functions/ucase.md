---
displayed_sidebar: docs
---

# ucase

この関数は文字列を大文字に変換します。関数 upper と同様です。

## 構文

```Haskell
VARCHAR ucase(VARCHAR str)
```

## 例

```Plain Text
mysql> SELECT ucase("AbC123");
+-----------------+
|ucase('AbC123')  |
+-----------------+
|ABC123           |
+-----------------+
```

## キーワード

UCASE