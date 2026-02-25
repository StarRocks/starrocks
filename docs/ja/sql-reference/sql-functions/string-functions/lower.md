---
displayed_sidebar: docs
---

# lower

引数内のすべての文字列を小文字に変換します。

## 構文

```Haskell
INT lower(VARCHAR str)
```

## 例

```Plain Text
MySQL > SELECT lower("AbC123");
+-----------------+
| lower('AbC123') |
+-----------------+
| abc123          |
+-----------------+
```

## キーワード

LOWER