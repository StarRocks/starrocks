---
displayed_sidebar: docs
---

# length

この関数は文字列の長さをバイト単位で返します。

## 構文

```Haskell
INT length(VARCHAR str)
```

## 例

```Plain Text
MySQL > select length("abc");
+---------------+
| length('abc') |
+---------------+
|             3 |
+---------------+
```

## キーワード

LENGTH