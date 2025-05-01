---
displayed_sidebar: docs
---

# length

## 説明

この関数は、文字列の長さ（バイト数）を返します。

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