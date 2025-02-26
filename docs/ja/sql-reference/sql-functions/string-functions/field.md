---
displayed_sidebar: docs
---

# field

str1, str2, str3, ... のリストの中で、str のインデックス（位置）を返します。str が見つからない場合は 0 を返します。

この関数は v3.5 からサポートされています。

## 構文

```Haskell
INT field(expr1, ...);
```

## 例

```Plain Text
MYSQL > select field('a', 'b', 'a', 'd');
+---------------------------+
| field('a', 'b', 'a', 'd') |
+---------------------------+
|                         2 |
+---------------------------+
```

## キーワード

FIELD