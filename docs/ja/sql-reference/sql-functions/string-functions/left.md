---
displayed_sidebar: docs
---

# left

## 説明

この関数は、指定された文字数分、与えられた文字列の左側から文字を返します。長さの単位は utf8 文字です。
注意: この関数は [strleft](strleft.md) とも呼ばれます。

## 構文

```SQL
VARCHAR left(VARCHAR str,INT len)
```

## 例

```SQL
MySQL > select left("Hello starrocks",5);
+----------------------------+
| left('Hello starrocks', 5) |
+----------------------------+
| Hello                      |
+----------------------------+
```

## キーワード

LEFT