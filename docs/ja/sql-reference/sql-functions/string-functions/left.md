---
displayed_sidebar: docs
---

# left

この関数は、指定された文字数分、与えられた文字列の左側から文字を返します。長さの単位は utf8 文字です。
注意: この関数は [strleft](strleft.md) とも呼ばれます。

## Syntax

```SQL
VARCHAR left(VARCHAR str,INT len)
```

## Examples

```SQL
MySQL > select left("Hello starrocks",5);
+----------------------------+
| left('Hello starrocks', 5) |
+----------------------------+
| Hello                      |
+----------------------------+
```

## keyword

LEFT