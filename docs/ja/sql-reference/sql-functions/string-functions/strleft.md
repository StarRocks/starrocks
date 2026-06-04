---
displayed_sidebar: docs
description: "文字列の左側から指定された文字数を抽出します。left関数のエイリアスです。"
---

# strleft

この関数は、指定された長さの文字数を文字列の左から抽出します。長さの単位は utf8 文字です。注意: この関数は [left](left.md) とも呼ばれます。

## Syntax

```SQL
VARCHAR strleft(VARCHAR str,INT len)
```

## Examples

```SQL
MySQL > select strleft("Hello starrocks",5);
+-------------------------------+
| strleft('Hello starrocks', 5) |
+-------------------------------+
| Hello                         |
+-------------------------------+
```

## keyword

STRLEFT