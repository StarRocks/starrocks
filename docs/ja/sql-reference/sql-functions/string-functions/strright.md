---
displayed_sidebar: docs
---

# strright

この関数は、指定された長さの文字数を文字列の右端から抽出します。長さの単位は utf-8 文字です。注意: この関数は [right](right.md) とも呼ばれます。

## Syntax

```SQL
VARCHAR strright(VARCHAR str,INT len)
```

## 例

```SQL
MySQL > select strright("Hello starrocks",9);
+--------------------------------+
| strright('Hello starrocks', 9) |
+--------------------------------+
| starrocks                      |
+--------------------------------+
```

## キーワード

STRRIGHT