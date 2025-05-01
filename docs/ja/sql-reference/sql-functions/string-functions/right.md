---
displayed_sidebar: docs
---

# right

## Description

この関数は、指定された長さの文字を与えられた文字列の右側から返します。長さの単位は utf8 文字です。  
注: この関数は [strright](strright.md) とも呼ばれます。

## Syntax

```SQL
VARCHAR right(VARCHAR str,INT len)
```

## Examples

```SQL
MySQL > select right("Hello starrocks",9);
+-----------------------------+
| right('Hello starrocks', 9) |
+-----------------------------+
| starrocks                   |
+-----------------------------+
```

## keyword

RIGHT