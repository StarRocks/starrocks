---
displayed_sidebar: docs
---

# right

この関数は、指定された長さの文字を、与えられた文字列の右側から返します。長さの単位は utf8 文字です。
注意: この関数は [strright](strright.md) とも呼ばれます。

## 構文

```SQL
VARCHAR right(VARCHAR str,INT len)
```

## 例

```SQL
MySQL > select right("Hello starrocks",9);
+-----------------------------+
| right('Hello starrocks', 9) |
+-----------------------------+
| starrocks                   |
+-----------------------------+
```

## キーワード

RIGHT