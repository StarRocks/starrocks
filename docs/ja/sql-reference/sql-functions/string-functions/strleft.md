---
displayed_sidebar: docs
---

# strleft

## 説明

この関数は、指定された長さの文字数を文字列から抽出します（左から開始）。長さの単位は utf8 文字です。
注: この関数は [left](left.md) とも呼ばれます。

## 構文

```SQL
VARCHAR strleft(VARCHAR str,INT len)
```

## 例

```SQL
MySQL > select strleft("Hello starrocks",5);
+-------------------------------+
| strleft('Hello starrocks', 5) |
+-------------------------------+
| Hello                         |
+-------------------------------+
```

## キーワード

STRLEFT