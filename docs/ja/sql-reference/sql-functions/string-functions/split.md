---
displayed_sidebar: docs
---

# split

この関数は、指定された文字列をセパレータに従って分割し、分割された部分を ARRAY で返します。

## 構文

```SQL
ARRAY<VARCHAR> split(VARCHAR content, VARCHAR delimiter)
```

## 例

```SQL
mysql> select split("a,b,c",",");
+---------------------+
| split('a,b,c', ',') |
+---------------------+
| ["a","b","c"]       |
+---------------------+

mysql> select split("a,b,c",",b,");
+-----------------------+
| split('a,b,c', ',b,') |
+-----------------------+
| ["a","c"]             |
+-----------------------+

mysql> select split("abc","");
+------------------+
| split('abc', '') |
+------------------+
| ["a","b","c"]    |
+------------------+
```

## キーワード

SPLIT