---
displayed_sidebar: docs
---

# locate

## 説明

この関数は、文字列内の部分文字列の位置を見つけるために使用されます（1から数え始め、文字単位で測定されます）。第3引数の pos が指定されている場合、pos より下の位置で substr の位置を見つけ始めます。str が見つからない場合は、0 を返します。

## 構文

```Haskell
INT locate(VARCHAR substr, VARCHAR str[, INT pos])
```

## 例

```Plain Text
MySQL > SELECT LOCATE('bar', 'foobarbar');
+----------------------------+
| locate('bar', 'foobarbar') |
+----------------------------+
|                          4 |
+----------------------------+

MySQL > SELECT LOCATE('xbar', 'foobar');
+--------------------------+
| locate('xbar', 'foobar') |
+--------------------------+
|                        0 |
+--------------------------+

MySQL > SELECT LOCATE('bar', 'foobarbar', 5);
+-------------------------------+
| locate('bar', 'foobarbar', 5) |
+-------------------------------+
|                             7 |
+-------------------------------+
```

## キーワード

LOCATE