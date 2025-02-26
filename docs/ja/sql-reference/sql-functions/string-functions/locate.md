---
displayed_sidebar: docs
---

# locate

この関数は、文字列内の部分文字列の位置を見つけるために使用されます（1から数え始め、文字単位で測定します）。第3引数の pos が指定されている場合、pos より後の位置で substr を見つけ始めます。str が見つからない場合は、0 を返します。

## Syntax

```Haskell
INT locate(VARCHAR substr, VARCHAR str[, INT pos])
```

## Examples

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

## keyword

LOCATE