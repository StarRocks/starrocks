---
displayed_sidebar: docs
---

# if

`expr1` が TRUE と評価される場合、`expr2` を返します。それ以外の場合は、`expr3` を返します。

## Syntax

```Haskell
if(expr1,expr2,expr3);
```

## Parameters

`expr1`: 条件です。BOOLEAN 値でなければなりません。

`expr2` と `expr3` はデータ型が互換性がある必要があります。

## Return value

返り値は `expr2` と同じ型です。

## Examples

```Plain Text
mysql> select if(true,1,2);
+----------------+
| if(TRUE, 1, 2) |
+----------------+
|              1 |
+----------------+

mysql> select if(false,2.14,2);
+--------------------+
| if(FALSE, 2.14, 2) |
+--------------------+
|               2.00 |
+--------------------+
```
