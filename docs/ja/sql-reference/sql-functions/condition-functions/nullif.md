---
displayed_sidebar: docs
---

# nullif

`expr1` が `expr2` と等しい場合、NULL を返します。それ以外の場合は、`expr1` を返します。

## Syntax

```Haskell
nullif(expr1,expr2);
```

## Parameters

`expr1` と `expr2` はデータ型が互換性がある必要があります。

## Return value

戻り値は `expr1` と同じ型になります。

## Examples

```Plain Text
mysql> select nullif(1,2);
+--------------+
| nullif(1, 2) |
+--------------+
|            1 |
+--------------+

mysql> select nullif(1,1);
+--------------+
| nullif(1, 1) |
+--------------+
|         NULL |
+--------------+
```