---
displayed_sidebar: docs
---

# sign

`x` の符号を返します。入力が負の数、0、または正の数の場合、それぞれ `-1`、`0`、または `1` が出力されます。

## Syntax

```Haskell
SIGN(x);
```

## Parameters

`x`: DOUBLE データ型をサポートします。

## Return value

FLOAT データ型の値を返します。

## Examples

```Plain
mysql> select sign(3.14159);
+---------------+
| sign(3.14159) |
+---------------+
|             1 |
+---------------+
1 row in set (0.02 sec)
```