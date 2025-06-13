---
displayed_sidebar: docs
---

# sin

`x` のサインを返します。`x` はラジアンで指定します。

## Syntax

```Haskell
SIN(x);
```

## Parameters

`x`: DOUBLE データ型をサポートします。

## Return value

DOUBLE データ型の値を返します。

## Examples

```Plain
mysql> select sin(3.14);
+-----------------------+
| sin(3.14)             |
+-----------------------+
| 0.0015926529164868282 |
+-----------------------+
1 row in set (0.21 sec)
```