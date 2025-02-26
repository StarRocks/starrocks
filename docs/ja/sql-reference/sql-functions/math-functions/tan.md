---
displayed_sidebar: docs
---

# tan

`x` の正接を返します。ここで、`x` はラジアンで指定します。

## Syntax

```Haskell
TAN(x);
```

## Parameters

`x`: DOUBLE データ型をサポートします。

## Return value

DOUBLE データ型の値を返します。

## Examples

```Plain
mysql> select tan(3.14);
+-----------------------+
| tan(3.14)             |
+-----------------------+
| -0.001592654936407223 |
+-----------------------+
1 row in set (0.12 sec)
```