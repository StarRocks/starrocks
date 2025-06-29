---
displayed_sidebar: docs
---

# radians

`x` を角度からラジアンに変換します。

## Syntax

```Haskell
REDIANS(x);
```

## Parameters

`x`: DOUBLE データ型をサポートします。

## Return value

DOUBLE データ型の値を返します。

## Examples

```Plain
mysql> select radians(90);
+--------------------+
| radians(90)        |
+--------------------+
| 1.5707963267948966 |
+--------------------+
1 row in set (0.00 sec)
```