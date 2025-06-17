---
displayed_sidebar: docs
---

# positive

`x` をそのままの値として返します。

## Syntax

```Haskell
POSITIVE(x);
```

## Parameters

`x`: BIGINT、DOUBLE、DECIMALV2、DECIMAL32、DECIMAL64、および DECIMAL128 データ型をサポートします。

## Return value

`x` のデータ型と同じデータ型の値を返します。

## Examples

```Plain
mysql> select positive(3);
+-------------+
| positive(3) |
+-------------+
|           3 |
+-------------+
1 row in set (0.01 sec)

mysql> select positive(cast(3.14 as decimalv2));
+--------------------------------------+
| positive(CAST(3.14 AS DECIMAL(9,0))) |
+--------------------------------------+
|                                 3.14 |
+--------------------------------------+
1 row in set (0.01 sec)
```