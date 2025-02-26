---
displayed_sidebar: docs
---

# floor, dfloor

`x` 以下の最大の整数を返します。

## Syntax

```SQL
FLOOR(x);
```

## Parameters

`x`: DOUBLE がサポートされています。

## Return value

BIGINT データ型の値を返します。

## Examples

```Plaintext
mysql> select floor(3.14);
+-------------+
| floor(3.14) |
+-------------+
|           3 |
+-------------+
1 row in set (0.01 sec)
```