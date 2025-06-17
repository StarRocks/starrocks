---
displayed_sidebar: docs
---

# exp, dexp

e を `x` 乗した値を返します。この関数は自然対数関数と呼ばれます。

## Syntax

```SQL
EXP(x);
```

## Parameters

`x`: 指数の数値。DOUBLE がサポートされています。

## Return value

DOUBLE データ型の値を返します。

## Examples

e を 3.14 乗した値を返します。

```Plaintext
mysql> select exp(3.14);
+--------------------+
| exp(3.14)          |
+--------------------+
| 23.103866858722185 |
+--------------------+
1 row in set (0.01 sec)
```