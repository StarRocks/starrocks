---
displayed_sidebar: docs
---

# sqrt, dsqrt

値の平方根を計算します。dsqrt は sqrt と同じです。

## Syntax

```Haskell
DOUBLE SQRT(DOUBLE x);
DOUBLE DSQRT(DOUBLE x);
```

## Parameters

`x`: 数値のみを指定できます。この関数は、計算前に数値を DOUBLE 値に変換します。

## Return value

DOUBLE データ型の値を返します。

## Usage notes

数値以外の値を指定した場合、この関数は `NULL` を返します。

## Examples

```Plain
mysql> select sqrt(3.14);
+-------------------+
| sqrt(3.14)        |
+-------------------+
| 1.772004514666935 |
+-------------------+
1 row in set (0.01 sec)


mysql> select dsqrt(3.14);
+-------------------+
| dsqrt(3.14)       |
+-------------------+
| 1.772004514666935 |
+-------------------+
```