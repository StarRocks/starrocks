---
displayed_sidebar: docs
---

# cbrt

引数の立方根を計算します。

この関数は v3.3 以降でサポートされています。

## 構文

```Haskell
DOUBLE cbrt(DOUBLE arg)
```

## パラメータ

`arg`: 数値のみを指定できます。この関数は、数値を立方根を計算する前に DOUBLE 値に変換します。

## 戻り値

DOUBLE データ型の値を返します。非数値を指定した場合、この関数は `NULL` を返します。

## 例

```Plain
mysql> select cbrt(8);
+---------+
| cbrt(8) |
+---------+
|       2 |
+---------+

mysql> select cbrt(-8);
+----------+
| cbrt(-8) |
+----------+
|       -2 |
+----------+

mysql> select cbrt(0);
+---------+
| cbrt(0) |
+---------+
|       0 |
+---------+

mysql> select cbrt("");
+----------+
| cbrt('') |
+----------+
|     NULL |
+----------+
```

## キーワード

cbrt, 立方根