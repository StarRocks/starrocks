---
displayed_sidebar: docs
---

# atan

引数のアークタンジェントを計算します。

## Syntax

```Haskell
DOUBLE atan(DOUBLE arg)
```

### Parameters

`arg`: 数値のみを指定できます。この関数は、数値を DOUBLE 値に変換してから、その値のアークタンジェントを計算します。

## Return value

DOUBLE データ型の値を返します。

## Usage notes

非数値を指定した場合、この関数は `NULL` を返します。

## Examples

```Plain
mysql> select atan(1);
+--------------------+
| atan(1)            |
+--------------------+
| 0.7853981633974483 |
+--------------------+

mysql> select atan(0);
+---------+
| atan(0) |
+---------+
|       0 |
+---------+

mysql> select atan(-1);
+---------------------+
| atan(-1)            |
+---------------------+
| -0.7853981633974483 |
+---------------------+

mysql> select atan("");
+----------+
| atan('') |
+----------+
|     NULL |
+----------+
```

## keyword

ATAN