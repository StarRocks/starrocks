---
displayed_sidebar: docs
---

# acos

引数のアークコサインを計算します。

## Syntax

```Haskell
DOUBLE acos(DOUBLE arg)
```

### Parameters

`arg`: 数値のみを指定できます。この関数は、数値をアークコサインを計算する前に DOUBLE 値に変換します。

## Return value

DOUBLE データ型の値を返します。

## Usage notes

非数値を指定した場合、この関数は `NULL` を返します。

## Examples

```Plain
mysql> select acos(-1);
+-------------------+
| acos(-1)          |
+-------------------+
| 3.141592653589793 |
+-------------------+

mysql> select acos(0);
+--------------------+
| acos(0)            |
+--------------------+
| 1.5707963267948966 |
+--------------------+

mysql> select acos(1);
+---------+
| acos(1) |
+---------+
|       0 |
+---------+

mysql> select acos("");
+----------+
| acos('') |
+----------+
|     NULL |
+----------+
```

## keyword

ACOS