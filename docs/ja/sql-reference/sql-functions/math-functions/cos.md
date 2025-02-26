---
displayed_sidebar: docs
---

# cos

引数のコサインを計算します。

## Syntax

```Haskell
DOUBLE cos(DOUBLE arg)
```

### Parameters

`arg`: 数値のみを指定できます。この関数は、数値をコサイン計算する前に DOUBLE 値に変換します。

## Return value

DOUBLE データ型の値を返します。

## Usage notes

非数値を指定した場合、この関数は `NULL` を返します。

## Examples

```Plain
mysql> select cos(-1);
+--------------------+
| cos(-1)            |
+--------------------+
| 0.5403023058681398 |
+--------------------+

mysql> select cos(0);
+--------+
| cos(0) |
+--------+
|      1 |
+--------+

mysql> select cos(1);
+--------------------+
| cos(1)             |
+--------------------+
| 0.5403023058681398 |
+--------------------+

mysql> select cos("");
+---------+
| cos('') |
+---------+
|    NULL |
+---------+
```

## keyword

COS