---
displayed_sidebar: docs
---

# square

値の平方を計算します。

## Syntax

```Haskell
square(arg)
```

### Parameters

`arg`: 平方を計算したい値を指定します。数値のみを指定できます。この関数は、数値を平方計算する前に DOUBLE 値に変換します。

## Return value

DOUBLE データ型の値を返します。

## Usage notes

非数値を指定した場合、この関数は `NULL` を返します。

## Examples

例 1: 数値の平方を計算します。

```Plain
mysql>  select square(11);
+------------+
| square(11) |
+------------+
|        121 |
+------------+
```

例 2: 非数値の平方を計算します。返される値は `NULL` です。

```Plain
mysql>  select square('2021-01-01');
+----------------------+
| square('2021-01-01') |
+----------------------+
|                 NULL |
+----------------------+
```
