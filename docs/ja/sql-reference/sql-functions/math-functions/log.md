---
displayed_sidebar: docs
---

# log

数値の対数を指定された基数（または基底）で計算します。基数が指定されていない場合、この関数は [ln](../math-functions/ln.md) と同等です。

## Syntax

```SQL
log([base,] arg)
```

## Parameters

- `base`: 任意。基数です。DOUBLE データ型のみサポートされています。このパラメータが指定されていない場合、この関数は [ln](../math-functions/ln.md) と同等です。

> **NOTE**
>
> StarRocks は、`base` が負の数、0、または 1 に指定された場合、NULL を返します。

- `arg`: 対数を計算したい値です。DOUBLE データ型のみサポートされています。

> **NOTE**
>
> StarRocks は、`arg` が負の数または 0 に指定された場合、NULL を返します。

## Return value

DOUBLE データ型の値を返します。

## Example

例 1: 8 の対数を基数 2 で計算します。

```Plain
mysql> select log(2,8);
+-----------+
| log(2, 8) |
+-----------+
|         3 |
+-----------+
1 row in set (0.01 sec)
```

例 2: 10 の対数を基数 *e* で計算します（基数は指定されていません）。

```Plain
mysql> select log(10);
+-------------------+
| log(10)           |
+-------------------+
| 2.302585092994046 |
+-------------------+
1 row in set (0.09 sec)
```