---
displayed_sidebar: docs
---

# log2

数値の底 2 の対数を計算します。

## Syntax

```SQL
log2(arg)
```

## Parameters

- `arg`: 対数を計算したい値。DOUBLE データ型のみサポートされています。

> **NOTE**
>
> StarRocks は、`arg` が負または 0 に指定された場合、NULL を返します。

## Return value

DOUBLE データ型の値を返します。

## Example

例 1: 8 の底 2 の対数を計算します。

```Plain
mysql> select log2(8);
+---------+
| log2(8) |
+---------+
|       3 |
+---------+
1 row in set (0.00 sec)
```