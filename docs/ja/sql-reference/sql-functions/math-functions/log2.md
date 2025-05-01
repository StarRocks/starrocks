---
displayed_sidebar: docs
---

# log2

## 説明

数値の底 2 の対数を計算します。

## 構文

```SQL
log2(arg)
```

## パラメータ

- `arg`: 対数を計算したい値です。DOUBLE データ型のみサポートされています。

> **注意**
>
> StarRocks は、`arg` が負または 0 に指定された場合、NULL を返します。

## 戻り値

DOUBLE データ型の値を返します。

## 例

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