---
displayed_sidebar: docs
---

# nullif

## 説明

`expr1` が `expr2` と等しい場合、NULL を返します。それ以外の場合は、`expr1` を返します。

## 構文

```Haskell
nullif(expr1,expr2);
```

## パラメータ

`expr1`: この式は、次のいずれかのデータ型に評価される必要があります: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DATETIME, DATE, DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128, VARCHAR, BITMAP, PERCENTILE, HLL, TIME.

`expr2`: `expr1` と同じデータ型に評価される式。

> `expr1` と `expr2` はデータ型が一致している必要があります。

## 戻り値

戻り値は `expr1` と同じ型です。

## 例

```Plain Text
mysql> select nullif(1,2);
+--------------+
| nullif(1, 2) |
+--------------+
|            1 |
+--------------+

select nullif(1,1);
+--------------+
| nullif(1, 1) |
+--------------+
|         NULL |
+--------------+
```