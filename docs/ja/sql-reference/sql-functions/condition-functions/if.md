---
displayed_sidebar: docs
---

# if

## 説明

`expr1` が TRUE と評価される場合、`expr2` を返します。それ以外の場合は、`expr3` を返します。

## 構文

```Haskell
if(expr1,expr2,expr3);
```

## パラメータ

`expr1`: 条件です。BOOLEAN 値である必要があります。

`expr2`: 条件が真の場合に返される値です。この式は次のデータ型のいずれかに評価される必要があります: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DATETIME, DATE, DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128, VARCHAR, BITMAP, PERCENTILE, HLL, TIME。

`expr3`: 条件が偽の場合に返される値です。データ型は `expr2` と同じです。

> `expr2` と `expr3` はデータ型が一致している必要があります。

## 戻り値

戻り値は `expr2` と同じ型です。

## 例

```Plain Text
mysql> select if(false,1,2);
+-----------------+
| if(FALSE, 1, 2) |
+-----------------+
|               2 |
+-----------------+
1 row in set (0.00 sec)
```