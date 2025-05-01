---
displayed_sidebar: docs
---

# bitor

## 説明

2つの数値式のビット単位のORを返します。

## 構文

```Haskell
BITOR(x,y);
```

## パラメータ

- `x`: この式は、次のデータ型のいずれかに評価される必要があります: TINYINT, SMALLINT, INT, BIGINT, LARGEINT。

- `y`: この式は、次のデータ型のいずれかに評価される必要があります: TINYINT, SMALLINT, INT, BIGINT, LARGEINT。

> `x` と `y` はデータ型が一致している必要があります。

## 戻り値

戻り値は `x` と同じ型です。いずれかの値がNULLの場合、結果はNULLです。

## 例

```Plain Text
mysql> select bitor(3,0);
+-------------+
| bitor(3, 0) |
+-------------+
|           3 |
+-------------+
1 row in set (0.00 sec)
```