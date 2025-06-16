---
displayed_sidebar: docs
---

# bitor

2 つの数値式のビット単位の OR を返します。

## 構文

```Haskell
BITOR(x,y);
```

## パラメータ

- `x`: この式は、TINYINT、SMALLINT、INT、BIGINT、LARGEINT のいずれかのデータ型に評価される必要があります。

- `y`: この式は、TINYINT、SMALLINT、INT、BIGINT、LARGEINT のいずれかのデータ型に評価される必要があります。

> `x` と `y` はデータ型が一致している必要があります。

## 戻り値

戻り値は `x` と同じ型です。いずれかの値が NULL の場合、結果は NULL です。

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