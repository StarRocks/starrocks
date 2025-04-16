---
displayed_sidebar: docs
---

# bitnot

数値式のビット単位の否定を返します。

## 構文

```Haskell
BITNOT(x);
```

## パラメータ

`x`: この式は、TINYINT、SMALLINT、INT、BIGINT、LARGEINT のいずれかのデータ型に評価される必要があります。

## 戻り値

戻り値は `x` と同じ型です。いずれかの値が NULL の場合、結果は NULL です。

## 例

```Plain Text
mysql> select bitnot(3);
+-----------+
| bitnot(3) |
+-----------+
|        -4 |
+-----------+
```