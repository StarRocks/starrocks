---
displayed_sidebar: docs
---

# floor, dfloor

## 説明

`x` 以下の最大の整数を返します。

## 構文

```SQL
FLOOR(x);
```

## パラメータ

`x`: DOUBLE がサポートされています。

## 戻り値

BIGINT データ型の値を返します。

## 例

```Plaintext
mysql> select floor(3.14);
+-------------+
| floor(3.14) |
+-------------+
|           3 |
+-------------+
1 row in set (0.01 sec)
```