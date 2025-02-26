---
displayed_sidebar: docs
---

# pow, power, dpow, fpow

`x` を `y` 乗した結果を返します。

## 構文

```Haskell
POW(x,y);POWER(x,y);
```

## パラメータ

`x`: DOUBLE データ型をサポートします。

`y`: DOUBLE データ型をサポートします。

## 戻り値

DOUBLE データ型の値を返します。

## 例

```Plain
mysql> select pow(2,2);
+-----------+
| pow(2, 2) |
+-----------+
|         4 |
+-----------+
1 row in set (0.00 sec)

mysql> select power(4,3);
+-------------+
| power(4, 3) |
+-------------+
|          64 |
+-------------+
1 row in set (0.00 sec)
```