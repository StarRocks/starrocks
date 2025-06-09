---
displayed_sidebar: docs
---

# radians

## 説明

`x` を角度からラジアンに変換します。

## 構文

```Haskell
REDIANS(x);
```

## パラメータ

`x`: DOUBLE データ型をサポートします。

## 戻り値

DOUBLE データ型の値を返します。

## 例

```Plain
mysql> select radians(90);
+--------------------+
| radians(90)        |
+--------------------+
| 1.5707963267948966 |
+--------------------+
1 row in set (0.00 sec)
```