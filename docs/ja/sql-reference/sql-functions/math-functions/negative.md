---
displayed_sidebar: docs
---

# negative

入力 `arg` の負の値を返します。

## 構文

```Plain
negative(arg)
```

## パラメータ

`arg` は以下のデータ型をサポートします:

- BIGINT
- DOUBLE
- DECIMALV2
- DECIMAL32
- DECIMAL64
- DECIMAL128

## 戻り値

入力と同じデータ型の値を返します。

## 例

```Plain
mysql> select negative(3);
+-------------+
| negative(3) |
+-------------+
|          -3 |
+-------------+
1 row in set (0.00 sec)

mysql> select negative(cast(3.14 as decimalv2));
+--------------------------------------+
| negative(CAST(3.14 AS DECIMAL(9,0))) |
+--------------------------------------+
|                                -3.14 |
+--------------------------------------+
1 row in set (0.01 sec)
```