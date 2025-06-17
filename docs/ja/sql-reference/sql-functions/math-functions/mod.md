---
displayed_sidebar: docs
---

# mod

`dividend` を `divisor` で割った余りを返す剰余関数です。

## 構文

```SQL
mod(dividend, divisor)
```

## パラメータ

- `dividend`: 割られる数。
- `divisor`: 割る数。

`dividend` と `divisor` は以下のデータ型をサポートしています:

- TINYINT
- SMALLINT
- INT
- BIGINT
- LARGEINT
- FLOAT
- DOUBLE
- DECIMALV2
- DECIMAL32
- DECIMAL64
- DECIMAL128

> **注意**
>
> `dividend` と `divisor` はデータ型が一致している必要があります。StarRocks はデータ型が一致しない場合、暗黙的な変換を行います。

## 戻り値

`dividend` と同じデータ型の値を返します。`divisor` が 0 に指定された場合、StarRocks は NULL を返します。

## 例

```Plain
mysql> select mod(3.14,3.14);
+-----------------+
| mod(3.14, 3.14) |
+-----------------+
|               0 |
+-----------------+

mysql> select mod(3.14, 3);
+--------------+
| mod(3.14, 3) |
+--------------+
|         0.14 |
+--------------+

select mod(11,-5);
+------------+
| mod(11, -5)|
+------------+
|          1 |
+------------+

select mod(-11,5);
+-------------+
| mod(-11, 5) |
+-------------+
|          -1 |
+-------------+
```