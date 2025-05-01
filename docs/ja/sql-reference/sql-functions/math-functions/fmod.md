---
displayed_sidebar: docs
---

# fmod

## 説明

除算 ( `dividend`/`divisor` ) の浮動小数点剰余を返します。これは剰余関数です。

## 構文

```SQL
fmod(dividend,devisor);
```

## パラメータ

- `dividend`:  DOUBLE または FLOAT がサポートされています。

- `devisor`: DOUBLE または FLOAT がサポートされています。

> **注意**
>
> `devisor` のデータ型は `dividend` のデータ型と同じである必要があります。それ以外の場合、StarRocks はデータ型を変換するために暗黙の型変換を行います。

## 戻り値

出力のデータ型と符号は `dividend` のデータ型と符号と同じである必要があります。`divisor` が `0` の場合、`NULL` が返されます。

## 例

```Plaintext
mysql> select fmod(3.14,3.14);
+------------------+
| fmod(3.14, 3.14) |
+------------------+
|                0 |
+------------------+

mysql> select fmod(11.5,3);
+---------------+
| fmod(11.5, 3) |
+---------------+
|           2.5 |
+---------------+

mysql> select fmod(3,6);
+------------+
| fmod(3, 6) |
+------------+
|          3 |
+------------+

mysql> select fmod(3,0);
+------------+
| fmod(3, 0) |
+------------+
|       NULL |
+------------+
```