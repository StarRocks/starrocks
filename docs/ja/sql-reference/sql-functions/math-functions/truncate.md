---
displayed_sidebar: docs
---

# truncate

## 説明

入力を小数点以下の指定された桁数で、最も近い小さい値に切り捨てます。

## 構文

```Shell
truncate(arg1,arg2);
```

## パラメータ

- `arg1`: 切り捨て対象の入力。以下のデータ型をサポートします:
  - DOUBLE
  - DECIMAL128
- `arg2`: 小数点以下に保持する桁数。INT データ型をサポートします。

## 戻り値

`arg1` と同じデータ型の値を返します。

## 例

```Plain
mysql> select truncate(3.14,1);
+-------------------+
| truncate(3.14, 1) |
+-------------------+
|               3.1 |
+-------------------+
1 row in set (0.00 sec)
```