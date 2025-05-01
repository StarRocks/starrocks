---
displayed_sidebar: docs
---

# ceil, dceil

## 説明

入力 `arg` からの値を、最も近い等しいまたは大きい整数に切り上げて返します。

## 構文

```Shell
ceil(arg)
```

## パラメータ

`arg` は DOUBLE データ型をサポートします。

## 戻り値

BIGINT データ型の値を返します。

## 例

```Plain
mysql> select ceil(3.14);
+------------+
| ceil(3.14) |
+------------+
|          4 |
+------------+
1 row in set (0.15 sec)
```