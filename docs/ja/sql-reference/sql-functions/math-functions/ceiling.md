---
displayed_sidebar: docs
---

# ceiling

入力された `arg` の値を、等しいかそれより大きい最も近い整数に丸めて返します。

## 構文

```Shell
ceiling(arg)
```

## パラメータ

`arg` は DOUBLE データ型をサポートします。

## 戻り値

BIGINT データ型の値を返します。

## 例

```Plain
mysql> select ceiling(3.14);
+---------------+
| ceiling(3.14) |
+---------------+
|             4 |
+---------------+
1 row in set (0.00 sec)
```