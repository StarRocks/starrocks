---
displayed_sidebar: docs
---

# truncate

指定された小数点以下の桁数で、入力をそれ以下の最も近い値に切り捨てます。

## Syntax

```Shell
truncate(arg1,arg2);
```

## Parameter

- `arg1`: 切り捨てる対象の入力。以下のデータ型をサポートします:
  - DOUBLE
  - DECIMAL128
- `arg2`: 小数点以下の桁数を指定します。INT データ型をサポートします。

## Return value

`arg1` と同じデータ型の値を返します。

## Examples

```Plain
mysql> select truncate(3.14,1);
+-------------------+
| truncate(3.14, 1) |
+-------------------+
|               3.1 |
+-------------------+
1 row in set (0.00 sec)
```