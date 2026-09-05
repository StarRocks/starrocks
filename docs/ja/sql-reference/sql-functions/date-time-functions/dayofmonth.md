---
displayed_sidebar: docs
description: "日付から日部分を取得し、1から31の範囲の値を返します。"
---

# dayofmonth

日付から日部分を取得し、1 から 31 の範囲の値を返します。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## Syntax

```Haskell
INT DAYOFMONTH(DATETIME date)
```

## Examples

```Plain Text
MySQL > select dayofmonth('1987-01-31');
+-----------------------------------+
| dayofmonth('1987-01-31 00:00:00') |
+-----------------------------------+
|                                31 |
+-----------------------------------+
```

## keyword

DAYOFMONTH