---
displayed_sidebar: docs
---

# dayofmonth

## 説明

日付から日部分を取得し、1から31の範囲の値を返します。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## 構文

```Haskell
INT DAYOFMONTH(DATETIME date)
```

## 例

```Plain Text
MySQL > select dayofmonth('1987-01-31');
+-----------------------------------+
| dayofmonth('1987-01-31 00:00:00') |
+-----------------------------------+
|                                31 |
+-----------------------------------+
```

## キーワード

DAYOFMONTH