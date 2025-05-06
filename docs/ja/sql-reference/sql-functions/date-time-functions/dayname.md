---
displayed_sidebar: docs
---

# dayname

## 説明

日付に対応する曜日を返します。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## 構文

```Haskell
VARCHAR DAYNAME(date)
```

## 例

```Plain Text
MySQL > select dayname('2007-02-03 00:00:00');
+--------------------------------+
| dayname('2007-02-03 00:00:00') |
+--------------------------------+
| Saturday                       |
+--------------------------------+
```

## キーワード

DAYNAME