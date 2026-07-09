---
displayed_sidebar: docs
description: "日付に対応する曜日を返します。"
---

# dayname

日付に対応する曜日を返します。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## Syntax

```Haskell
VARCHAR DAYNAME(date)
```

## Examples

```Plain Text
MySQL > select dayname('2007-02-03 00:00:00');
+--------------------------------+
| dayname('2007-02-03 00:00:00') |
+--------------------------------+
| Saturday                       |
+--------------------------------+
```

## keyword

DAYNAME