---
displayed_sidebar: docs
---

# monthname

指定された日付の月名を返します。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## Syntax

```Haskell
VARCHAR MONTHNAME(date)
```

## Examples

```Plain Text
MySQL > select monthname('2008-02-03 00:00:00');
+----------------------------------+
| monthname('2008-02-03 00:00:00') |
+----------------------------------+
| February                         |
+----------------------------------+
```

## keyword

MONTHNAME, monthname