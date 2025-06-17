---
displayed_sidebar: docs
---

# week_iso

指定された日付のISO標準週を `1` から `53` の範囲内の整数として返します。

## Syntax

```Haskell
INT WEEK_ISO(DATETIME date)
```

## Parameters

`date`: 変換したい日付。DATE または DATETIME 型である必要があります。

## Examples

次の例は、日付 `2008-02-20 00:00:00` のISO標準週を返します。

```SQL
MySQL > select week_iso ('2008-02-20 00:00:00');
+-----------------------------------+
| week_iso('2008-02-20 00:00:00')   |
+-----------------------------------+
|                                 8 |
+-----------------------------------+
```

## Keywords

WEEK_ISO