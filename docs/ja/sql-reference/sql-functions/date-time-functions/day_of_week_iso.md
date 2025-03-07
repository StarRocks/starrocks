---
displayed_sidebar: docs
---

# dayofweek_iso

指定された日付に対して、ISO 標準の曜日を `1` から `7` の範囲の整数で返します。この標準では、`1` は月曜日を表し、`7` は日曜日を表します。

## Syntax

```Haskell
INT DAY_OF_WEEK_ISO(DATETIME date)
```

## Parameters

`date`: 変換したい日付。DATE または DATETIME 型である必要があります。

## Examples

次の例は、日付 `2023-01-01` の ISO 標準の曜日を返します。

```SQL
MySQL > select dayofweek_iso('2023-01-01');
+-----------------------------+
| dayofweek_iso('2023-01-01') |
+-----------------------------+
|                           7 |
+-----------------------------+
```

## Keywords

DAY_OF_WEEK_ISO