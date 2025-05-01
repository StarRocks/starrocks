---
displayed_sidebar: docs
---

# dayofweek_iso

## 説明

指定された日付のISO標準の曜日を、`1` から `7` の範囲内の整数で返します。この標準では、`1` が月曜日、`7` が日曜日を表します。

## 構文

```Haskell
INT DAY_OF_WEEK_ISO(DATETIME date)
```

## パラメータ

`date`: 変換したい日付。DATE または DATETIME 型である必要があります。

## 例

次の例は、日付 `2023-01-01` のISO標準の曜日を返します。

```SQL
MySQL > select dayofweek_iso('2023-01-01');
+-----------------------------+
| dayofweek_iso('2023-01-01') |
+-----------------------------+
|                           7 |
+-----------------------------+
```

## キーワード

DAY_OF_WEEK_ISO