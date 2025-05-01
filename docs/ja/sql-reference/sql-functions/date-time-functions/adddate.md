---
displayed_sidebar: docs
---

# adddate,days_add

## Description

指定された時間間隔を日付に追加します。

## Syntax

```Haskell
DATETIME ADDDATE(DATETIME|DATE date,INTERVAL expr type)
```

## Parameters

- `date`: 有効な日付または日時の式でなければなりません。
- `expr`: 追加したい時間間隔。INT 型でなければなりません。
- `type`: 時間間隔の単位。以下のいずれかの値にのみ設定できます: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND。

## Return value

DATETIME 値を返します。日付が存在しない場合、例えば `2020-02-30` の場合、NULL が返されます。日付が DATE 値の場合、それは DATETIME 値に変換されます。

## Examples

```Plain Text
select adddate('2010-11-30 23:59:59', INTERVAL 2 DAY);
+-------------------------------------------------+
| adddate('2010-11-30 23:59:59', INTERVAL 2 DAY) |
+-------------------------------------------------+
| 2010-12-02 23:59:59                             |
+-------------------------------------------------+

select adddate('2010-12-03', INTERVAL 2 DAY);
+----------------------------------------+
| adddate('2010-12-03', INTERVAL 2 DAY) |
+----------------------------------------+
| 2010-12-05 00:00:00                    |
+----------------------------------------+
```