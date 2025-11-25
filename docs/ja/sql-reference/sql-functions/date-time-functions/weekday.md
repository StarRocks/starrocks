---
displayed_sidebar: docs
---

# weekday

指定された日付の曜日インデックスを返します。例えば、月曜日は0、日曜日のインデックスは6。

## Syntax

```Haskell
INT WEEKDAY(DATETIME date)
```

## Parameters

`date`: パラメータは、DATE または DATETIME 型である必要があります。または、DATE または DATETIME 値にキャストできる有効な式である必要があります。

## Examples

```SQL
MySQL > select weekday('2023-01-01');
+-----------------------+
| weekday('2023-01-01') |
+-----------------------+
|                     6 |
+-----------------------+
```

## Keywords

WEEKDAY