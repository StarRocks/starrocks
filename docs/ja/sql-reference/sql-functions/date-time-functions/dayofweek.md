---
displayed_sidebar: docs
---

# dayofweek

## Description

指定された日付の曜日インデックスを返します。たとえば、日曜日のインデックスは 1、月曜日は 2、土曜日は 7 です。

`date` パラメータは DATE または DATETIME 型である必要があります。または、DATE または DATETIME 値にキャストできる有効な式である必要があります。

## Syntax

```Haskell
INT dayofweek(DATETIME date)
```

## Examples

```Plain Text
MySQL > select dayofweek('2019-06-25');
+----------------------------------+
| dayofweek('2019-06-25 00:00:00') |
+----------------------------------+
|                                3 |
+----------------------------------+

MySQL > select dayofweek(cast(20190625 as date));
+-----------------------------------+
| dayofweek(CAST(20190625 AS DATE)) |
+-----------------------------------+
|                                 3 |
+-----------------------------------+
```

## keyword

DAYOFWEEK