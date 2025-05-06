---
displayed_sidebar: docs
---

# unix_timestamp

## Description

DATE または DATETIME 値を UNIX タイムスタンプに変換します。

パラメータが指定されていない場合、この関数は現在の時刻を UNIX タイムスタンプに変換します。

`date` パラメータは DATE または DATETIME 型でなければなりません。

1970-01-01 00:00:00 より前または 2038-01-19 11:14:07 より後の時間に対しては、この関数は 0 を返します。

日付形式の詳細については、 [date_format](./date_format.md) を参照してください。

この関数は異なるタイムゾーンで異なる結果を返すことがあります。詳細については、 [Configure a time zone](../../../administration/management/timezone.md) を参照してください。

## Syntax

```Haskell
BIGINT UNIX_TIMESTAMP()
BIGINT UNIX_TIMESTAMP(DATETIME date)
BIGINT UNIX_TIMESTAMP(DATETIME date, STRING fmt)
```

## Examples

```Plain Text
MySQL > select unix_timestamp();
+------------------+
| unix_timestamp() |
+------------------+
|       1558589570 |
+------------------+

MySQL > select unix_timestamp('2007-11-30 10:30:19');
+---------------------------------------+
| unix_timestamp('2007-11-30 10:30:19') |
+---------------------------------------+
|                            1196389819 |
+---------------------------------------+

MySQL > select unix_timestamp('2007-11-30 10:30-19', '%Y-%m-%d %H:%i-%s');
+---------------------------------------+
| unix_timestamp('2007-11-30 10:30-19') |
+---------------------------------------+
|                            1196389819 |
+---------------------------------------+

MySQL > select unix_timestamp('2007-11-30 10:30%3A19', '%Y-%m-%d %H:%i%%3A%s');
+---------------------------------------+
|unix_timestamp('2007-11-30 10:30%3A19')|
+---------------------------------------+
|                            1196389819 |
+---------------------------------------+

MySQL > select unix_timestamp('1969-01-01 00:00:00');
+---------------------------------------+
| unix_timestamp('1969-01-01 00:00:00') |
+---------------------------------------+
|                                     0 |
+---------------------------------------+
```

## keyword

UNIX_TIMESTAMP, UNIX, TIMESTAMP