---
displayed_sidebar: docs
---

# convert_tz

DATE または DATETIME の値をあるタイムゾーンから別のタイムゾーンに変換します。

この関数は、異なるタイムゾーンに対して異なる結果を返すことがあります。詳細については、[Configure a time zone](../../../administration/management/timezone.md) を参照してください。

## Syntax

```Haskell
DATETIME CONVERT_TZ(DATETIME|DATE dt, VARCHAR from_tz, VARCHAR to_tz)
```

## Parameters

- `dt`: 変換する DATE または DATETIME の値。

- `from_tz`: 元のタイムゾーン。VARCHAR がサポートされています。タイムゾーンは 2 つの形式で表現できます。1 つは Time Zone Database（例: Asia/Shanghai）、もう 1 つは UTC オフセット（例: +08:00）です。

- `to_tz`: 目的のタイムゾーン。VARCHAR がサポートされています。その形式は `from_tz` と同じです。

## Return value

DATETIME データ型の値を返します。入力が DATE 値の場合、DATETIME 値に変換されます。入力パラメータが無効または NULL の場合、この関数は NULL を返します。

## Usage notes

Time Zone Database については、[List of tz database time zones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)（Wikipedia）を参照してください。

## Examples

例 1: 上海の日時をロサンゼルスに変換します。

```plaintext
select convert_tz('2019-08-01 13:21:03', 'Asia/Shanghai', 'America/Los_Angeles');
+---------------------------------------------------------------------------+
| convert_tz('2019-08-01 13:21:03', 'Asia/Shanghai', 'America/Los_Angeles') |
+---------------------------------------------------------------------------+
| 2019-07-31 22:21:03                                                       |
+---------------------------------------------------------------------------+
1 row in set (0.00 sec)                                                       |
```

例 2: 上海の日付をロサンゼルスに変換します。

```plaintext
select convert_tz('2019-08-01', 'Asia/Shanghai', 'America/Los_Angeles');
+------------------------------------------------------------------+
| convert_tz('2019-08-01', 'Asia/Shanghai', 'America/Los_Angeles') |
+------------------------------------------------------------------+
| 2019-07-31 09:00:00                                              |
+------------------------------------------------------------------+
1 row in set (0.00 sec)
```

例 3: UTC+08:00 の日時をロサンゼルスに変換します。

```plaintext
select convert_tz('2019-08-01 13:21:03', '+08:00', 'America/Los_Angeles');
+--------------------------------------------------------------------+
| convert_tz('2019-08-01 13:21:03', '+08:00', 'America/Los_Angeles') |
+--------------------------------------------------------------------+
| 2019-07-31 22:21:03                                                |
+--------------------------------------------------------------------+
1 row in set (0.00 sec)
```

## Keywords

CONVERT_TZ, timezone, time zone