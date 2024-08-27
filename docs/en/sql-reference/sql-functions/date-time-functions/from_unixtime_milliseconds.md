---
displayed_sidebar: "English"
---

# from_unixtime_milliseconds

Converts a UNIX timestamp in milliseconds into the required time format. The default format is `%Y-%m-%d %H:%i:%s.%f`. It also supports the formats in [date_format](./date_format.md).

This function may return different results for different time zones. For more information, see [Configure a time zone](../../../administration/management/timezone.md).

## Syntax

```Haskell
VARCHAR from_unixtime_milliseconds(BIGINT unix_timestamp_ms[, VARCHAR string_format])
```

## Parameters

- `unix_timestamp`: the UNIX timestamp in milliseconds you want to convert. It must be of the BIGINT type. If the specified timestamp is less than 0 or larger than 253402243199000, NULL will be returned. That is, the range for timestamp is `1970-01-01 00:00:00` to `9999-12-30 11:59:59`(varies because of timezone).

- `string_format`: the required time format.

## Return value

Returns a DATETIME or DATE value of the VARCHAR type. If `string_format` specifies the DATE format, a DATE value of the VARCHAR type is returned.

If the timestamp exceeds the value range or if `string_format` is invalid, NULL will be returned.

## keyword

FROM_UNIXTIME,FROM,UNIXTIME

## Examples

```Plain Text
select from_unixtime_milliseconds(100);
+---------------------------------+
| from_unixtime_milliseconds(100) |
+---------------------------------+
| 1970-01-01 00:00:00.100000      |
+---------------------------------+

select from_unixtime_milliseconds(100, '%y-%m-%d');
+---------------------------------------------+
| from_unixtime_milliseconds(100, '%y-%m-%d') |
+---------------------------------------------+
| 70-01-01                                    |
+---------------------------------------------+

select from_unixtime_milliseconds(9999123, '%Y-%m-%d %H:%i:%s.%f');
+-------------------------------------------------------------+
| from_unixtime_milliseconds(9999123, '%Y-%m-%d %H:%i:%s.%f') |
+-------------------------------------------------------------+
| 1970-01-01 02:46:39.123000                                  |
+-------------------------------------------------------------+
```
