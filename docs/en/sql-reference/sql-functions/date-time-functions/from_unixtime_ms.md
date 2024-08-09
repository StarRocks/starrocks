---
displayed_sidebar: "English"
---

# from_unixtime_ms

Converts a UNIX timestamp in milliseconds into DATETMIE.

This function may return different results for different time zones. For more information, see [Configure a time zone](../../../administration/management/timezone.md).

## Syntax

```Haskell
VARCHAR from_unixtime_ms(BIGINT unix_timestamp_ms)
```

## Parameters

- `unix_timestamp_ms`: the UNIX timestamp you want to convert. It must be of the BIGINT type. If the specified timestamp is less than 0 or larger than 253402243199000, NULL will be returned. That is, the range for timestamp is `1970-01-01 00:00:00` to `9999-12-30 11:59:59`(varies because of timezone).

## Return value

Returns a DATETIME value.

If the timestamp exceeds the value range, NULL will be returned.

## Examples

```Plain Text
select from_unixtime_ms(1000);
+------------------------+
| from_unixtime_ms(1000) |
+------------------------+
| 1970-01-01 00:00:01    |
+------------------------+

select from_unixtime_ms(9001);
+------------------------+
| from_unixtime_ms(9001) |
+------------------------+
| 1970-01-01 00:00:09    |
+------------------------+
```

## keyword

FROM_UNIXTIME,FROM,UNIXTIME
