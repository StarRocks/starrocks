---
displayed_sidebar: docs
---

# from_unixtime_ms

## Description

Converts a Unix timestamp in milliseconds to a datetime string. Currently, only supports one format 'yyyy-MM-dd HH:mm:ss'.

This function is similar to [from_unixtime](./from_unixtime.md), but it accepts a Unix timestamp in milliseconds instead of seconds.

Note that there is no corresponding `unix_timestamp_ms()` function.

This function is affected by the time zone setting. For more information, see [Configure a time zone](../../../administration/management/timezone.md).

## Syntax

```Haskell
VARCHAR from_unixtime_ms(BIGINT unix_timestamp_ms)
```

## Parameters

- `unix_timestamp_ms`: The Unix timestamp in milliseconds to convert, of the BIGINT type.

## Return value

Returns a datetime string of the VARCHAR type.

If the input timestamp is out of range, NULL is returned.

## Examples

```Plain Text
mysql> SELECT from_unixtime_ms(1196440219000);
+------------------------------+
| from_unixtime_ms(1196440219000) |
+------------------------------+
| 2007-12-01 00:30:19          |
+------------------------------+

mysql> SELECT from_unixtime_ms(1196440219123);
+------------------------------+
| from_unixtime_ms(1196440219123) |
+------------------------------+
| 2007-12-01 00:30:19          |
+------------------------------+

```

## Keywords

FROM_UNIXTIME_MS, FROM, UNIXTIME, MS, MILLISECONDS

## Related functions

- [from_unixtime](./from_unixtime.md): Converts a Unix timestamp in seconds to a datetime string
