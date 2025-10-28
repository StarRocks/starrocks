---
displayed_sidebar: docs
---

# to_datetime

Converts a Unix timestamp to a DATETIME type value **based on the current time zone settings**.

For detailed instructions on setting a time zone, see [Configure a time zone](../../../administration/management/timezone.md).

If you want to convert a Unix timestamp to a DATETIME type value independent of the current session's time zone settings, you can use [to_datetime_ntz](./to_datetime_ntz.md).

## Syntax

```sql
DATETIME to_datetime(BIGINT unix_ts, INT scale)
```

## Parameters

| Name      | Type   | Required | Description                                             |
| --------- | ------ | -------- | ------------------------------------------------------- |
| `unix_ts` | BIGINT | Yes      | The Unix timestamp to convert. For example, `1598306400` (seconds) and `1598306400123` (milliseconds). |
| `scale`   | INT    | No       | Time precision. Valid values:<ul><li>`0` indicates seconds (Default).</li><li>`3` indicates milliseconds.</li><li>`6` indicates microseconds.</li></ul> |

## Return Value

- On success: returns a `DATETIME` value based on the current session's time zone.
- On failure: returns `NULL`. Common reasons include:
  - Invalid `scale` (not 0, 3, or 6)
  - Value out of DATETIME range (0001-01-01 to 9999-12-31)

## Example 

```sql
SET time_zone = 'Asia/Shanghai';

SELECT to_datetime(1598306400);
-- Returns: 2020-08-25 06:00:00

SELECT to_datetime(1598306400123, 3);
-- Returns: 2020-08-25 06:00:00.123000
```