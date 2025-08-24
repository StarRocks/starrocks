---
displayed_sidebar: docs
---

# to_datetime_ntz

Converts a Unix timestamp to a DATETIME type value. **This function is always based on UTC+0, regardless of the time zone settings.**

If you want to convert a Unix timestamp to a DATETIME type value based on the current time zone, you can use [to_datetime](./to_datetime.md).

## Syntax

```sql
DATETIME to_datetime_ntz(BIGINT unix_ts, INT scale)
```

## Parameters

| Name      | Type   | Required | Description                                             |
| --------- | ------ | -------- | ------------------------------------------------------- |
| `unix_ts` | BIGINT | Yes      | The Unix timestamp to convert. For example, `1598306400` (seconds) and `1598306400123` (milliseconds). |
| `scale`   | INT    | No       | Time precision. Valid values:<ul><li>`0` indicates seconds (Default).</li><li>`3` indicates milliseconds.</li><li>`6` indicates microseconds.</li></ul> |

## Return Value

- On success: returns a `DATETIME` value based on UTC+0.
- On failure: returns `NULL`. Common reasons include:
  - Invalid `scale` (not 0, 3, or 6)
  - Value out of DATETIME range (0001-01-01 to 9999-12-31)

## Example

```sql
SELECT to_datetime_ntz(1598306400);
-- Returns: 2020-08-24 22:00:00

SELECT to_datetime_ntz(1598306400123456, 6);
-- Returns: 2020-08-24 22:00:00.123456
```
