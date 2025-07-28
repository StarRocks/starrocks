---
displayed_sidebar: docs
---


# to_datetime

> Converts Unix timestamp to `DATETIME` type (time zone aware)

## Description

`to_datetime` converts a Unix timestamp (starting from 1970-01-01 00:00:00 UTC) to a `DATETIME` value, **according to the current session's `time_zone` setting**.

---

## Syntax

```sql
DATETIME to_datetime(BIGINT unix_ts)
DATETIME to_datetime(BIGINT unix_ts, INT scale)
```

---

## Parameters

| Name      | Type   | Required | Description |
|-----------|--------|----------|-------------|
| `unix_ts` | BIGINT | Yes      | Unix timestamp, e.g., `1598306400` (seconds), `1598306400123` (milliseconds) |
| `scale`   | INT    | No       | Time precision:<br/>• 0 = seconds (default)<br/>• 3 = milliseconds<br/>• 6 = microseconds |

---

## Return Value

- On success: returns a `DATETIME` value in the current session's time zone.
- On failure: returns `NULL`, common reasons include:
  - Invalid `scale` (not 0, 3, or 6)
  - Value out of `DATETIME` range (0001-01-01 to 9999-12-31)

---

## Example 

```sql
SET time_zone = 'Asia/Shanghai';

SELECT to_datetime(1598306400);
-- Returns: 2020-08-25 06:00:00

SELECT to_datetime(1598306400123, 3);
-- Returns: 2020-08-25 06:00:00.123000
```

---

