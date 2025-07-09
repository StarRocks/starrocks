# to_datetime_ntz

> Converts Unix timestamp to `DATETIME` type (non-time zone aware, always UTC)

## Description

`to_datetime_ntz` converts a Unix timestamp to a `DATETIME` value **always using UTC+0**, regardless of the session time zone.

---

## Syntax

```sql
DATETIME to_datetime_ntz(BIGINT unix_ts)
DATETIME to_datetime_ntz(BIGINT unix_ts, INT scale)
```

---

## Parameters

| Name      | Type   | Required | Description |
|-----------|--------|----------|-------------|
| `unix_ts` | BIGINT | Yes      | Unix timestamp, e.g., `1598306400` (seconds), `1598306400123` (milliseconds) |
| `scale`   | INT    | No       | Time precision:<br/>• 0 = seconds (default)<br/>• 3 = milliseconds<br/>• 6 = microseconds |

---

## Return Value

- On success: returns a UTC `DATETIME` value.
- On failure: returns `NULL`, common reasons include:
  - Invalid `scale` (not 0, 3, or 6)
  - Value out of `DATETIME` range (0001-01-01 to 9999-12-31)

---

## Example (time zone independent)

```sql
SELECT to_datetime_ntz(1598306400);
-- Returns: 2020-08-24 22:00:00

SELECT to_datetime_ntz(1598306400123456, 6);
-- Returns: 2020-08-24 22:00:00.123456
```

---

