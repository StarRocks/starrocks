---
displayed_sidebar: docs
---

# unixtime\_to\_datetime

> Convert a Unix epoch timestamp to a `DATETIME` value.

`unixtime_to_datetime` converts a Unix timestamp—the number of seconds elapsed since **1970-01-01 00:00:00 UTC**—into a StarRocks `DATETIME`.
The function comes in two overloads:

| Argument  | Description                                                                                                           |
| --------- | --------------------------------------------------------------------------------------------------------------------- |
| `unix_ts` | **Required.** The Unix timestamp. Accepts `BIGINT`.                                                                   |
| `scale`   | **Optional.** The time-unit scale:<br/>  • `0` → seconds (default)<br/>  • `3` → milliseconds<br/>  • `6` → microseconds |

If `scale` is omitted, the value is treated as seconds. Use `3` or `6` to interpret the input as milliseconds or microseconds.
The function returns **`NULL`** when either of the following is true:

* `scale` is not one of `{0, 3, 6}`
* The resulting datetime falls outside the valid range `0001-01-01` – `9999-12-31`

---

## Syntax

```haskell
DATETIME unixtime_to_datetime(BIGINT unix_ts)
DATETIME unixtime_to_datetime(BIGINT unix_ts, INT scale)
```

---

## Parameter details

| Parameter | Type   | Required | Description                                                                                               |
| --------- | ------ | -------- | --------------------------------------------------------------------------------------------------------- |
| `unix_ts` | BIGINT | Yes      | The Unix timestamp, e.g. `1598306400` (seconds), `1598306400123` (milliseconds), `1598306400123456` (µs). |
| `scale`   | INT    | No       | Unit scale: `0` = seconds (default), `3` = milliseconds, `6` = microseconds.                              |

---

## Return value

* **Success** – A `DATETIME` accurate to microsecond.
* **Failure / NULL** – Returned when `scale` is invalid or the result is out of range.

---

## Examples

### Seconds

```sql
mysql> SELECT unixtime_to_datetime(1598306400);
+--------------------------------------+
| unixtime_to_datetime(1598306400)     |
+--------------------------------------+
| 2020-08-24 22:00:00                  |
+--------------------------------------+
```

### Milliseconds

```sql
mysql> SELECT unixtime_to_datetime(1598306400123, 3);
+------------------------------------------+
| unixtime_to_datetime(1598306400123, 3)   |
+------------------------------------------+
| 2020-08-24 22:00:00.123000               |
+------------------------------------------+
```

### Microseconds

```sql
mysql> SELECT unixtime_to_datetime(1598306400123456, 6);
+----------------------------------------------+
| unixtime_to_datetime(1598306400123456, 6)    |
+----------------------------------------------+
| 2020-08-24 22:00:00.123456                   |
+----------------------------------------------+
```

### Invalid `scale` → NULL

```sql
mysql> SELECT unixtime_to_datetime(1598306400, 10);
+----------------------------------+
| unixtime_to_datetime(1598306400) |
+----------------------------------+
| NULL                             |
+----------------------------------+
```
