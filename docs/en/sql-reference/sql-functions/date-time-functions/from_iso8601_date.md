---
displayed_sidebar: docs
---

# from_iso8601_date

Converts a date string in ISO 8601 format to DATE type. The function supports the following ISO 8601 formats:
- YYYY-MM-DD (Standard date format)
- YYYY-Www (ISO week format)
- YYYY-Www-D (ISO week and day format)
- YYYY-DDD (Ordinal date format)

## Syntax

```Haskell
FROM_ISO8601_DATE(STRING iso_date)
```

## Parameters

`iso_date`: A date string in ISO 8601 format. Supports the following formats:
- `YYYY-MM-DD`: Year-Month-Day format (e.g., "2023-12-31")
- `YYYY-Www`: Year and week format (e.g., "2023-W01")
- `YYYY-Www-D`: Year, week, and day of week format (e.g., "2023-W01-1", where 1=Monday, 7=Sunday)
- `YYYY-DDD`: Ordinal date format (e.g., "2023-365")

## Return Value

- Returns a DATE value for valid ISO 8601 date strings
- Returns NULL for invalid format or date

## Examples

```SQL
-- Standard date format
mysql> SELECT from_iso8601_date("2020-05-11");
+----------------------------------+
| from_iso8601_date("2020-05-11")  |
+----------------------------------+
| 2020-05-11                       |
+----------------------------------+

-- ISO week format (start of week 10 of 2020)
mysql> SELECT from_iso8601_date("2020-W10");
+----------------------------------+
| from_iso8601_date("2020-W10")    |
+----------------------------------+
| 2020-03-02                       |
+----------------------------------+

-- ISO week and day format (3rd day of week 10 of 2020)
mysql> SELECT from_iso8601_date("2020-W10-3");
+----------------------------------+
| from_iso8601_date("2020-W10-3")  |
+----------------------------------+
| 2020-03-04                       |
+----------------------------------+

-- Ordinal date format
mysql> SELECT from_iso8601_date("2020-123");
+----------------------------------+
| from_iso8601_date("2020-123")    |
+----------------------------------+
| 2020-05-02                       |
+----------------------------------+

-- Invalid format example
mysql> SELECT from_iso8601_date("invalid");
+----------------------------------+
| from_iso8601_date("invalid")     |
+----------------------------------+
| NULL                             |
+----------------------------------+
```

## Notes

1. Important notes about ISO week numbers:
   - ISO weeks start on Monday
   - The first week of a year is the week containing the first Thursday of that year
   - A year can have 52 or 53 weeks
   - The first week of some years might start in the previous year (e.g., "2020-W01" -> "2019-12-30")

2. For day of week values:
   - 1 = Monday
   - 2 = Tuesday
   - 3 = Wednesday
   - 4 = Thursday
   - 5 = Friday
   - 6 = Saturday
   - 7 = Sunday

## Keywords

FROM_ISO8601_DATE
