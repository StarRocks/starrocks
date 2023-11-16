---
displayed_sidebar: "English"
---

# to_iso8601

## Description

Convert a date into an ISO 8601 string.

## Syntax

```Haskell
VARCHAR TO_ISO8601(DATETIME date)
```

## Parameters

- The `date` parameter must be a valid date or date expression.

## Examples

```Plain Text
mysql> select to_iso8601(date'2020-01-01');
+--------------------------+
| to_iso8601('2020-01-01') |
+--------------------------+
| 2020-01-01               |
+--------------------------+

mysql> select to_iso8601(datetime'2020-01-01 00:00:00.01');
+------------------------------------------+
| to_iso8601('2020-01-01 00:00:00.010000') |
+------------------------------------------+
| 2020-01-01T00:00:00.010000               |
+------------------------------------------+
```

## keyword

FORMAT