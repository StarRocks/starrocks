---
displayed_sidebar: "English"
---

# to_iso8601

## Description

Converts the specified date into a string in ISO 8601 format.

## Syntax

```Haskell
VARCHAR TO_ISO8601(DATETIME | DATE date)
```

## Parameters

`date`: the date you want to convert. It must be a valid date or date expression.

## Examples

Example 1: Convert the date `2020-01-01` into a string in ISO 8601 format.

```SQL
MySQL > select to_iso8601(date'2020-01-01');
+--------------------------+
| to_iso8601('2020-01-01') |
+--------------------------+
| 2020-01-01               |
+--------------------------+
```

Example 2: Convert the date `2020-01-01 00:00:00.01` into a string in ISO 8601 format.

```SQL
MySQL > select to_iso8601(datetime'2020-01-01 00:00:00.01');
+------------------------------------------+
| to_iso8601('2020-01-01 00:00:00.010000') |
+------------------------------------------+
| 2020-01-01T00:00:00.010000               |
+------------------------------------------+
```

## Keywords

TO_ISO8601
