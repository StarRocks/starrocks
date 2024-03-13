---
displayed_sidebar: "English"
---

# dayofweek_iso

## Description

Returns the ISO standard day of the week for the specified date as an integer within the range of `1` to `7`. In this standard, `1` represents Monday, and `7` represents Sunday.

## Syntax

```Haskell
INT DAY_OF_WEEK_ISO(DATETIME date)
```

## Parameters

`date`: the date you want to convert. It must be of the DATE or DATETIME type.

## Examples

The following example returns the ISO standard day of the week for the date `2023-01-01`:

```SQL
MySQL > select dayofweek_iso('2023-01-01');
+-----------------------------+
| dayofweek_iso('2023-01-01') |
+-----------------------------+
|                           7 |
+-----------------------------+
```

## Keywords

DAY_OF_WEEK_ISO
