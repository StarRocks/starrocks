---
displayed_sidebar: docs
---

# week_iso

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

Returns the ISO standard week of the year for the specified date as an integer within the range of `1` to `53`.

## Syntax

```Haskell
INT WEEK_ISO(DATETIME date)
```

## Parameters

`date`: the date you want to convert. It must be of the DATE or DATETIME type.

## Examples

The following example returns the ISO standard week of the year for the date `2008-02-20 00:00:00`:

```SQL
MySQL > select week_iso ('2008-02-20 00:00:00');
+-----------------------------------+
| week_iso('2008-02-20 00:00:00')   |
+-----------------------------------+
|                                 8 |
+-----------------------------------+
```

## Keywords

WEEK_ISO
