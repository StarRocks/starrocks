# week_iso

## Description

Returns the ISO standard week number for a given date within a year.

The date parameter must be of the DATE or DATETIME type.

## Syntax

```Haskell
INT WEEK_ISO(DATETIME date)
```

## Examples

```Plain Text
MySQL > select week_iso('2008-02-20 00:00:00');
+-----------------------------------+
| week_iso('2008-02-20 00:00:00')   |
+-----------------------------------+
|                                 8 |
+-----------------------------------+
```

## keyword

WEEK_ISO
