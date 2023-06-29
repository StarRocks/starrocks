# dayofweek_iso

## Description

Returns the ISO standard weekday number for a given date.

The date parameter must be of the DATE or DATETIME type.

## Syntax

```Haskell
INT DAY_OF_WEEK_ISO(DATETIME date)
```

## Examples

```Plain Text
MySQL > select dayofweek_iso('2008-02-20 00:00:00');
+-----------------------------------+
| dayofweek_iso('2008-02-20 00:00:00')   |
+-----------------------------------+
|                                 3 |
+-----------------------------------+
```

## keyword

DAY_OF_WEEK_ISO
