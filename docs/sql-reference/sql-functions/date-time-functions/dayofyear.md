# dayofyear

## Description

Returns the day of the year for a given date.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```Haskell
INT DAYOFYEAR(DATETIME|DATE date)
```

## Examples

```Plain Text
MySQL > select dayofyear('2007-02-03 00:00:00');
+----------------------------------+
| dayofyear('2007-02-03 00:00:00') |
+----------------------------------+
|                               34 |
+----------------------------------+
```

## keyword

DAYOFYEAR
