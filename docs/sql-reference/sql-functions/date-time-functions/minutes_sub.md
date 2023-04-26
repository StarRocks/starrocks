# minutes_sub

## Description

Subtracts specified minutes from the date, accurate to the minute.

## Syntax

```Haskell
DATETIME minutes_sub(DATETIME|DATE date, INT minutes);
```

## Parameters

- `date`: the base date. It must be of the DATETIME or DATE type.

- `minutes`: the minutes to reduce. It must be of the INT type, it could be greater, equal or less than zero.

## Return value

Returns a DATETIME value.

Returns NULL if either `date` or `minutes` is NULL.

## Examples

```Plain Text
select minutes_sub('2022-01-01 01:03:01', 2);
+---------------------------------------+
| minutes_sub('2022-01-01 01:03:01', 2) |
+---------------------------------------+
| 2022-01-01 01:01:01                   |
+---------------------------------------+
select minutes_sub('2022-01-01 01:01:01', -1);
+----------------------------------------+
| minutes_sub('2022-01-01 01:01:01', -1) |
+----------------------------------------+
| 2022-01-01 01:02:01                    |
+----------------------------------------+
select minutes_sub('2022-01-01', 1);
+------------------------------+
| minutes_sub('2022-01-01', 1) |
+------------------------------+
| 2021-12-31 23:59:00          |
+------------------------------+
```