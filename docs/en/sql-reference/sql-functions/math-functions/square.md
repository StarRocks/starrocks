# square

## Description

Calculates the square of a value.

## Syntax

```Haskell
square(arg)
```

### Parameters

`arg`: the value whose square you want to calculate. You can specify only a numeric value. This function converts the numeric value into a DOUBLE value before it calculates the square of the value.

## Return value

Returns a value of the DOUBLE data type.

## Usage notes

If you specify a non-numeric value, this function returns `NULL`.

## Examples

Example 1: Calculate the square of a numeric value.

```Plain
mysql>  select square(11);
+------------+
| square(11) |
+------------+
|        121 |
+------------+
```

Example 2: Calculate the square of a non-numeric value. The return value is `NULL`.

```Plain
mysql>  select square('2021-01-01');
+----------------------+
| square('2021-01-01') |
+----------------------+
|                 NULL |
+----------------------+
```
