# seconds_diff

## Description

Returns the second difference between two date expressions (`expr1` − `expr2`), accurate to the second.

## Syntax

```Haskell
BIGINT seconds_diff(DATETIME expr1,DATETIME expr2);
```

## Parameters

- `expr1`: the end time. It must be of the DATETIME type.

- `expr2`: the start time. It must be of the DATETIME type.

## Return value

Returns a BIGINT value.

NULL is returned if the date does not exist, for example, 2022-02-29.

## Examples

```Plain
select seconds_diff('2010-11-30 23:59:59', '2010-11-30 20:59:59');
+------------------------------------------------------------+
| seconds_diff('2010-11-30 23:59:59', '2010-11-30 20:59:59') |
+------------------------------------------------------------+
|                                                      10800 |
+------------------------------------------------------------+
```
