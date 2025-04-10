---
displayed_sidebar: docs
---

# milliseconds_diff

## Description

Returns the time difference between the start date and end date in milliseconds.

This function is supported from v3.2.4.

## Syntax

```Haskell
BIGINT milliseconds_diff(DATETIME expr1, DATETIME expr2);
```

## Parameters

- `expr1`: the end time. It must be of the DATETIME type. If `expr1` is earlier than `expr2`, a negative value is returned.

- `expr2`: the start time. It must be of the DATETIME type.

## Return value

Returns a value of the BIGINT type. NULL is returned if the input date does not exist.

## Examples

```Plain Text
select milliseconds_diff('2024-01-25 21:00:00.423','2024-01-25 21:00:00.123');
+-------------------------------------------------------------------------+
| milliseconds_diff('2024-01-25 21:00:00.423', '2024-01-25 21:00:00.123') |
+-------------------------------------------------------------------------+
|                                                                     300 |
+-------------------------------------------------------------------------+

select milliseconds_diff('2024-01-25 21:00:01', '2024-01-25 21:00:00');
+-----------------------------------------------------------------+
| milliseconds_diff('2024-01-25 21:00:01', '2024-01-25 21:00:00') |
+-----------------------------------------------------------------+
|                                                            1000 |
+-----------------------------------------------------------------+

select milliseconds_diff('2024-01-25 00:00:01', '2024-01-25');
+--------------------------------------------------------+
| milliseconds_diff('2024-01-25 00:00:01', '2024-01-25') |
+--------------------------------------------------------+
|                                                   1000 |
+--------------------------------------------------------+
```
