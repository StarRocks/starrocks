---
displayed_sidebar: docs
description: "Adds a specified number of quarters to the date, accurate to the quarter."
---

# quarters_add



Adds a specified number of quarters to the date. One quarter equals three months.

## Syntax

```Haskell
DATETIME quarters_add(DATETIME expr1, INT expr2);
```

## Parameters

- `expr1`: the start time. It must be of the DATETIME or DATE type.

- `expr2`: the quarters to add. It must be of the INT type. It can be greater, equal, or less than zero. A negative value subtracts quarters from `expr1`.

## Return value

Returns a DATETIME value.

## Examples

```Plain
select quarters_add('2019-08-01 13:21:03', 1);
+----------------------------------------+
| quarters_add('2019-08-01 13:21:03', 1) |
+----------------------------------------+
| 2019-11-01 13:21:03                    |
+----------------------------------------+

select quarters_add('2019-08-01', 1);
+-------------------------------+
| quarters_add('2019-08-01', 1) |
+-------------------------------+
| 2019-11-01 00:00:00           |
+-------------------------------+

select quarters_add('2019-08-01 13:21:03', -1);
+-----------------------------------------+
| quarters_add('2019-08-01 13:21:03', -1) |
+-----------------------------------------+
| 2019-05-01 13:21:03                     |
+-----------------------------------------+
```