---
displayed_sidebar: docs
description: "Subtracts a specified number of quarters from the date."
---

# quarters_sub



Subtracts a specified number of quarters from the date. One quarter equals three months.

## Syntax

```Haskell
DATETIME quarters_sub(DATETIME expr1, INT expr2);
```

## Parameters

- `expr1`: the specified date. It must be of the DATETIME or DATE type.

- `expr2`: the quarters to subtract. It must be of the INT type. It can be greater, equal, or less than zero. A negative value adds quarters to `expr1`.

## Return value

Returns a DATETIME value.

## Examples

```Plain
select quarters_sub('2022-02-28 15:04:05', 1);
+----------------------------------------+
| quarters_sub('2022-02-28 15:04:05', 1) |
+----------------------------------------+
| 2021-11-28 15:04:05                    |
+----------------------------------------+

select quarters_sub('2022-02-28', 1);
+-------------------------------+
| quarters_sub('2022-02-28', 1) |
+-------------------------------+
| 2021-11-28 00:00:00           |
+-------------------------------+
```