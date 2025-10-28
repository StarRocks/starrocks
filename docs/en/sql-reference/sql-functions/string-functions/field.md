---
displayed_sidebar: docs
---

# field

Returns the index (position) of a value in a list of values.

This function is supported from v3.5.

## Syntax

```sql
INT field(VARCHAR val, VARCHAR val1, val2, ...);
```

- `val`: The value to search for in the list.
- `val1`, `val2`, ...: The values in the list.

## Usage notes

- If the specified value is not found in the list of values, this function will return `0`. If the specified value is NULL, this function will return `0`.
- If all arguments to the function are strings, all arguments are compared as strings. If all arguments are numbers, they are compared as numbers. Otherwise, the arguments are compared as DOUBLE.

## Examples

```sql
MYSQL > select field('a', 'b', 'a', 'd');
+---------------------------+
| field('a', 'b', 'a', 'd') |
+---------------------------+
|                         2 |
+---------------------------+
```

## keyword

FIELD