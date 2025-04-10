---
displayed_sidebar: docs
---

# named_struct

## Description

Creates a struct with the specified field names and values.

This function is supported from v3.1 onwards.

## Syntax

```Haskell
STRUCT named_struct({STRING name1, ANY val1} [, ...] )
```

## Parameters

- `nameN`: A STRING field.

- `valN`: An expression of any type that specifies the value for field N. Values are nullable.

The expressions of names and values must be in pairs. Otherwise, the struct cannot be created. You must pass at least one pair of field name and value, separated by a comma (`,`).

## Return value

Returns a STRUCT value.

## Examples

```plain
SELECT named_struct('a', 1, 'b', 2, 'c', 3);
+--------------------------------------+
| named_struct('a', 1, 'b', 2, 'c', 3) |
+--------------------------------------+
| {"a":1,"b":2,"c":3}                  |
+--------------------------------------+

SELECT named_struct('a', null, 'b', 2, 'c', 3);
+-----------------------------------------+
| named_struct('a', null, 'b', 2, 'c', 3) |
+-----------------------------------------+
| {"a":null,"b":2,"c":3}                  |
+-----------------------------------------+
```

## References

- [STRUCT data type](../../data-types/semi_structured/STRUCT.md)
- [row/struct](row.md)
