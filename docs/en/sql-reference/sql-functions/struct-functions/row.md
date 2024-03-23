---
displayed_sidebar: "English"
---

# row

## Description

Create an unnamed STRUCT/ROW value from the given values.

## Syntax

```
STRUCT row(ANY val, ...)
```

## Parameters

This function is a variable argument function. Callers should give at least one argument.

## Return value

Return a STRUCT value which is consisted from the input values.

## Examples

```Plaintext
select row(1,"Star","Rocks");
+-------------------------+
| row(1, 'Star', 'Rocks') |
+-------------------------+
| {1,"Star","Rocks"}      |
+-------------------------+
```

<<<<<<< HEAD
```Plaintext
select row("StarRocks", NULL);
+------------------------+
| row('StarRocks', NULL) |
+------------------------+
| {"StarRocks",null}     |
+------------------------+
```
=======
## References

- [STRUCT data type](../../data-types/semi_structured/STRUCT.md)
- [named_struct](named_struct.md)
>>>>>>> 3aa7e96e5e ([Doc] Organize sqlref: move data types up (#43007))
