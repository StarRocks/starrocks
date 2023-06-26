# row

## Description

Creates a named STRUCT or ROW value from the given values.

The row() and struct() functions are equivalent. They support unnamed struct. You do not need to specify the field names. StarRocks automatically generates column names, like `col1`, `col2`...

This function is supported from v3.1 onwards.

## Syntax

```Haskell
STRUCT row(ANY val, ...)
```

## Parameters

`val`: an expression of any supported type.

This function is a variable argument function. You must pass at least one argument.

## Return value

Returns a STRUCT value which consists of the input values.

## Examples

```Plaintext
select row(1,"Star","Rocks");
+------------------------------------------+
| row(1, 'Star', 'Rocks')                  |
+------------------------------------------+
| {"col1":1,"col2":"Star","col3":"Rocks"}  |
+-------------------------+
```

```Plaintext
select row("StarRocks", NULL);
+-----------------------------------+
| row('StarRocks', NULL)            |
+-----------------------------------+
|  {"col1":"StarRocks","col2":null} |
+------------------------+
```

## References

- [STRUCT data type](../../sql-statements/data-types/STRUCT.md)
- [named_struct](named_struct.md)
