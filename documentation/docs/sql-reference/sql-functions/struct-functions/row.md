# row

## Description

Creates a named STRUCT or ROW value from the given values. It supports unnamed struct. You do not need to specify the field names. StarRocks automatically generates column names, such as `col1, col2,...`.

This function is supported from v3.1 onwards.

struct() is the alias of row().

## Syntax

```Haskell
STRUCT row(ANY val, ...)
```

## Parameters

`val`: an expression of any supported type.

This function is a variable argument function. You must pass at least one argument. `value` is nullable. Separate multiple values with a comma (`,`).

## Return value

Returns a STRUCT value which consists of the input values.

## Examples

```Plaintext
select row(1,"Apple","Pear");
+-----------------------------------------+
| row(1, 'Apple', 'Pear')                 |
+-----------------------------------------+
| {"col1":1,"col2":"Apple","col3":"Pear"} |
+-----------------------------------------+

select row("Apple", NULL);
+------------------------------+
| row('Apple', NULL)           |
+------------------------------+
| {"col1":"Apple","col2":null} |
+------------------------------+

select struct(1,2,3);
+------------------------------+
| row(1, 2, 3)                 |
+------------------------------+
| {"col1":1,"col2":2,"col3":3} |
+------------------------------+
```

## References

- [STRUCT data type](../../sql-statements/data-types/STRUCT.md)
- [named_struct](named_struct.md)
