# bitmap_to_array

## Description

Converts a BITMAP into a BIGINT array.

## Syntax

```Haskell
 ARRAY<BIGINT> BITMAP_TO_ARRAY (bitmap)
```

## Parameters

`bitmap`: the bitmap you want to convert.

## Return value

Returns a BIGINT array.

## Examples

```Plain
select bitmap_to_array(bitmap_from_string("1, 7"));
+----------------------------------------------+
| bitmap_to_array(bitmap_from_string('1, 7'))  |
+----------------------------------------------+
| [1,7]                                        |
+----------------------------------------------+

select bitmap_to_array(NULL);
+-----------------------+
| bitmap_to_array(NULL) |
+-----------------------+
| NULL                  |
+-----------------------+
```
