# element_at

## Description

Returns the array element at the specific position. if any parameter is NULL, the result is also NULL.

It is the alias of `[]` to get an element from an array.

## Syntax

```Haskell
element_at(any_array, position)
```
position is an integer, which is in [1, any_array.lenght], other values will return null.

## Examples

```plain text
mysql> select element_at([11,2,3],3);
+-----------------------+
|  element_at([11,2,3]) |
+-----------------------+
|                     3 |
+-----------------------+
1 row in set (0.00 sec)
```

## keyword

ELEMENT_AT, ARRAY