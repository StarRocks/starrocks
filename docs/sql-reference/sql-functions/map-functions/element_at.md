# element_at

## Description

Returns the map element with the specific key. if any parameter is NULL, the result is also NULL.

It is the alias of `[]` to get an element from a map.

## Syntax

```Haskell
element_at(any_map, any_key)
```
if any_key exists in any_map, the specific key-value pair will return, otherwise return null.

## Examples

```plain text
mysql> select element_at({1:3,2:3},1);
+-------------------------+
| element_at({1:3,2:3},1) |
+-------------------------+
|                   {1:3} |
+-------------------------+
1 row in set (0.00 sec)
mysql> select element_at({1:3,2:3},3);
+-------------------------+
| element_at({1:3,2:3},3) |
+-------------------------+
|                    NULL |
+-------------------------+
1 row in set (0.00 sec)
```

## keyword

ELEMENT_AT, MAP