---
displayed_sidebar: "English"
---

# element_at



Returns the element at the specified position (index) from a given array. If any parameter is NULL or if the position does not exist, the result is NULL.

This function is the alias of the subscript operator `[]`. It is supported from v3.0 onwards.

If you want to retrieve a value from a key-value pair in a map, see [element_at](../map-functions/element_at.md).

## Syntax

```Haskell
element_at(any_array, position)
```

## Parameters

- `any_array`: an array expression from which to retrieve elements.
- `position`: the position of the element in the array. It must be a positive integer. Value range: [1, array length]. If `position` does not exist, NULL is returned.

## Examples

```plain text
mysql> select element_at([2,3,11],3);
+---------------+
| [2, 3, 11][3] |
+---------------+
|            11 |
+---------------+

mysql> select element_at(["a","b","c"],1);
+--------------------+
| ['a', 'b', 'c'][1] |
+--------------------+
| a                  |
+--------------------+
```

## keyword

ELEMENT_AT, ARRAY
