# array_remove

## Description

Removes an element from an array.

## Syntax

```Haskell
array_remove(any_array, any_element)
```

## Parameters

- `any_array`: the array to be searched.
- `any_element`: an expression that matches the element in an array.

## Return value

Returns the array from which specified element has been removed.

## Examples

```Plain_Text
mysql> select array_remove([1,2,3,null,3], 3);

+---------------------------------+

| array_remove([1,2,3,NULL,3], 3) |

+---------------------------------+

| [1,2,null]                      |

+---------------------------------+

1 row in set (0.01 sec)
```

## Keywords

ARRAY_REMOVE, ARRAY
