---
displayed_sidebar: docs
---

# array_position

## Description

Obtains the position of an element in an array.

## Syntax

```Haskell
array_position(any_array, any_element)
```

## Parameters

- `any_array`: the array to be searched.
- `any_element`: an expression that matches the element in an array.

## Return value

Returns the position of the specified element if the element can be found in the specified array and returns 0 if the element cannot be found in the array.

## Examples

- Example 1: Obtain the position of an element in an array.

```plaintext
mysql> select array_position(["apple","orange","pear"], "orange");

+-----------------------------------------------------+

| array_position(['apple','orange','pear'], 'orange') |

+-----------------------------------------------------+

|                                                   2 |

+-----------------------------------------------------+

1 row in set (0.01 sec)
```

- Example 2: Obtain the position of `NULL` in an array.

```sql
mysql> select array_position([1, NULL], NULL);
```

```plaintext
+--------------------------------+

| array_position([1,NULL], NULL) |

+--------------------------------+

|                              2 |

+--------------------------------+

1 row in set (0.00 sec)
```

- Example 3: Obtain the position of a sub-array in a multi-dimensional array. A position can be returned only when a sub-array that consists of the same elements in the same order as the specified sub-array can be found in the multi-dimensional array.

```Lua
mysql> select array_position([[1,2,3], [4,5,6]], [4,5,6]);

+--------------------------------------------+

| array_position([[1,2,3],[4,5,6]], [4,5,6]) |

+--------------------------------------------+

|                                          2 |

+--------------------------------------------+

1 row in set (0.00 sec)



mysql> select array_position([[1,2,3], [4,5,6]], [4,6,5]);

+--------------------------------------------+

| array_position([[1,2,3],[4,5,6]], [4,6,5]) |

+--------------------------------------------+

|                                          0 |

+--------------------------------------------+

1 row in set (0.00 sec)
```

## Keywords

ARRAY_POSITION, ARRAY
