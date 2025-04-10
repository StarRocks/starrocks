---
displayed_sidebar: docs
---

# array_sort

## Description

Sorts the elements of an array in ascending order.

## Syntax

```Haskell
array_sort(array)
```

## Parameters

`array`: the array whose elements you want to sort. Only the ARRAY data type is supported.

## Return value

Returns an array.

## Usage notes

- This function sorts the elements of an array only in ascending order.

- `NULL` values are placed at the beginning of the array that is returned.

- If you want to sort the elements of an array in descending order, use the [reverse](../string-functions/reverse.md) function.

- The elements of the returned array have the same data type as the elements of the input array.

## Examples

The following table is used as an example:

```plaintext
mysql> select * from test;

+------+--------------+

| c1   | c2           |

+------+--------------+

|    1 | [4,3,null,1] |

|    2 | NULL         |

|    3 | [null]       |

|    4 | [8,5,1,4]    |

+------+--------------+
```

Sort the values of column `c2` in ascending order.

```plaintext
mysql> select c1, array_sort(c2) from test;

+------+------------------+

| c1   | array_sort(`c2`) |

+------+------------------+

|    1 | [null,1,3,4]     |

|    2 | NULL             |

|    3 | [null]           |

|    4 | [1,4,5,8]        |

+------+------------------+
```
