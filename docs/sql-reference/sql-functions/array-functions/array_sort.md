# array_sort

## Description

Sorts the elements of an array in ascending order.

## Syntax

```SQL
array_sort(array)
```

## Parameters

`array`: the array whose elements you want to sort. Only the ARRAY data type is supported.

## Return value

Returns an array.

## Usage notes

- This function can sort the elements of an array only in ascending order.

- `NULL` values are placed at the beginning of the array that is returned.

- If you want to sort the elements of an array in descending order, use the [reverse](https://docs.starrocks.com/zh-cn/2.2/sql-reference/sql-functions/string-functions/reverse) function.

- The elements of the array that is returned are of the same data type as the elements of the array that you specify.

## Examples

In this section, the following table is used as an example:

```Plain_Text
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

```Plain_Text
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
