# array_distinct

## Description

Removes duplicate elements from an array.

## Syntax

```Haskell
array_distinct(array)
```

## Parameters

`array`: the array from which you want to remove duplicate elements. Only the ARRAY data type is supported.

## Return value

Returns an array.

## Usage notes

- The elements of the array that is returned may be sorted in a different order than the elements of the array that you specify.

- The elements of the array that is returned are of the same data type as the elements of the array that you specify.

## Examples

In this section, the following table is used as an example:

```Plain%20Text
mysql> select * from test;

+------+---------------+

| c1   | c2            |

+------+---------------+

|    1 | [1,1,2]       |

|    2 | [1,null,null] |

|    3 | NULL          |

|    4 | [null]        |

+------+---------------+
```

Remove duplicate values from column `c2`.

```Plain%20Text
mysql> select c1, array_distinct(c2) from test;

+------+----------------------+

| c1   | array_distinct(`c2`) |

+------+----------------------+

|    1 | [2,1]                |

|    2 | [null,1]             |

|    3 | NULL                 |

|    4 | [null]               |

+------+----------------------+
```
