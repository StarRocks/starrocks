---
displayed_sidebar: docs
---

# array_sort



Sorts the elements of an array in ascending order.

## Syntax

```Haskell
array_sort(array)
```

```Haskell
array_sort(array, (x,y)->expr(x,y))
```

## Parameters

`array`: the array whose elements you want to sort. Only the ARRAY data type is supported.

`lambda_comparator`: an optional lambda function comparator with format `(x,y)->expr(x,y)`. The lambda comparator:
- Must only depend on arguments x and y
- Must return boolean or numeric type
- Result should not be NULL
- Must satisfy strict weak ordering requirements

## Return value

Returns an array.

## Usage notes

### Basic Sorting

- This function sorts the elements of an array in ascending order when used without a lambda comparator.
- `NULL` values are placed at the beginning of the array that is returned.
- The elements of the returned array have the same data type as the elements of the input array.

### Lambda Comparator Sorting

- When using a lambda comparator, the sorting order is determined by the expression result:
  - For boolean return: `true` means x should come before y
  - For numeric return: negative means x comes before y

- The lambda comparator must satisfy strict weak ordering requirements:
  - **Irreflexivity**: For all `x`, `expr(x, x)` must return false (boolean) or non-negative (numeric)
  - **Asymmetry**: If `expr(x, y)` holds, then `expr(y, x)` must not hold
  - **Transitivity**: If `expr(x, y)` and `expr(y, z)` hold, then `expr(x, z)` must hold
  - **Connectedness**: For all `x` and `y`, either `expr(x, y)` or `expr(y, x)` or `x = y` must be true

- The lambda comparator expression must:
  - Only depend on parameters `x` and `y`
  - Return a boolean or numeric type
  - Never return NULL
  - Always produce consistent results for the same input values

### Descending Order

- If you want to sort the elements of an array in descending order, use the [reverse](../string-functions/reverse.md) function.

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

### Lambda Comparator Examples

Sort an array of integers in descending order using a lambda comparator:

```plaintext
mysql> select array_sort([3,1,4,2], (x,y) -> x > y);
+-----------------------------------------+
| array_sort([3, 1, 4, 2], (x, y) -> x > y) |
+-----------------------------------------+
| [4, 3, 2, 1]                             |
+-----------------------------------------+
```

Sort an array of integers using numeric return values:

```plaintext
mysql> select array_sort([3,1,4,2], (x,y) -> x - y);
+----------------------------------------+
| array_sort([3, 1, 4, 2], (x, y) -> x - y) |
+----------------------------------------+
| [1, 2, 3, 4]                           |
+----------------------------------------+
```

Sort an array of strings by length:

```plaintext
mysql> select array_sort(['apple', 'banana', 'cherry', 'date'], (x,y) -> length(x) - length(y));
+----------------------------------------------------------------------------+
| array_sort(['apple', 'banana', 'cherry', 'date'], (x, y) -> length(x) - length(y)) |
+----------------------------------------------------------------------------+
| ['date', 'apple', 'banana', 'cherry']                                      |
+----------------------------------------------------------------------------+
```

Sort an array of strings alphabetically using boolean return:

```plaintext
mysql> select array_sort(['banana', 'apple', 'cherry'], (x,y) -> x > y);
+--------------------------------------------------------+
| array_sort(['banana', 'apple', 'cherry'], (x, y) -> x > y) |
+--------------------------------------------------------+
| ['apple', 'banana', 'cherry']                           |
+--------------------------------------------------------+
```

Sort an array with custom business logic (sort even numbers first, then odd numbers):

```plaintext
mysql> select array_sort([5,2,8,1,9,4], (x,y) -> 
    case when x % 2 = 0 and y % 2 = 0 then x - y
         when x % 2 = 0 and y % 2 = 1 then -1
         when x % 2 = 1 and y % 2 = 0 then 1
         else x - y end);
+--------------------------------------------------------------------------------------------------------------------+
| array_sort([5, 2, 8, 1, 9, 4], (x, y) -> case when x % 2 = 0 and y % 2 = 0 then x - y when x % 2 = 0 and y % 2 = 1 then -1 when x % 2 = 1 and y % 2 = 0 then 1 else x - y end) |
+--------------------------------------------------------------------------------------------------------------------+
| [2, 4, 8, 1, 5, 9]                                                                                                |
+--------------------------------------------------------------------------------------------------------------------+
```

Sort an array of decimals with custom precision comparison:

```plaintext
mysql> select array_sort([3.141, 2.718, 1.414, 2.236], (x,y) -> round(x, 2) - round(y, 2));
+----------------------------------------------------------------------------------------+
| array_sort([3.141, 2.718, 1.414, 2.236], (x, y) -> round(x, 2) - round(y, 2))         |
+----------------------------------------------------------------------------------------+
| [1.414, 2.236, 2.718, 3.141]                                                          |
+----------------------------------------------------------------------------------------+
```
