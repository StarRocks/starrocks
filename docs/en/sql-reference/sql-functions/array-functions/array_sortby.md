---
displayed_sidebar: "English"
---

# array_sortby

## Description

Sorts elements in an array according to the ascending order of elements in another array or array converted from a lambda expression. For more information, see [Lambda expression](../Lambda_expression.md). This function is supported from v2.5.

Elements in the two arrays are like key-value pairs. For example, b = [7,5,6] is the sorting key of a = [3,1,4]. According to the key-value pair relationship, elements in the two arrays have the following one-to-one mapping.

| **Array** | **Element 1** | **Element 2** | **Element 3** |
| --------- | ------------- | ------------- | ------------- |
| a         | 3             | 1             | 4             |
| b         | 7             | 5             | 6             |

After array `b` is sorted in ascending order, it becomes [5,6,7]. Array `a` becomes [1,4,3] accordingly.

| **Array** | **Element 1** | **Element 2** | **Element 3** |
| --------- | ------------- | ------------- | ------------- |
| a         | 1             | 4             | 3             |
| b         | 5             | 6             | 7             |

## Syntax

```Haskell
array_sortby(array0, array1)
array_sortby(<lambda function>, array0 [, array1...])
```

- `array_sortby(array0, array1)`

   Sorts `array0` according to the ascending order of `array1`.

- `array_sortby(<lambda function>, array0 [, array1...])`

   Sorts `array0` according to the array returned from the lambda function.

## Parameters

- `array0`: the array you want to sort. It must be an array, array expression, or `null`. Elements in the array must be sortable.
- `array1`: the sorting array used to sort `array0`. It must be an array, array expression, or `null`.
- `lambda function`: the lambda expression used to generate the sorting array.

## Return value

Returns an array.

## Usage notes

- This function can sort elements of an array only in ascending order.
- `NULL` values are placed at the beginning of the array that is returned.
- If you want to sort elements of an array in descending order, use the [reverse](./reverse.md) function.
- If the sorting array (`array1`) is null, data in `array0` remains unchanged.
- The elements of the returned array have the same data type as the elements of `array0`. The attribute of null values are also the same.
- The two arrays must have the same number of elements. Otherwise, an error is returned.

## Examples

The following table is used to demonstrate how to use this function.

```SQL
CREATE TABLE `test_array` (
  `c1` int(11) NULL COMMENT "",
  `c2` ARRAY<int(11)> NULL COMMENT "",
  `c3` ARRAY<int(11)> NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c1`)
PROPERTIES (
"replication_num" = "3",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "false",
"compression" = "LZ4"
);

insert into test_array values
(1,[4,3,5],[82,1,4]),
(2,null,[23]),
(3,[4,2],[6,5]),
(4,null,null),
(5,[],[]),
(6,NULL,[]),
(7,[],null),
(8,[null,null],[3,6]),
(9,[432,21,23],[5,4,null]);

select * from test_array order by c1;
+------+-------------+------------+
| c1   | c2          | c3         |
+------+-------------+------------+
|    1 | [4,3,5]     | [82,1,4]   |
|    2 | NULL        | [23]       |
|    3 | [4,2]       | [6,5]      |
|    4 | NULL        | NULL       |
|    5 | []          | []         |
|    6 | NULL        | []         |
|    7 | []          | NULL       |
|    8 | [null,null] | [3,6]      |
|    9 | [432,21,23] | [5,4,null] |
+------+-------------+------------+
9 rows in set (0.00 sec)
```

Example 1: Sort `c3` according to `c2`. This example also provides the result of array_sort() for comparison.

```Plaintext
select c1, c3, c2, array_sort(c2), array_sortby(c3,c2)
from test_array order by c1;
+------+------------+-------------+----------------+----------------------+
| c1   | c3         | c2          | array_sort(c2) | array_sortby(c3, c2) |
+------+------------+-------------+----------------+----------------------+
|    1 | [82,1,4]   | [4,3,5]     | [3,4,5]        | [1,82,4]             |
|    2 | [23]       | NULL        | NULL           | [23]                 |
|    3 | [6,5]      | [4,2]       | [2,4]          | [5,6]                |
|    4 | NULL       | NULL        | NULL           | NULL                 |
|    5 | []         | []          | []             | []                   |
|    6 | []         | NULL        | NULL           | []                   |
|    7 | NULL       | []          | []             | NULL                 |
|    8 | [3,6]      | [null,null] | [null,null]    | [3,6]                |
|    9 | [5,4,null] | [432,21,23] | [21,23,432]    | [4,null,5]           |
+------+------------+-------------+----------------+----------------------+
```

Example 2: Sort array `c3` based on `c2` generated from a lambda expression. This example is equivalent to Example 1. It also provides the result of array_sort() for comparison.

```Plaintext
select
    c1,
    c3,
    c2,
    array_sort(c2) as sorted_c2_asc,
    array_sortby((x,y) -> y, c3, c2) as sorted_c3_by_c2
from test_array order by c1;
+------+------------+-------------+---------------+-----------------+
| c1   | c3         | c2          | sorted_c2_asc | sorted_c3_by_c2 |
+------+------------+-------------+---------------+-----------------+
|    1 | [82,1,4]   | [4,3,5]     | [3,4,5]       | [82,1,4]        |
|    2 | [23]       | NULL        | NULL          | [23]            |
|    3 | [6,5]      | [4,2]       | [2,4]         | [5,6]           |
|    4 | NULL       | NULL        | NULL          | NULL            |
|    5 | []         | []          | []            | []              |
|    6 | []         | NULL        | NULL          | []              |
|    7 | NULL       | []          | []            | NULL            |
|    8 | [3,6]      | [null,null] | [null,null]   | [3,6]           |
|    9 | [5,4,null] | [432,21,23] | [21,23,432]   | [4,null,5]      |
+------+------------+-------------+---------------+-----------------+
```

Example 3: Sort array `c3` based on the ascending order of `c2+c3`.

```Plain
select
    c3,
    c2,
    array_map((x,y)-> x+y,c3,c2) as sum,
    array_sort(array_map((x,y)-> x+y, c3, c2)) as sorted_sum,
    array_sortby((x,y) -> x+y, c3, c2) as sorted_c3_by_sum
from test_array where c1=1;
+----------+---------+----------+------------+------------------+
| c3       | c2      | sum      | sorted_sum | sorted_c3_by_sum |
+----------+---------+----------+------------+------------------+
| [82,1,4] | [4,3,5] | [86,4,9] | [4,9,86]   | [1,4,82]         |
+----------+---------+----------+------------+------------------+
```

## References

[array_sort](array_sort.md)
