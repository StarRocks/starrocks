---
displayed_sidebar: docs
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

when the number of arrays involved in sorting is greater than two，The purpose of this function is to sort array0 based on the values in multiple array columns (such as array1, array2, array3, etc.). The sorting rules are as follows:

First, compare the corresponding elements in array1;
If they are the same, compare the corresponding elements in array2;
And so on, until the last array column.

**Note:**

The elements in the arrays participating in the sort must be of a sortable type or JSON type.
The size of the arrays participating in the sort must be consistent with the original array (except for NULL values).

**Example Explanation**

Given the following four arrays:

```
array0 = [1, 2, 3, 4, 5]
array1 = [6, 5, 5, 5, 4]
array2 = ["d", "b", "a", "b", "4"]
array3 = ["2023-01-01", "2023-01-04", "2023-01-03", "2023-01-05", "2023-01-02"]

```

Sorting Steps:
1. Compare array1:

* The sorted index order of array1 is [4, 2, 3, 1, 0] because 4 < 5 < 5 < 5 < 6.

2. For the elements [5, 5, 5] in array1, compare array2:

* The sorted order of [a, b, b] in array2 is [2, 3, 4] because a < b = b.

3. For the elements [b, b] in array1 and array2, compare array3:

* The sorted order of [2023-01-04, 2023-01-05] in array3 is [2, 4] because 2023-01-04 < 2023-01-05.

Final Sorting Result for array0:

```
array0 = [5, 3, 2, 4, 1]
```

## Syntax

```Haskell
array_sortby(array0, array1)
array_sortby(<lambda function>, array0 [, array1...])
array_sortby(array0, array1, [array2, array3...])
```

- `array_sortby(array0, array1)`

   Sorts `array0` according to the ascending order of `array1`.

- `array_sortby(<lambda function>, array0 [, array1...])`

   Sorts `array0` according to the array returned from the lambda function.

- `array_sortby(array0, array1, [array2, array3...])`

   Sort array0 in ascending order based on the values of multiple array columns (such as array1, array2, array3, etc.). The sorting rule is: first, compare the corresponding elements of array1; if they are the same, compare the corresponding elements of array2; and so on, until the last array column.

## Parameters

- `array0`: the array you want to sort. It must be an array, array expression, or `null`. Elements in the array must be sortable.
- `array1`: the sorting array used to sort `array0`. It must be an array, array expression, or `null`.
- `lambda function`: the lambda expression used to generate the sorting array.
- `array1, [array2, array3...]`：the sorting array used to sort `array0`. It must be an array, array expression, or `null`.

## Return value

Returns an array.

## Usage notes

- This function can sort elements of an array only in ascending order.
- `NULL` values are placed at the beginning of the array that is returned.
- If you want to sort elements of an array in descending order, use the [reverse](../string-functions/reverse.md) function.
- If the sorting array (`array1`) is null, data in `array0` remains unchanged.
- The elements of the returned array have the same data type as the elements of `array0`. The attribute of null values are also the same.
- All arrays must have the same number of elements. Otherwise, an error is returned.

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

```SQL
CREATE TABLE test_array_sortby_muliti (
    id INT(11) not null,
    array_col1 ARRAY<INT>,
    array_col2 ARRAY<DOUBLE>,
    array_col3 ARRAY<VARCHAR(20)>,
    array_col4 ARRAY<DATE>
) ENGINE=OLAP
DUPLICATE KEY(id)
COMMENT "OLAP"
DISTRIBUTED BY HASH(id)
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "DEFAULT",
    "enable_persistent_index" = "false",
    "compression" = "LZ4"
);

INSERT INTO test_array_sortby_multi VALUES
(1, [4, 3, 5], [1.1, 2.2, 2.2], ['a', 'b', 'c'], ['2023-01-01', '2023-01-02', '2023-01-03']),
(2, [6, 7, 8], [6.6, 5.5, 6.6], ['d', 'e', 'd'], ['2023-01-04', '2023-01-05', '2023-01-06']),
(3, NULL, [7.7, 8.8, 8.8], ['g', 'h', 'h'], ['2023-01-07', '2023-01-08', '2023-01-09']),
(4, [9, 10, 11], NULL, ['k', 'k', 'j'], ['2023-01-10', '2023-01-12', '2023-01-11']),
(5, [12, 13, 14], [10.10, 11.11, 11.11], NULL, ['2023-01-13', '2023-01-14', '2023-01-15']),
(6, [15, 16, 17], [14.14, 13.13, 14.14], ['m', 'o', 'o'], NULL),
(7, [18, 19, 20], [16.16, 16.16, 18.18], ['p', 'p', 'r'], ['2023-01-16', NULL, '2023-01-18']),
(8, [21, 22, 23], [19.19, 20.20, 19.19], ['a', 't', 'a'], ['2023-01-19', '2023-01-20', '2023-01-21']),
(9, [24, 25, 26], NULL, ['y', 'y', 'z'], ['2023-01-25', '2023-01-24', '2023-01-26']),
(10, [24, 25, 26], NULL, ['y', 'y', 'z'], ['2023-01-25', NULL, '2023-01-26']);


select * from test_array_sortby_multi order by id asc;
+------+------------+---------------------+---------------+------------------------------------------+
| id   | array_col1 | array_col2          | array_col3    | array_col4                               |
+------+------------+---------------------+---------------+------------------------------------------+
|    1 | [4,3,5]    | [1.1,2.2,2.2]       | ["a","b","c"] | ["2023-01-01","2023-01-02","2023-01-03"] |
|    2 | [6,7,8]    | [6.6,5.5,6.6]       | ["d","e","d"] | ["2023-01-04","2023-01-05","2023-01-06"] |
|    3 | NULL       | [7.7,8.8,8.8]       | ["g","h","h"] | ["2023-01-07","2023-01-08","2023-01-09"] |
|    4 | [9,10,11]  | NULL                | ["k","k","j"] | ["2023-01-10","2023-01-12","2023-01-11"] |
|    5 | [12,13,14] | [10.1,11.11,11.11]  | NULL          | ["2023-01-13","2023-01-14","2023-01-15"] |
|    6 | [15,16,17] | [14.14,13.13,14.14] | ["m","o","o"] | NULL                                     |
|    7 | [18,19,20] | [16.16,16.16,18.18] | ["p","p","r"] | ["2023-01-16",null,"2023-01-18"]         |
|    8 | [21,22,23] | [19.19,20.2,19.19]  | ["a","t","a"] | ["2023-01-19","2023-01-20","2023-01-21"] |
|    9 | [24,25,26] | NULL                | ["y","y","z"] | ["2023-01-25","2023-01-24","2023-01-26"] |
|   10 | [24,25,26] | NULL                | ["y","y","z"] | ["2023-01-25",null,"2023-01-26"]         |
+------+------------+---------------------+---------------+------------------------------------------+
```

Example 1: Sort `array_col1` according to `array_col2`, `array_col3`. 

```Plaintext
select id, array_col1, array_col2, array_col3, array_sortby(array_col1, array_col2, array_col3) from test_array_sortby_multi order by id asc;
+------+------------+---------------------+---------------+--------------------------------------------------------+
| id   | array_col1 | array_col2          | array_col3    | array_sortby(array_col1, array_col2, array_col3)       |
+------+------------+---------------------+---------------+--------------------------------------------------------+
|    1 | [4,3,5]    | [1.1,2.2,2.2]       | ["a","b","c"] | [4,3,5]                                                |
|    2 | [6,7,8]    | [6.6,5.5,6.6]       | ["d","e","d"] | [7,6,8]                                                |
|    3 | NULL       | [7.7,8.8,8.8]       | ["g","h","h"] | NULL                                                   |
|    4 | [9,10,11]  | NULL                | ["k","k","j"] | [11,9,10]                                              |
|    5 | [12,13,14] | [10.1,11.11,11.11]  | NULL          | [12,13,14]                                             |
|    6 | [15,16,17] | [14.14,13.13,14.14] | ["m","o","o"] | [16,15,17]                                             |
|    7 | [18,19,20] | [16.16,16.16,18.18] | ["p","p","r"] | [18,19,20]                                             |
|    8 | [21,22,23] | [19.19,20.2,19.19]  | ["a","t","a"] | [21,23,22]                                             |
|    9 | [24,25,26] | NULL                | ["y","y","z"] | [24,25,26]                                             |
|   10 | [24,25,26] | NULL                | ["y","y","z"] | [24,25,26]                                             |
+------+------------+---------------------+---------------+--------------------------------------------------------+
```

Example 2: Sort `array_col1` according to `array_col2`, `array_col3`, `array_col4`. 

```Plaintext
select id, array_col1, array_col2, array_col3, array_col4, array_sortby(array_col1, array_col2, array_col3, array_col4) from test_array_sortby_multi order by id asc;
+------+------------+---------------------+---------------+------------------------------------------+--------------------------------------------------------------------+
| id   | array_col1 | array_col2          | array_col3    | array_col4                               | array_sortby(array_col1, array_col2, array_col3, array_col4)       |
+------+------------+---------------------+---------------+------------------------------------------+--------------------------------------------------------------------+
|    1 | [4,3,5]    | [1.1,2.2,2.2]       | ["a","b","c"] | ["2023-01-01","2023-01-02","2023-01-03"] | [4,3,5]                                                            |
|    2 | [6,7,8]    | [6.6,5.5,6.6]       | ["d","e","d"] | ["2023-01-04","2023-01-05","2023-01-06"] | [7,6,8]                                                            |
|    3 | NULL       | [7.7,8.8,8.8]       | ["g","h","h"] | ["2023-01-07","2023-01-08","2023-01-09"] | NULL                                                               |
|    4 | [9,10,11]  | NULL                | ["k","k","j"] | ["2023-01-10","2023-01-12","2023-01-11"] | [11,9,10]                                                          |
|    5 | [12,13,14] | [10.1,11.11,11.11]  | NULL          | ["2023-01-13","2023-01-14","2023-01-15"] | [12,13,14]                                                         |
|    6 | [15,16,17] | [14.14,13.13,14.14] | ["m","o","o"] | NULL                                     | [16,15,17]                                                         |
|    7 | [18,19,20] | [16.16,16.16,18.18] | ["p","p","r"] | ["2023-01-16",null,"2023-01-18"]         | [19,18,20]                                                         |
|    8 | [21,22,23] | [19.19,20.2,19.19]  | ["a","t","a"] | ["2023-01-19","2023-01-20","2023-01-21"] | [21,23,22]                                                         |
|    9 | [24,25,26] | NULL                | ["y","y","z"] | ["2023-01-25","2023-01-24","2023-01-26"] | [25,24,26]                                                         |
|   10 | [24,25,26] | NULL                | ["y","y","z"] | ["2023-01-25",null,"2023-01-26"]         | [25,24,26]                                                         |
+------+------------+---------------------+---------------+------------------------------------------+--------------------------------------------------------------------+
```

## References

[array_sort](array_sort.md)