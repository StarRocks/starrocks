---
displayed_sidebar: "English"
---

# array_sortby_muti

The purpose of this function is to sort array0 based on the values in multiple array columns (such as array1, array2, array3, etc.). The sorting rules are as follows:

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
array_sortby_multi(array0, array1, [array2, array3...])
```

  For each row in the table, sort array0 based on the values in multiple array columns (array1, array2, array3, etc.). The sorting rule is: first compare the corresponding elements in array1, if they are the same, then compare the corresponding elements in array2, and so on, until the last array column.

## Parameters

- `array0`: the array you want to sort. It must be an array, array expression, or `null`. Elements in the array must be sortable.
- `array1, [array2, array3...]`: the sorting array used to sort `array0`. It must be an array, array expression, or `null`.

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

INSERT INTO test_array_sortby_muliti VALUES
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


select * from test_array_sortby_muliti order by id asc;
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

Example 1: Sort `array_col1` according to `array_col2`. 

```Plaintext
select id, array_col1, array_col2, array_sortby_multi(array_col1, array_col2) from test_array_sortby_muliti order by id asc;
+------+------------+---------------------+--------------------------------------------+
| id   | array_col1 | array_col2          | array_sortby_multi(array_col1, array_col2) |
+------+------------+---------------------+--------------------------------------------+
|    1 | [4,3,5]    | [1.1,2.2,2.2]       | [4,3,5]                                    |
|    2 | [6,7,8]    | [6.6,5.5,6.6]       | [7,6,8]                                    |
|    3 | NULL       | [7.7,8.8,8.8]       | NULL                                       |
|    4 | [9,10,11]  | NULL                | [9,10,11]                                  |
|    5 | [12,13,14] | [10.1,11.11,11.11]  | [12,13,14]                                 |
|    6 | [15,16,17] | [14.14,13.13,14.14] | [16,15,17]                                 |
|    7 | [18,19,20] | [16.16,16.16,18.18] | [18,19,20]                                 |
|    8 | [21,22,23] | [19.19,20.2,19.19]  | [21,23,22]                                 |
|    9 | [24,25,26] | NULL                | [24,25,26]                                 |
|   10 | [24,25,26] | NULL                | [24,25,26]                                 |
+------+------------+---------------------+--------------------------------------------+
```

Example 2: Sort `array_col1` according to `array_col2`, `array_col3`. 

```Plaintext
select id, array_col1, array_col2, array_col3, array_sortby_multi(array_col1, array_col2, array_col3) from test_array_sortby_muliti order by id asc;
+------+------------+---------------------+---------------+--------------------------------------------------------+
| id   | array_col1 | array_col2          | array_col3    | array_sortby_multi(array_col1, array_col2, array_col3) |
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

Example 3: Sort `array_col1` according to `array_col2`, `array_col3`, `array_col4`. 

```Plaintext
select id, array_col1, array_col2, array_col3, array_col4, array_sortby_multi(array_col1, array_col2, array_col3, array_col4) from test_array_sortby_muliti order by id asc;
+------+------------+---------------------+---------------+------------------------------------------+--------------------------------------------------------------------+
| id   | array_col1 | array_col2          | array_col3    | array_col4                               | array_sortby_multi(array_col1, array_col2, array_col3, array_col4) |
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
