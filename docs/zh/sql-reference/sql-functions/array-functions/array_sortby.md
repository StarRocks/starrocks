---
displayed_sidebar: "Chinese"
---

# array_sortby

## 功能

对数组中的元素根据另外一个键值数组元素或者 Lambda 函数生成的键值数组元素进行升序排列。有关 Lambda 表达式的详细信息，参见 [Lambda expression](../Lambda_expression.md)。该函数从 2.5 版本开始支持。

举例，有两个数组 a = [3,1,4]，b = [7,5,6]。将 `b` 作为排序键，对 `a` 里的元素进行排序。

根据键值对关系，`b` 的元素 [7,5,6] 一一对应 `a` 的 元素[3,1,4]。

转换前：

| 数组  | 第一个元素  | 第二个元素 | 第三个元素 |
| ---- | ---------- | ---------- | ---------- |
| a    | 3          | 1          | 4          |
| b    | 7          | 5          | 6          |

转换后，b 按照升序排列为 [5,6,7]，对应 a 的元素位置也进行相应调整，变为 [1,4,3]。

| 数组 | 第一个元素 | 第二个元素 | 第三个元素 |
| ---- | ---------- | ---------- | ---------- |
| a    | 1          | 4          | 3          |
| b    | 5          | 6          | 7          |

## 语法

```Haskell
array_sortby(array0, array1)
array_sortby(<lambda function>, array0 [, array1...])
```

- `array_sortby(array0, array1)`

  根据 `array1` 的键值数组元素对 `array0` 进行升序排序。

- `array_sortby(<lambda_function>, array0 [, array1...])`

  根据 `lambda_function` 生成的键值数组元素，对 `array0` 进行升序排序。

## 参数说明

- `array0`：需要排序的数组，支持的数据类型为 ARRAY，或者 `null`。数组中的元素必须为可排序的元素。
- `array1`：用于排序的键值数组，支持的数据类型为 ARRAY，或者 `null`。
- `lambda_function`：lambda 函数，用于生成排序键值数组。

## 返回值说明

返回的数据类型为 ARRAY。

## 注意事项

- 只支持升序排序。
- 如果需要降序排列，可以对排序后的结果，调用 [reverse()](../string-functions/reverse.md) 函数。
- `null` 值会排在最前面。
- 返回数组中的元素类型和输入数组中的元素类型一致，`null` 属性一致。
- 如果用于排序的键值数组或表达式为 `null`，数据保持不变。
- 排序涉及的两个数组的元素个数必须一致，否则返回报错。

## 示例

下面的示例使用如下数据表。

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

示例一：将数组 `c3` 按照 `c2` 的值进行升序排序。

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

示例二：将数组 `c3` 按照 Lambda 表达式生成的键值数组进行升序排序。该函数与上个示例功能对等。

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
|    1 | [82,1,4]   | [4,3,5]     | [3,4,5]       | [1,82,4]        |
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

示例三：将数组 `c3` 按照按照 c2+c3 的和的升序排序。

```SQL
select
    c3,
    c2,
    array_map((x,y)-> x+y,c3,c2) as sum,
    array_sort(array_map((x,y)-> x+y,c3,c2)) as sorted_sum,
    array_sortby((x,y) -> x+y , c3,c2) as sorted_c3_by_sum
from test_array where c1=1;
+----------+---------+----------+------------+------------------+
| c3       | c2      | sum      | sorted_sum | sorted_c3_by_sum |
+----------+---------+----------+------------+------------------+
| [82,1,4] | [4,3,5] | [86,4,9] | [4,9,86]   | [1,4,82]         |
+----------+---------+----------+------------+------------------+
```

## 相关文档

[array_sort](./array_sort.md)
