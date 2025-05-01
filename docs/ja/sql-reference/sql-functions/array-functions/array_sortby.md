---
displayed_sidebar: docs
---

# array_sortby

## 説明

配列内の要素を、別の配列またはラムダ式から変換された配列の昇順に従ってソートします。詳細は [Lambda expression](../Lambda_expression.md) を参照してください。この関数は v2.5 からサポートされています。

2 つの配列内の要素はキーと値のペアのようなものです。例えば、b = [7,5,6] は a = [3,1,4] のソートキーです。キーと値のペアの関係に基づいて、2 つの配列内の要素は以下のように一対一で対応します。

| **Array** | **Element 1** | **Element 2** | **Element 3** |
| --------- | ------------- | ------------- | ------------- |
| a         | 3             | 1             | 4             |
| b         | 7             | 5             | 6             |

配列 `b` が昇順にソートされると、[5,6,7] になります。これに応じて配列 `a` は [1,4,3] になります。

| **Array** | **Element 1** | **Element 2** | **Element 3** |
| --------- | ------------- | ------------- | ------------- |
| a         | 1             | 4             | 3             |
| b         | 5             | 6             | 7             |

## 構文

```Haskell
array_sortby(array0, array1)
array_sortby(<lambda function>, array0 [, array1...])
```

- `array_sortby(array0, array1)`

   `array1` の昇順に従って `array0` をソートします。

- `array_sortby(<lambda function>, array0 [, array1...])`

   ラムダ関数から返される配列に従って `array0` をソートします。

## パラメータ

- `array0`: ソートしたい配列。配列、配列式、または `null` でなければなりません。配列内の要素はソート可能である必要があります。
- `array1`: `array0` をソートするために使用されるソート配列。配列、配列式、または `null` でなければなりません。
- `lambda function`: ソート配列を生成するために使用されるラムダ式。

## 戻り値

配列を返します。

## 使用上の注意

- この関数は配列の要素を昇順にのみソートできます。
- `NULL` 値は返される配列の先頭に配置されます。
- 配列の要素を降順にソートしたい場合は、[reverse](../string-functions/reverse.md) 関数を使用してください。
- ソート配列 (`array1`) が null の場合、`array0` のデータは変更されません。
- 返される配列の要素は、`array0` の要素と同じデータ型を持ちます。null 値の属性も同じです。
- 2 つの配列は同じ数の要素を持たなければなりません。そうでない場合、エラーが返されます。

## 例

この関数の使用方法を示すために、以下のテーブルを使用します。

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

例 1: `c2` に従って `c3` をソートします。この例では比較のために array_sort() の結果も提供しています。

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

例 2: ラムダ式から生成された `c2` に基づいて配列 `c3` をソートします。この例は例 1 と同等です。比較のために array_sort() の結果も提供しています。

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

例 3: `c2+c3` の昇順に基づいて配列 `c3` をソートします。

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

## 参照

[array_sort](array_sort.md)