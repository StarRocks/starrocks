---
displayed_sidebar: "Chinese"
---

# unnest

## 功能

UNNEST 是一种表函数 (table function)，用于将一个数组展开成多行。

您可以将 StarRocks 的 Lateral Join 与 UNNEST 功能结合使用，实现常见的列转行逻辑，比如展开 STRING，ARRAY，和 BITMAP 类型的数据。更多使用示例，参见 [Lateral join](../../../using_starrocks/Lateral_join.md)。

从 2.5 版本开始，UNNEST 支持传入多个 array 参数，并且多个 array 的元素类型和长度（元素个数）可以不同。对于长度不同的情况，以最长数组的长度为基准，长度小于这个长度的数组使用 NULL 进行元素补充，参见 [示例二](#示例二unnest-接收多个参数)。

## 语法

```Haskell
unnest(array0[, array1 ...])
```

## 参数说明

`array`：待转换的数组或者能转化成数组的表达式，必填。

### 返回值说明

返回数组展开后的多行数据。返回值的数据类型取决于数组中的元素类型。

有关 StarRocks 支持的数组元素类型，请参见 [ARRAY](../../sql-statements/data-types/Array.md)。

## 注意事项

- UNNEST 必须与 lateral join 一起使用，但是 lateral join 关键字可以在查询中省略。
- 支持输入多个数组，数组的长度和类型可以不同。
- 如果输入的数组为 NULL 或 空，则计算时跳过。
- 如果数组中的某个元素为 NULL，该元素对应的位置返回 NULL。

## **示例**

### 示例一：UNNEST 接收一个参数

```SQL
-- 创建表 student_score，其中 scores 为 ARRAY 类型的列。
CREATE TABLE student_score
(
    `id` bigint(20) NULL COMMENT "",
    `scores` ARRAY<int> NULL COMMENT ""
)
DUPLICATE KEY (id)
DISTRIBUTED BY HASH(`id`);

-- 向表插入数据。
INSERT INTO student_score VALUES
(1, [80,85,87]),
(2, [77, null, 89]),
(3, null),
(4, []),
(5, [90,92]);

--查询表数据。
SELECT * FROM student_score ORDER BY id;
+------+--------------+
| id   | scores       |
+------+--------------+
|    1 | [80,85,87]   |
|    2 | [77,null,89] |
|    3 | NULL         |
|    4 | []           |
|    5 | [90,92]      |
+------+--------------+

-- 将 scores 列中的数组元素展开成多行。
SELECT id, scores, unnest FROM student_score, unnest(scores);
+------+--------------+--------+
| id   | scores       | unnest |
+------+--------------+--------+
|    1 | [80,85,87]   |     80 |
|    1 | [80,85,87]   |     85 |
|    1 | [80,85,87]   |     87 |
|    2 | [77,null,89] |     77 |
|    2 | [77,null,89] |   NULL |
|    2 | [77,null,89] |     89 |
|    5 | [90,92]      |     90 |
|    5 | [90,92]      |     92 |
+------+--------------+--------+
```

可以看到对于 `id = 1` 的 `scores` 数组，根据元素个数拆成了 3 行。

`id = 2`的 `scores` 数组中包含 null 元素，对应位置返回 null。

`id = 3` 和 `id = 4` 的 `scores` 数组分别是 NULL 和 空，计算时跳过。

### 示例二：UNNEST 接收多个参数

```SQL
-- 创建表。
CREATE TABLE example_table (
id varchar(65533) NULL COMMENT "",
type varchar(65533) NULL COMMENT "",
scores ARRAY<int> NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(id)
COMMENT "OLAP"
DISTRIBUTED BY HASH(id)
PROPERTIES (
"replication_num" = "3");

-- 向表插入数据。
INSERT INTO example_table VALUES
("1", "typeA;typeB", [80,85,88]),
("2", "typeA;typeB;typeC", [87,90,95]);

-- 查询表中数据。
SELECT * FROM example_table;
+------+-------------------+------------+
| id   | type              | scores     |
+------+-------------------+------------+
| 1    | typeA;typeB       | [80,85,88] |
| 2    | typeA;typeB;typeC | [87,90,95] |
+------+-------------------+------------+

-- 使用 UNNEST 将 type 和 scores 这两列中的元素展开为多行。
SELECT id, unnest.type, unnest.scores
FROM example_table, unnest(split(type, ";"), scores) as unnest(type,scores);
+------+-------+--------+
| id   | type  | scores |
+------+-------+--------+
| 1    | typeA |     80 |
| 1    | typeB |     85 |
| 1    | NULL  |     88 |
| 2    | typeA |     87 |
| 2    | typeB |     90 |
| 2    | typeC |     95 |
+------+-------+--------+
```

`UNNEST` 函数中的 `type` 列和 `scores` 列数据类型不相同。

`type` 列为 VARCHAR 类型，计算过程中使用 split() 函数转为了 ARRAY 类型。

`id = 1` 的 `type` 转化后得到数组 ["typeA","typeB"]，包含 2 个元素；`id = 2` 的 `type` 转化后得到数组 ["typeA","typeB","typeC"]，包含 3 个元素。以数组最长长度 3 为基准，对 ["typeA","typeB"] 补充了 NULL。
