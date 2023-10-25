# unnest

## 功能

UNNEST 是一种表函数 (table function)，用于将一个数组展开成多行。

您可以将 StarRocks 的 Lateral Join 与 UNNEST 功能结合使用，实现常见的行转列逻辑，比如展开 STRING，ARRAY，和 BITMAP 类型的数据。更多使用示例，参见 [Lateral join](../../../using_starrocks/Lateral_join.md)。

## 语法

```Haskell
unnest(array0[, array1 ...])
```

## 参数说明

`array`：待转换的数组或者能转化成数组的表达式，必填。

### 返回值说明

返回数组展开后的多行数据。返回值的数据类型取决于数组中的元素类型。

有关 StarRocks 支持的数组元素类型，请参见 [ARRAY](../../../using_starrocks/Array.md)。

## 注意事项

- UNNEST 必须与 lateral join 一起使用，但是 lateral join 关键字可以在查询中省略。
- 如果输入的数组为 NULL 或 空，则计算时跳过。
- 如果数组中的某个元素为 NULL，该元素对应的位置返回 NULL。

## **示例**

```SQL
-- 创建表 student_score，其中 scores 为 ARRAY 类型的列。
CREATE TABLE student_score
(
    `id` bigint(20) NULL COMMENT "",
    `scores` ARRAY<int> NULL COMMENT ""
)
DUPLICATE KEY (id)
DISTRIBUTED BY HASH(`id`) BUCKETS 1;

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
