---
displayed_sidebar: docs
---

# unnest

## Description

UNNEST is a table function that takes an array and converts elements in that array into multiple rows of a table. The conversion is also known as "flattening".

You can use Lateral Join with UNNEST to implement common conversions, for example, from STRING, ARRAY, or BITMAP to multiple rows. For more information, see [Lateral join](../../../using_starrocks/Lateral_join.md).

From v2.5, UNNEST can take a variable number of array parameters. The arrays can vary in type and length (number of elements). If the arrays have different lengths, the largest length prevails, which means nulls will be added to arrays that are less than this length. See [Example 2](#example-2-unnest-takes-multiple-parameters) for more information.

From v3.2.7, UNNEST can be used with LEFT JOIN ON TRUE, which is to retain all rows in the left table even if the corresponding rows in the right table are empty or have null values. NULLs are returned for such empty or NULL rows. See [Example 3](#example-3-unnest-with-left-join-on-true) for more information.

## Syntax

```Haskell
unnest(array0[, array1 ...])
```

## Parameters

`array`: the array you want to convert. It must be an array or expression that can evaluate to an ARRAY data type. You can specify one or more arrays or array expressions.

## Return value

Returns the multiple rows converted from the array. The type of return value depends on the types of elements in the array.

For the element types supported in an array, see [ARRAY](../../data-types/semi_structured/Array.md).

## Usage notes

- UNNEST is a table function. It must be used with Lateral Join but the keyword Lateral Join does not need to be explicitly specified.
- If the array expression evaluates to NULL or it is empty, no rows will be returned (except for LEFT JOIN ON TRUE).
- If an element in the array is NULL, NULL is returned for that element.

## Examples

### Example 1: UNNEST takes one parameter

```SQL
-- Create table student_score where scores is an ARRAY column.
CREATE TABLE student_score
(
    `id` bigint(20) NULL COMMENT "",
    `scores` ARRAY<int> NULL COMMENT ""
)
DUPLICATE KEY (id)
DISTRIBUTED BY HASH(`id`);

-- Insert data into this table.
INSERT INTO student_score VALUES
(1, [80,85,87]),
(2, [77, null, 89]),
(3, null),
(4, []),
(5, [90,92]);

-- Query data from this table.
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

-- Use UNNEST to flatten the scores column into multiple rows.
SELECT id, scores, unnest FROM student_score, unnest(scores) AS unnest;
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

[80,85,87] corresponding to `id = 1` is converted into three rows.

[77,null,89] corresponding to `id = 2` retains the null value.

`scores` corresponding to  `id = 3` and `id = 4` are NULL and empty, which are skipped.

### Example 2: UNNEST takes multiple parameters

```SQL
-- Create table example_table where the type and scores columns vary in type.
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

-- Insert data into the table.
INSERT INTO example_table VALUES
("1", "typeA;typeB", [80,85,88]),
("2", "typeA;typeB;typeC", [87,90,95]);

-- Query data from the table.
SELECT * FROM example_table;
+------+-------------------+------------+
| id   | type              | scores     |
+------+-------------------+------------+
| 1    | typeA;typeB       | [80,85,88] |
| 2    | typeA;typeB;typeC | [87,90,95] |
+------+-------------------+------------+

-- Use UNNEST to convert type and scores into multiple rows.
SELECT id, unnest.type, unnest.scores
FROM example_table, unnest(split(type, ";"), scores) AS unnest(type,scores);
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

`type` and  `scores` in `UNNEST` vary in type and length.

`type` is a VARCHAR column while `scores` is an ARRAY column. The split() function is used to convert `type` into ARRAY.

For `id = 1`, `type` is converted into ["typeA","typeB"], which has two elements.

For `id = 2`, `type` is converted into  ["typeA","typeB","typeC"], which has three elements.

To ensure consistent numbers of rows for each `id`, a null element is added to ["typeA","typeB"].

### Example 3: UNNEST with LEFT JOIN ON TRUE

```SQL
-- Create table student_score where scores is an ARRAY column.
CREATE TABLE student_score
(
`id` bigint(20) NULL COMMENT "",
`scores` ARRAY<int> NULL COMMENT ""
)
DUPLICATE KEY (id)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
"replication_num" = "1"
);

-- Insert data into this table.
INSERT INTO student_score VALUES
(1, [80,85,87]),
(2, [77, null, 89]),
(3, null),
(4, []),
(5, [90,92]);

-- Query data from this table.
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

-- Use LEFT JOIN ON TRUE.
SELECT id, scores, unnest FROM student_score LEFT JOIN unnest(scores) AS unnest ON TRUE ORDER BY 1, 3;
+------+--------------+--------+
| id   | scores       | unnest |
+------+--------------+--------+
|  1   | [80,85,87]   |     80 |
|  1   | [80,85,87]   |     85 |
|  1   | [80,85,87]   |     87 |
|  2   | [77,null,89] |   NULL |
|  2   | [77,null,89] |     77 |
|  2   | [77,null,89] |     89 |
|  3   | NULL         |   NULL |
|  4   | []           |   NULL |
|  5   | [90,92]      |     90 |
|  5   | [90,92]      |     92 |
+------+--------------+--------+
```

[80,85,87] corresponding to `id = 1` is converted into three rows.

The null value in [77,null,89] corresponding to `id = 2` is retained.

`scores` corresponding to `id = 3` and `id = 4` are NULL and empty. Left Join reserves these two rows and returns NULLs for them.
