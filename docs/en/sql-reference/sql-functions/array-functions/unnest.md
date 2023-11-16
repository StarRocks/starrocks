# unnest

## Description

UNNEST is a table function that takes an array and converts elements in that array into multiple rows of a table. The conversion is also known as "flattening".

You can use Lateral join with UNNEST to implement common conversions, for example, from STRING, ARRAY, or BITMAP to multiple rows. For more information, see [Lateral join](../../../using_starrocks/Lateral_join.md).

## Syntax

```Haskell
unnest(array)
```

## Parameters

`array`: the array you want to convert. It must be an array or an expression that can evaluate to an ARRAY data type.

## Return value

Returns the multiple rows converted from the array. The type of return value depends on the types of elements in the array.

## Usage notes

- UNNEST is a table function. It must be used with Lateral Join but the keyword Lateral Join does not need to be explicitly specified.
- If the array expression evaluates to NULL or it is empty, no rows will be returned. 
- If an element in the array is NULL, NULL is returned for that element.

## Examples

```SQL
-- Create table student_score where scores is an ARRAY column.
CREATE TABLE student_score
(
    `id` bigint(20) NULL COMMENT "",
    `scores` ARRAY<int> NULL COMMENT ""
)
DUPLICATE KEY (id)
DISTRIBUTED BY HASH(`id`) BUCKETS 1;
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

[80,85,87] corresponding to `id = 1` is converted into three rows.

[77,null,89] corresponding to `id = 2` retains the null value.

`scores` corresponding to  `id = 3` and `id = 4` are NULL and empty, which are skipped.
