---
displayed_sidebar: docs
---

# array_generate

## Description

Returns an array of distinct values within the range specified by `start` and `end`, with an increment of `step`.

This function is supported from v3.1.

## Syntax

```Haskell
ARRAY array_generate([start,] end [, step])
```

## Parameters

- `start`: optional, the start value. It must be a constant or a column that evaluates to TINYINT, SMALLINT, INT, BIGINT, or LARGEINT. The default value is 1.
- `end`: required, the end value. It must be a constant or a column that evaluates to TINYINT, SMALLINT, INT, BIGINT, or LARGEINT.
- `step`: optional, the increment. It must be a constant or a column that evaluates to TINYINT, SMALLINT, INT, BIGINT, or LARGEINT. When `start` is less than `end`, the default value is 1. When `start` is greater than `end`, the default value is -1.

## Return value

Returns an array whose elements have the same data type as the input parameters.

## Usage notes

- If any input parameter is a column, you must specify the table to which the column belongs.
- If any input parameter is a column, you must specify other parameters. Default values are not supported.
- If any input parameter is NULL, NULL is returned.
- If `step` is 0, an empty array is returned.
- If `start` equals to `end`, the value is returned.

## Examples

### The input parameters are constants

```Plain Text
mysql> select array_generate(9);
+---------------------+
| array_generate(9)   |
+---------------------+
| [1,2,3,4,5,6,7,8,9] |
+---------------------+

select array_generate(9,12);
+-----------------------+
| array_generate(9, 12) |
+-----------------------+
| [9,10,11,12]          |
+-----------------------+

select array_generate(9,6);
+----------------------+
| array_generate(9, 6) |
+----------------------+
| [9,8,7,6]            |
+----------------------+

select array_generate(9,6,-1);
+--------------------------+
| array_generate(9, 6, -1) |
+--------------------------+
| [9,8,7,6]                |
+--------------------------+

select array_generate(3,3);
+----------------------+
| array_generate(3, 3) |
+----------------------+
| [3]                  |
+----------------------+
```

### One of the input parameter is a column

```sql
CREATE TABLE `array_generate`
(
  `c1` TINYINT,
  `c2` SMALLINT,
  `c3` INT
)
ENGINE = OLAP
DUPLICATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`);

INSERT INTO `array_generate` VALUES
(1, 6, 3),
(2, 9, 4);
```

```Plain Text
mysql> select array_generate(1,c2,2) from `array_generate`;
+--------------------------+
| array_generate(1, c2, 2) |
+--------------------------+
| [1,3,5]                  |
| [1,3,5,7,9]              |
+--------------------------+
```
