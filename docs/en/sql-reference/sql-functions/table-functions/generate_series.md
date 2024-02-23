---
displayed_sidebar: "English"
---

# generate_series

## Description

Generates a series of integer or datetime values within the interval specified by `start` and `end`, and with an optional `step`.

generate_series() is a table function. A table function can return a row set for each input row. The row set can contain zero, one, or multiple rows. Each row can contain one or more columns.

To use generate_series() in StarRocks, you must enclose it in the TABLE keyword if the input parameters are constants. If the input parameters are expressions, such as column names, the TABLE keyword is not required (See Example 5).

This function is supported from v3.1.

## Syntax

```SQL
generate_series(start, end [,step])
```

## Parameters

- `start`: the starting value of the series, required. Supported data types are INT, BIGINT, LARGEINT, DATE, and DATETIME.
- `end`: the ending value of the series, required. Supported data types are INT, BIGINT, LARGEINT, DATE, and DATETIME.
- `step`: the value to increment or decrement, optional. Supported data types are INT, BIGINT, and LARGEINT. If not specified, the default step is 1. `step` can be either negative or positive, but cannot be zero. when generate time series, `step` can support integer(default unit DAY) and time interval with following values: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND.

When generate integer series, the three parameters must have the same data type, for example, `generate_series(INT start, INT end [, INT step])`.
When generate time series, `start` and `end` must be DATE/DATETIME or can be cast to DATETIME, invalid input datetime parameter will return zero rows, for example, `generate_series(DATETIME start, DATETIME end [, INTERVAL step DAY])`

It supports named arguments from v3.3, where all parameters are input with names, like `name=>expr`, for example, `generate_series(start=>3, end=>7, step=>2)`. Named arguments can disorder arguments and optionally set default arguments, but can't mix with positional arguments.

## Return value

Returns a series of values that have the same as the input parameters `start` and `end`.

- When `step` is positive, zero rows are returned if `start` is greater than `end`. Conversely, when `step` is negative, zero rows are returned if `start` is less than `end`.
- An error is returned if `step` is 0.
- This function deals with nulls as follows: If any input parameter is a literal null, an error is reported. If any input parameter is an expression and the result of the expression is null, 0 rows are returned (See Example 5).

## Examples

Example 1: Generate a sequence of values within the range [2,5] in ascending order with the default step `1`.

```SQL
MySQL > select * from TABLE(generate_series(2, 5));
+-----------------+
| generate_series |
+-----------------+
|               2 |
|               3 |
|               4 |
|               5 |
+-----------------+
```

Example 2: Generate a sequence of values within the range [2,5] in ascending order with the specified step `2`.

```SQL
MySQL > select * from TABLE(generate_series(2, 5, 2));
+-----------------+
| generate_series |
+-----------------+
|               2 |
|               4 |
+-----------------+
```

Example 3: Generate a sequence of values within the range [5,2] in descending order with the specified step `-1`.

```SQL
MySQL > select * from TABLE(generate_series(5, 2, -1));
+-----------------+
| generate_series |
+-----------------+
|               5 |
|               4 |
|               3 |
|               2 |
+-----------------+
```

Example 4: Zero rows are returned when `step` is negative and `start` is less than `end`.

```SQL
MySQL > select * from TABLE(generate_series(2, 5, -1));
Empty set (0.01 sec)
```

Example 5: Use table columns as the input parameters of generate_series(). In this use case, you do not need to use `TABLE()` with generate_series().

```SQL
CREATE TABLE t_numbers(start INT, end INT)
DUPLICATE KEY (start)
DISTRIBUTED BY HASH(start) BUCKETS 1;

INSERT INTO t_numbers VALUES
(1, 3),
(5, 2),
(NULL, 10),
(4, 7),
(9,6);

SELECT * FROM t_numbers;
+-------+------+
| start | end  |
+-------+------+
|  NULL |   10 |
|     1 |    3 |
|     4 |    7 |
|     5 |    2 |
|     9 |    6 |
+-------+------+

-- Generate multiple rows for rows (1,3) and (4,7) with step 1.
SELECT * FROM t_numbers, generate_series(t_numbers.start, t_numbers.end);
+-------+------+-----------------+
| start | end  | generate_series |
+-------+------+-----------------+
|     1 |    3 |               1 |
|     1 |    3 |               2 |
|     1 |    3 |               3 |
|     4 |    7 |               4 |
|     4 |    7 |               5 |
|     4 |    7 |               6 |
|     4 |    7 |               7 |
+-------+------+-----------------+

-- Generate multiple rows for rows (5,2) and (9,6) with step -1.
SELECT * FROM t_numbers, generate_series(t_numbers.start, t_numbers.end, -1);
+-------+------+-----------------+
| start | end  | generate_series |
+-------+------+-----------------+
|     5 |    2 |               5 |
|     5 |    2 |               4 |
|     5 |    2 |               3 |
|     5 |    2 |               2 |
|     9 |    6 |               9 |
|     9 |    6 |               8 |
|     9 |    6 |               7 |
|     9 |    6 |               6 |
+-------+------+-----------------+
```
Example 6: Generate a sequence of values within the range [2,5] in ascending order with the specified step `2`, using named arguments.

```SQL
MySQL > select * from TABLE(generate_series(start=>2, end=>5, step=>2));
+-----------------+
| generate_series |
+-----------------+
|               2 |
|               4 |
+-----------------+
```
Example 7: Generate time series within the range ['2023-12-31 23:59:56', '2024-01-01 00:00:06'] in ascending order with the specified time interval step `3` second.

```SQL
MySQL > select * from TABLE(generate_series('2023-12-31 23:59:56', '2024-01-01 00:00:06', interval 3 second));
+---------------------+
| generate_series     |
+---------------------+
| 2023-12-31 23:59:56 |
| 2023-12-31 23:59:59 |
| 2024-01-01 00:00:02 |
| 2024-01-01 00:00:05 |
+---------------------+
```
Example 8: Generate time series within the range ['2023-11-25', '2024-01-20'] with the specified integer step `15`, default time unit DAY.

```SQL
MySQL > select * from TABLE(generate_series('2023-11-25', '2024-01-20', 15));
+---------------------+
| generate_series     |
+---------------------+
| 2023-11-25 00:00:00 |
| 2023-12-10 00:00:00 |
| 2023-12-25 00:00:00 |
| 2024-01-09 00:00:00 |
+---------------------+
```
Example 9: Generate time series within the range ['2023-02-27', '2023-03-02'] without specified step, default step `INTERVAL 1 DAY`.

```SQL
MySQL > select * from table(generate_series('2023-02-27', '2023-03-02'));
+---------------------+
| generate_series     |
+---------------------+
| 2023-02-27 00:00:00 |
| 2023-02-28 00:00:00 |
| 2023-03-01 00:00:00 |
| 2023-03-02 00:00:00 |
+---------------------+
```
Example 10: Use table columns as the input parameters of when generate time series. In this use case, you do not need to use `TABLE()` with generate_series().

```SQL
CREATE TABLE t_datetime (seq int, start datetime, end datetime, step int)
DUPLICATE KEY (seq)
DISTRIBUTED BY HASH(seq) BUCKETS 1;

INSERT INTO t_datetime VALUES
(1, '2023-12-31 23:59:57', '2024-01-01 00:00:04', 2),
(2, '2024-01-01 00:00:06', '2023-12-31 23:59:56', -3),
(3, NULL, '2024-01-01 00:00:04', 2),
(4, '2023-12-31 23:59:57', NULL, 2),
(5, '2023-12-31 23:59:57', '2024-01-01 00:00:04', NULL);

SELECT * FROM t_datetime;
+------+---------------------+---------------------+------+
| seq  | start               | end                 | step |
+------+---------------------+---------------------+------+
|    1 | 2023-12-31 23:59:57 | 2024-01-01 00:00:04 |    2 |
|    2 | 2024-01-01 00:00:06 | 2023-12-31 23:59:56 |   -3 |
|    3 | NULL                | 2024-01-01 00:00:04 |    2 |
|    4 | 2023-12-31 23:59:57 | NULL                |    2 |
|    5 | 2023-12-31 23:59:57 | 2024-01-01 00:00:04 | NULL |
+------+---------------------+---------------------+------+

-- Generate multiple rows for rows with step interval 3 second.
SELECT * FROM t_datetime, generate_series(t_datetime.start, t_datetime.end, interval 3 second);
+------+---------------------+---------------------+------+---------------------+
| seq  | start               | end                 | step | generate_series     |
+------+---------------------+---------------------+------+---------------------+
|    1 | 2023-12-31 23:59:57 | 2024-01-01 00:00:04 |    2 | 2023-12-31 23:59:57 |
|    1 | 2023-12-31 23:59:57 | 2024-01-01 00:00:04 |    2 | 2024-01-01 00:00:00 |
|    1 | 2023-12-31 23:59:57 | 2024-01-01 00:00:04 |    2 | 2024-01-01 00:00:03 |
|    5 | 2023-12-31 23:59:57 | 2024-01-01 00:00:04 | NULL | 2023-12-31 23:59:57 |
|    5 | 2023-12-31 23:59:57 | 2024-01-01 00:00:04 | NULL | 2024-01-01 00:00:00 |
|    5 | 2023-12-31 23:59:57 | 2024-01-01 00:00:04 | NULL | 2024-01-01 00:00:03 |
+------+---------------------+---------------------+------+---------------------+

-- Generate multiple rows for rows with step column.
SELECT * FROM t_datetime, generate_series(t_datetime.start, t_datetime.end, interval t_datetime.step second);
+------+---------------------+---------------------+------+---------------------+
| seq  | start               | end                 | step | generate_series     |
+------+---------------------+---------------------+------+---------------------+
|    1 | 2023-12-31 23:59:57 | 2024-01-01 00:00:04 |    2 | 2023-12-31 23:59:57 |
|    1 | 2023-12-31 23:59:57 | 2024-01-01 00:00:04 |    2 | 2023-12-31 23:59:59 |
|    1 | 2023-12-31 23:59:57 | 2024-01-01 00:00:04 |    2 | 2024-01-01 00:00:01 |
|    1 | 2023-12-31 23:59:57 | 2024-01-01 00:00:04 |    2 | 2024-01-01 00:00:03 |
|    2 | 2024-01-01 00:00:06 | 2023-12-31 23:59:56 |   -3 | 2024-01-01 00:00:06 |
|    2 | 2024-01-01 00:00:06 | 2023-12-31 23:59:56 |   -3 | 2024-01-01 00:00:03 |
|    2 | 2024-01-01 00:00:06 | 2023-12-31 23:59:56 |   -3 | 2024-01-01 00:00:00 |
|    2 | 2024-01-01 00:00:06 | 2023-12-31 23:59:56 |   -3 | 2023-12-31 23:59:57 |
+------+---------------------+---------------------+------+---------------------+
```

## keywords

table function, generate series
