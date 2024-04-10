---
displayed_sidebar: "English"
---

# count_if

## Description

Returns the number of records that meet the specified condition or `0` if no records satisfy the condition.

This function does not support `DISTINCT`, for example `count_if(DISTINCT x)`.

This function is internally transformed to `COUNT` + `IF`:

- Before: `COUNT_IF(x)`
- After: `COUNT(IF(x, 1, NULL))`

## Syntax

```Haskell
COUNT_IF(condition)
```

## Parameters

`condition`: the condition that should evaluate to a BOOLEAN value. You can specify multiple count_if conditions in one SELECT statement.

## Return value

Returns a numeric value. If no records can be found, `0` is returned. This function ignores NULL values.

## Examples

Suppose there is a table named `test_count_if`.

```Plain
CREATE TABLE `test_count_if` (
  `v1` varchar(65533) NULL COMMENT "",
  `v2` varchar(65533) NULL COMMENT "",
  `v3` datetime NULL COMMENT "",
  `v4` int null
) ENGINE=OLAP
DUPLICATE KEY(v1, v2, v3)
PARTITION BY RANGE(`v3`)
(PARTITION p20220418 VALUES [("2022-04-18 00:00:00"), ("2022-04-19 00:00:00")),
PARTITION p20220419 VALUES [("2022-04-19 00:00:00"), ("2022-04-20 00:00:00")),
PARTITION p20220420 VALUES [("2022-04-20 00:00:00"), ("2022-04-21 00:00:00")),
PARTITION p20220421 VALUES [("2022-04-21 00:00:00"), ("2022-04-22 00:00:00")))
DISTRIBUTED BY HASH(`v1`) BUCKETS 4;

INSERT INTO test_count_if VALUES
('a','a', '2022-04-18 01:01:00', 1),
('a','b', '2022-04-18 02:01:00', NULL),
('a',NULL, '2022-04-18 02:05:00', 1),
('a','b', '2022-04-18 02:15:00', 3),
('a','b', '2022-04-18 03:15:00', 7),
('c',NULL, '2022-04-18 03:45:00', NULL),
('c',NULL, '2022-04-18 03:25:00', 2),
('c','a', '2022-04-18 03:27:00', 3);

SELECT * FROM test_count_if;
+------+------+---------------------+------+
| v1   | v2   | v3                  | v4   |
+------+------+---------------------+------+
| a    | NULL | 2022-04-18 02:05:00 |    1 |
| a    | a    | 2022-04-18 01:01:00 |    1 |
| a    | b    | 2022-04-18 02:01:00 | NULL |
| a    | b    | 2022-04-18 02:15:00 |    3 |
| a    | b    | 2022-04-18 03:15:00 |    7 |
| c    | NULL | 2022-04-18 03:25:00 |    2 |
| c    | NULL | 2022-04-18 03:45:00 | NULL |
| c    | a    | 2022-04-18 03:27:00 |    3 |
+------+------+---------------------+------+
```

Example 1: Count the number of rows in table `test_count_if` where `v2` is null.

```Plain
select count_if(v2 is null) from test_count_if;
+----------------------+
| count_if(v2 IS NULL) |
+----------------------+
|                    3 |
+----------------------+
```

Example 2: Count the number of rows where `v1 >= v2 or v4 = 1`

```Plain
select count_if(v1 >= v2 or v4 = 1)from test_count_if;
+----------------------------------+
| count_if((v1 >= v2) OR (v4 = 1)) |
+----------------------------------+
|                                3 |
+----------------------------------+
```

Example 3: Specify multiple conditions.

```Plain
select count_if(v1 >= v2), count_if(v1 >= v2 or v4 = 1), count_if(v1 >= v2 and v4 = 1) from test_count_if;
+--------------------+----------------------------------+-----------------------------------+
| count_if(v1 >= v2) | count_if((v1 >= v2) OR (v4 = 1)) | count_if((v1 >= v2) AND (v4 = 1)) |
+--------------------+----------------------------------+-----------------------------------+
|                  2 |                                3 |                                 1 |
```
