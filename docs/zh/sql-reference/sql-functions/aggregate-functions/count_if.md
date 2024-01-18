---
displayed_sidebar: "Chinese"
---

# count_if

## 功能

计算满足指定条件（`true`）的记录数。

该函数不支持 `DISTINCT`，举例：`count_if(DISTINCT x)`。

该函数会内部转化为 `COUNT` + `IF`:

- 转化前：`COUNT_IF(x)`  
- 转化后：`COUNT(IF(x, 1, NULL))`

## 语法

```Haskell
COUNT_IF(condition)
```

## 参数说明

`condition`: 条件表达式。可以在一条语句中指定多个 count_if 条件。

## 返回值说明

返回一个整数数值。如果没有匹配的记录，返回 0。该函数忽略 NULL 值。

## 示例

假设有表 `test_count_if`。

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

示例一：计算表中 `v2` 列的值为 NULL 的行数。

```Plain
select count_if(v2 is null) from test_count_if;
+----------------------+
| count_if(v2 IS NULL) |
+----------------------+
|                    3 |
+----------------------+
```

示例二：计算满足条件 `v1 >= v2 or v4 = 1` 的行数。

```Plain
select count_if(v1 >= v2 or v4 = 1)from test_count_if;
+----------------------------------+
| count_if((v1 >= v2) OR (v4 = 1)) |
+----------------------------------+
|                                3 |
+----------------------------------+
```

示例三：指定多个 count_if 条件。

```Plain
select count_if(v1 >= v2), count_if(v1 >= v2 or v4 = 1), count_if(v1 >= v2 and v4 = 1)
from test_count_if;
+--------------------+----------------------------------+-----------------------------------+
| count_if(v1 >= v2) | count_if((v1 >= v2) OR (v4 = 1)) | count_if((v1 >= v2) AND (v4 = 1)) |
+--------------------+----------------------------------+-----------------------------------+
|                  2 |                                3 |                                 1 |
```
