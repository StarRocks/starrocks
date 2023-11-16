---
displayed_sidebar: "Chinese"
---

# 关键字

本文介绍了什么是非保留关键字和保留关键字，并列举了 StarRocks 中的所有保留关键字。

## 简介

关键字在 SQL 语句中具有特殊的含义，比如 `CREATE` 和 `DROP`，其中：

- **非保留关键字** (Non-reserved keywords) 可以直接作为标识符，如表名和列名，不需要做特殊处理。例如，DB 是非保留关键字，如要创建一个名为 `DB` 的数据库，语法如下。

    ```SQL
    CREATE DATABASE DB;
    Query OK, 0 rows affected (0.00 sec)
    ```

- **保留关键字** (Reserved keywords) 不能直接作为标识符给变量或者函数等命名，需要做特殊处理。在 StarRocks 中，使用保留关键字作为标识符，需要用反引号 (`\`) 将其括起。例如，`LIKE` 为保留关键字，如要创建一个名为 `LIKE` 的数据库，语法如下。

    ```SQL
    CREATE DATABASE `LIKE`;
    Query OK, 0 rows affected (0.01 sec)
    ```

  如果未使用反引号 (`\`) 包裹，则会报错：

   ```SQL
    CREATE DATABASE LIKE;
    ERROR 1064 (HY000): Getting syntax error at line 1, column 16. Detail message: Unexpected input 'like', the most similar input is {a legal identifier}.
    ```

## 保留关键字

下面按照字母顺序列举了 StarRocks 中所有保留关键字，使用时需要用反引号 (`\`) 包裹。StarRocks 不同版本中的保留关键字可能会有不同。

### A

- ADD
- ALL
- ALTER
- ANALYZE
- AND
- ARRAY
- AS
- ASC

### B

- BETWEEN
- BIGINT
- BITMAP
- BOTH
- BY

### C

- CASE
- CHAR
- CHARACTER
- CHECK
- COLLATE
- COLUMN
- COMPACTION (3.1 及以后)
- CONVERT
- CREATE
- CROSS
- CUBE
- CURRENT_DATE
- CURRENT_ROLE (3.0 及以后)
- CURRENT_TIME
- CURRENT_TIMESTAMP
- CURRENT_USER

### D

- DATABASE
- DATABASES
- DECIMAL
- DECIMALV2
- DECIMAL32
- DECIMAL64
- DECIMAL128
- DEFAULT
- DEFERRED (3.0 及以后)
- DELETE
- DENSE_RANK
- DESC
- DESCRIBE
- DISTINCT
- DOUBLE
- DROP
- DUAL

### E

- ELSE
- EXCEPT
- EXISTS
- EXPLAIN

### F

- FALSE
- FIRST_VALUE
- FLOAT
- FOR
- FORCE
- FROM
- FULL
- FUNCTION

### G

- GRANT
- GROUP
- GROUPS
- GROUPING
- GROUPING_ID

### H

- HAVING
- HLL
- HOST

### I

- IF
- IGNORE
- IMMEDIATE (3.0 及以后)
- IN
- INDEX
- INFILE
- INNER
- INSERT
- INT
- INTEGER
- INTERSECT
- INTO
- IS

### J

- JOIN
- JSON

### K

- KEY
- KEYS
- KILL

### L

- LAG
- LARGEINT
- LAST_VALUE
- LATERAL
- LEAD
- LEFT
- LIKE
- LIMIT
- LOAD
- LOCALTIME
- LOCALTIMESTAMP

### M

- MAXVALUE
- MINUS
- MOD

### N

- NTILE
- NOT
- NULL

### O

- ON
- OR
- ORDER
- OUTER
- OUTFILE
- OVER

### P

- PARTITION
- PERCENTILE
- PRIMARY
- PROCEDURE

### Q

- QUALIFY

### R

- RANGE
- RANK
- READ
- REGEXP
- RELEASE
- RENAME
- REPLACE
- REVOKE
- RIGHT
- RLIKE
- ROW
- ROWS
- ROW_NUMBER

### S

- SCHEMA
- SCHEMAS
- SELECT
- SET
- SET_VAR
- SHOW
- SMALLINT
- SYSTEM

### T

- TABLE
- TERMINATED
- TEXT (3.1 及以后)
- THEN
- TINYINT
- TO
- TRUE

### U

- UNION
- UNIQUE
- UNSIGNED
- UPDATE
- USE
- USING

### V

- VALUES
- VARCHAR

### W

- WHEN
- WHERE
- WITH
