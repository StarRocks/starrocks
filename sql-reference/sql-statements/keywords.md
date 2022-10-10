# 关键字

本文介绍了什么是非保留关键字和保留关键字，并列举了 StarRocks 中的所有保留关键字。

## 简介

关键字在 SQL 语句中具有特殊的含义，比如 `CREATE` 和 `DROP`，其中：

- **非保留关键字**可以直接作为标识符，如表名和列名。例如，DB 是非保留关键字，如要创建一个名为 `DB` 的数据库，语法如下。

    ```SQL
    CREATE DATABASE DB;
    Query OK, 0 rows affected (0.00 sec)
    ```

- **保留关键字**不能直接作为标识符给变量或者函数等命名，需要做特殊处理。在 StarRocks 中，使用保留关键字作为标识符，需要用反引号 (\`) 将其括起。例如，LIKE 为保留关键字，如要创建一个名为 `LIKE` 的数据库，语法如下。

    ```SQL
    CREATE DATABASE `LIKE`;
    Query OK, 0 rows affected (0.01 sec)
    ```

## 保留关键字

下面列举了 StarRocks 中的所有保留关键字。

### A

- ADD
- ALL
- ALTER
- ANALYZE
- AND
- ANTI
- ARRAY
- AS
- ASC
- AUTHENTICATION

### B

- BETWEEN
- BIGINT
- BITMAP
- BOTH
- BY

### C

- CANCEL
- CASE
- CHAR
- CHECK
- COLLATE
- COLUMN
- CONVERT
- CREATE
- CROSS
- CUBE
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
- DELETE
- DENSE_RANK
- DESC
- DESCRIBE
- DISTINCT
- DISTRIBUTED
- DOUBLE
- DROP
- DUAL

### E

- ELSE
- EXCEPT
- EXISTS
- EXPLAIN
- EXPORT

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

### M

- MAXVALUE
- MINUS
- MOD

### N

- NTILE
- NODES
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
- SEMI
- SET
- SET_VAR
- SHOW
- SMALLINT
- SYSTEM
- SWAP

### T

- TABLE
- TERMINATED
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
