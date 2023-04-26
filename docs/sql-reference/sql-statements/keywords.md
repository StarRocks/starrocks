# Keywords

This topic describes non-reserved keywords and reserved keywords. It also provides a list of reserved keywords supported by StarRocks.

## Introduction

Keywords in SQL statements, such as `CREATE` and `DROP`, have special meanings when parsed by StarRocks. Keywords are classified into non-reserved keywords and reserved keywords.

- **Non-reserved keywords** can be directly used as identifiers, such as table names and column names. For example, DB is a non-reserved keyword. You can use the following statement to create a database named `DB`.

    ```SQL
    CREATE DATABASE DB;
    Query OK, 0 rows affected (0.00 sec)
    ```

- **Reserved keywords** can be used as identifiers only after special treatment. For example, LIKE is a reserved keyword. If you want to use it to identify a database, enclose it in a pair of backticks (\`).

    ```SQL
    CREATE DATABASE `LIKE`;
    Query OK, 0 rows affected (0.01 sec)
    ```

## Reserved keywords

Following are the reserved keywords supported by StarRocks.

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
- AUTO_INCREMENT (v3.0 and later)

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
- CHARACTER
- CHECK
- COLLATE
- COLUMN
- CONVERT
- CREATE
- CROSS
- CUBE
- CURRENT_DATE
- CURRENT_TIME
- CURRENT_TIMESTAMP
- CURRENT_USER
- CURRENT_ROLE (v3.0 and later)

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
- DEFERRED (v3.0 and later)

### E

- ELSE
- EXCEPT
- EXISTS
- EXPLAIN
- EXPORT
- ENCLOSE (v3.0 and later)
- ESCAPE (v3.0 and later)

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
- IMMEDIATE (v3.0 and later)

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
- PRIVILEGES (v3.0 and later)

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
- SEMI
- SET
- SET_VAR
- SHOW
- SMALLINT
- SYSTEM
- SWAP
- SKIP_HEADER (v3.0 and later)

### T

- TABLE
- TERMINATED
- THEN
- TINYINT
- TO
- TRUE
- TRIM_SPACE (v3.0 and later)

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
- VARBINARY (v3.0 and later)

### W

- WHEN
- WHERE
- WITH
