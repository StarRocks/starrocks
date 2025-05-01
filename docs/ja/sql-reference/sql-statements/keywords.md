---
displayed_sidebar: docs
---

# Keywords

このトピックでは、非予約キーワードと予約キーワードについて説明します。StarRocks の予約キーワードのリストを提供します。

## Introduction

SQL ステートメント内のキーワード、例えば `CREATE` や `DROP` は、StarRocks によって解析される際に特別な意味を持ちます。キーワードは非予約キーワードと予約キーワードに分類されます。

- **非予約キーワード** は、特別な処理をせずに識別子として直接使用できます。例えば、テーブル名やカラム名です。例えば、`DB` は非予約キーワードです。`DB` という名前のデータベースを作成できます。

    ```SQL
    CREATE DATABASE DB;
    Query OK, 0 rows affected (0.00 sec)
    ```

- **予約キーワード** は、特別な処理をした後でのみ識別子として使用できます。例えば、`LIKE` は予約キーワードです。データベースを識別するために使用したい場合は、バッククォート (`) で囲んでください。

    ```SQL
    CREATE DATABASE `LIKE`;
    Query OK, 0 rows affected (0.01 sec)
    ```

  バッククォートで囲まれていない場合、エラーが返されます:

    ```SQL
    CREATE DATABASE LIKE;
    ERROR 1064 (HY000): Getting syntax error at line 1, column 16. Detail message: Unexpected input 'like', the most similar input is {a legal identifier}.
    ```

## Reserved keywords

以下は、StarRocks の予約キーワードをアルファベット順に並べたものです。識別子として使用したい場合は、バッククォート (`) で囲む必要があります。予約キーワードは StarRocks のバージョンによって異なる場合があります。

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