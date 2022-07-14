# DROP MATERIALIZED VIEW

## description

This statement is used to delete a materialized view. Synchronization syntax.

Syntax:

```sql
DROP MATERIALIZED VIEW [IF EXISTS] mv_name ON table_name
```

1. IF EXISTS
If the materialized view does not exist, don't throw an error. If this keyword is not declared, an error will be reported if the materialized view does not exist.

2. mv_name
The name of the materialized view to be deleted. Required.

3. table_name
The name of the table to which the materialized view to be deleted belongs. Required.

## example

The structure of the table is as follows:

```Plain Text
mysql> desc all_type_table all;
+----------------+-------+----------+------+-------+---------+-------+
| IndexName      | Field | Type     | Null | Key   | Default | Extra |
+----------------+-------+----------+------+-------+---------+-------+
| all_type_table | k1    | TINYINT  | Yes  | true  | N/A     |       |
|                | k2    | SMALLINT | Yes  | false | N/A     | NONE  |
|                | k3    | INT      | Yes  | false | N/A     | NONE  |
|                | k4    | BIGINT   | Yes  | false | N/A     | NONE  |
|                | k5    | LARGEINT | Yes  | false | N/A     | NONE  |
|                | k6    | FLOAT    | Yes  | false | N/A     | NONE  |
|                | k7    | DOUBLE   | Yes  | false | N/A     | NONE  |
|                |       |          |      |       |         |       |
| k1_sumk2       | k1    | TINYINT  | Yes  | true  | N/A     |       |
|                | k2    | SMALLINT | Yes  | false | N/A     | SUM   |
+----------------+-------+----------+------+-------+---------+-------+
```

1. Drop the materialized view named k1_sumk2 onthe table all_type_table.

    ```sql
    drop materialized view k1_sumk2 on all_type_table;
    ```

    The structure of table whose materialized view is deleted is as follows.

    ```plain text
    +----------------+-------+----------+------+-------+---------+-------+
    | IndexName      | Field | Type     | Null | Key   | Default | Extra |
    +----------------+-------+----------+------+-------+---------+-------+
    | all_type_table | k1    | TINYINT  | Yes  | true  | N/A     |       |
    |                | k2    | SMALLINT | Yes  | false | N/A     | NONE  |
    |                | k3    | INT      | Yes  | false | N/A     | NONE  |
    |                | k4    | BIGINT   | Yes  | false | N/A     | NONE  |
    |                | k5    | LARGEINT | Yes  | false | N/A     | NONE  |
    |                | k6    | FLOAT    | Yes  | false | N/A     | NONE  |
    |                | k7    | DOUBLE   | Yes  | false | N/A     | NONE  |
    +----------------+-------+----------+------+-------+---------+-------+
    ```

2. Drop a non-existing materialized view on the table all_type_table.

    ```sql
    drop materialized view k1_k2 on all_type_table;
    ERROR 1064 (HY000): errCode = 2, detailMessage = Materialized view [k1_k2] does not exist in table [all_type_table]
    ```

    The delete request reports an error.

3. Drop the materialized view k1_k2 on the table all_type_table. No error is reported if  Materialized view does not exist.

    ```sql
    drop materialized view if exists k1_k2 on all_type_table;
    Query OK, 0 rows affected (0.00 sec)
    ```

    If it exists, it will be dropped; If it does not exist, no error will be reported.

## keyword

DROP, MATERIALIZED, VIEW
