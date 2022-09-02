# DROP MATERIALIZED VIEW

## Description

Drops a materialized view. You cannot drop a materialized view that is being created in process with this command. To drop a materialized view that is being created in process, see [Materialized View](../using_starrocks/Materialized_view.md#drop-an-unfinished-materialized-view) for further instructions.

> **CAUTION**
>
> Only users with the `DROP_PRIV` privilege in the database where the base table resides can drop a materialized view.

<<<<<<< HEAD
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
=======
## Syntax

```SQL
DROP MATERIALIZED VIEW [IF EXISTS] [database.]mv_name;
>>>>>>> bc23ed1f2 (2.4 Materialized View (#10768))
```

Parameters in brackets [] is optional.

## Parameters

<<<<<<< HEAD
    ```sql
    drop materialized view k1_sumk2 on all_type_table;
    ```
=======
| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| IF EXISTS     | no           | If this parameter is specified, StarRocks will not throw an exception when deleting a materialized view that does not exist. If this parameter is not specified, the system will throw an exception when deleting a materialized view that does not exist. |
| mv_name       | yes          | The name of the materialized view to delete.                 |
>>>>>>> bc23ed1f2 (2.4 Materialized View (#10768))

## Example

### Example 1: Drop an existing materialized view

1. View all existing materialized views in the database.

<<<<<<< HEAD
    ```sql
    drop materialized view k1_k2 on all_type_table;
    ERROR 1064 (HY000): errCode = 2, detailMessage = Materialized view [k1_k2] does not exist in table [all_type_table]
    ```
=======
  ```Plain
  MySQL > SHOW MATERIALIZED VIEW\G
  *************************** 1. row ***************************
              id: 470740
          name: order_mv1
  database_name: default_cluster:sr_hub
        text: SELECT `sr_hub`.`orders`.`dt` AS `dt`, `sr_hub`.`orders`.`order_id` AS `order_id`, `sr_hub`.`orders`.`user_id` AS `user_id`, sum(`sr_hub`.`orders`.`cnt`) AS `total_cnt`, sum(`sr_hub`.`orders`.`revenue`) AS `total_revenue`, count(`sr_hub`.`orders`.`state`) AS `state_count` FROM `sr_hub`.`orders` GROUP BY `sr_hub`.`orders`.`dt`, `sr_hub`.`orders`.`order_id`, `sr_hub`.`orders`.`user_id`
          rows: 0
  1 rows in set (0.00 sec)
  ```
>>>>>>> bc23ed1f2 (2.4 Materialized View (#10768))

2. Drop the materialized view `order_mv1`.

  ```SQL
  DROP MATERIALIZED VIEW order_mv1;
  ```

3. Check if the dropped materialized view exists.

  ```Plain
  MySQL > SHOW MATERIALIZED VIEW;
  Empty set (0.01 sec)
  ```

### Example 2: Drop a non-existing materialized view

- If the parameter `IF EXISTS` is specified, StarRocks will not throw an exception when deleting a materialized view that does not exist.

```Plain
MySQL > DROP MATERIALIZED VIEW IF EXISTS k1_k2;
Query OK, 0 rows affected (0.00 sec)
```

- If the parameter `IF EXISTS` is not specified, the system will throw an exception when deleting a materialized view that does not exist.

```Plain
MySQL > DROP MATERIALIZED VIEW k1_k2;
ERROR 1064 (HY000): Materialized view k1_k2 is not find
```
