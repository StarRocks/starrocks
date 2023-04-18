# DROP MATERIALIZED VIEW

## Description

Drops a materialized view. 

You cannot drop a synchronous materialized view that is being created in process with this command. To drop a synchronous materialized view that is being created in process, see [Synchronous materialized View - Drop an unfinished materialized view](../using_starrocks/Materialized_view.md#drop-an-unfinished-materialized-view) for further instructions.

> **CAUTION**
>
> Only users with the `DROP_PRIV` privilege in the database where the base table resides can drop a materialized view.

## Syntax

```SQL
DROP MATERIALIZED VIEW [IF EXISTS] [database.]mv_name
```

Parameters in brackets [] is optional.

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| IF EXISTS     | no           | If this parameter is specified, StarRocks will not throw an exception when deleting a materialized view that does not exist. If this parameter is not specified, the system will throw an exception when deleting a materialized view that does not exist. |
| mv_name       | yes          | The name of the materialized view to delete.                 |

## Examples

Example 1: Drop an existing materialized view

1. View all existing materialized views in the database.

  ```Plain
  MySQL > SHOW MATERIALIZED VIEWS\G
  *************************** 1. row ***************************
              id: 470740
          name: order_mv1
  database_name: default_cluster:sr_hub
        text: SELECT `sr_hub`.`orders`.`dt` AS `dt`, `sr_hub`.`orders`.`order_id` AS `order_id`, `sr_hub`.`orders`.`user_id` AS `user_id`, sum(`sr_hub`.`orders`.`cnt`) AS `total_cnt`, sum(`sr_hub`.`orders`.`revenue`) AS `total_revenue`, count(`sr_hub`.`orders`.`state`) AS `state_count` FROM `sr_hub`.`orders` GROUP BY `sr_hub`.`orders`.`dt`, `sr_hub`.`orders`.`order_id`, `sr_hub`.`orders`.`user_id`
          rows: 0
  1 rows in set (0.00 sec)
  ```

2. Drop the materialized view `order_mv1`.

  ```SQL
  DROP MATERIALIZED VIEW order_mv1;
  ```

3. Check if the dropped materialized view exists.

  ```Plain
  MySQL > SHOW MATERIALIZED VIEWS;
  Empty set (0.01 sec)
  ```

Example 2: Drop a non-existing materialized view

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
