# DROP MATERIALIZED VIEW

## 功能

删除物化视图。

此命令无法用于删除正在创建中的同步物化视图。如要删除创建中的同步物化视图，请参考 [同步物化视图 - 删除物化视图](../../../using_starrocks/Materialized_view-single_table.md#删除同步物化视图)。

> **注意**
>
> 只有拥有对应物化视图 DROP 权限的用户才可以删除物化视图。

## 语法

```SQL
DROP MATERIALIZED VIEW [IF EXISTS] [database.]mv_name
```

## 参数

| **参数**  | **必选** | **说明**                                                     |
| --------- | -------- | ------------------------------------------------------------ |
| IF EXISTS | 否       | 如果声明该参数，删除不存在的物化视图系统不会报错。如果不声明该参数，删除不存在的物化视图系统会报错。 |
| mv_name   | 是       | 待删除的物化视图的名称。                                     |

## 示例

示例一：删除存在的物化视图

1. 查看当前数据库中存在的物化视图。

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

2. 删除物化视图 `order_mv1`。

    ```Plain
    DROP MATERIALIZED VIEW order_mv1;
    ```

3. 删除后重新查看当前数据库中存在的物化视图将不会显示该物化视图。

    ```Plain
    MySQL > SHOW MATERIALIZED VIEWS;
    Empty set (0.01 sec)
    ```

示例二：删除不存在的物化视图

- 当未声明 `IF EXISTS` 参数时，删除一个不属于当前数据库的物化视图 `k1_k2` 会报错。

```Plain
MySQL > DROP MATERIALIZED VIEW k1_k2;
ERROR 1064 (HY000): Materialized view k1_k2 is not find
```

- 当声明 `IF EXISTS` 参数时，删除一个不属于当前数据库的物化视图 `k1_k2` 不会报错。

```Plain
MySQL > DROP MATERIALIZED VIEW IF EXISTS k1_k2;
Query OK, 0 rows affected (0.00 sec)
```
