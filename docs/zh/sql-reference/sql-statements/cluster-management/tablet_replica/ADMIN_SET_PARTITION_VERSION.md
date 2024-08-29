---
displayed_sidebar: docs
---

# ADMIN SET PARTITION VERSION

## 功能

手动设置分区的版本。

请注意，手动设置分区版本是一项危险操作，仅建议在集群元数据出现问题时才使用该方式。在正常情况下，分区的版本会自动与其内部的 Tablet 保持一致。

:::tip

该操作需要 SYSTEM 级 OPERATE 权限。请参考 [GRANT](../../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```sql
ADMIN SET TABLE <table_name> PARTITION ( <partition_name> | <partition_id> ) 
TO VERSION <version>
```

## 参数说明

- `table_name`: 分区所属表的名称。
- `partition_name`: 分区的名称。您需要使用 `partition_name` 或 `partition_id` 来指定分区。对于非分区表，`partition_name` 与表名相同。
- `partition_id`: 分区的 ID。您需要使用 `partition_name` 或 `partition_id` 来指定分区。对于采用随机分桶策略的表，您必须使用 `partition_id` 来指定分区。
- `version`: 要设置的分区版本。

## 示例

1. 将非分区表 `t1` 的版本设置为 `10`。

    ```sql
    ADMIN SET TABLE t1 PARTITION(t1) TO VERSION 10;
    ```

2. 将表 `t2` 中分区 `p1` 的版本设置为 `10`。

    ```sql
    ADMIN SET TABLE t2 PARTITION(p1) TO VERSION 10;
    ```

3. 将 ID 为 `123456` 的分区的版本设置为 `10`。`t3` 为采用随机分桶策略的表。

    ```sql
    ADMIN SET TABLE t3 PARTITION('123456') TO VERSION 10;
    ```
