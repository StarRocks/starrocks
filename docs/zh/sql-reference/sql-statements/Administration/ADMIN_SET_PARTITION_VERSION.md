---
displayed_sidebar: "Chinese"
---

# ADMIN SET PARTITION VERSION

## 功能

手动设置分区的版本，该操作比较危险，在集群元数据出问题的时候才会用到，正常情况下分区的版本会自动与tablet的版本保持一致。

:::tip

该操作需要 SYSTEM 级 OPERATE 权限。请参考 [GRANT](../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```sql
ADMIN SET TABLE table_name PARTITION (partition_name | partition_id) TO VERSION xxx;
```

说明：

1. 对于非分区表，partition_name和表名相同。
2. 对于random distribution的表，需要用partition_id来指定分区。

## 示例

1. 将表t1的版本设置成10，t1是非分区表。

    ```sql
    ADMIN SET TABLE t1 PARTITION(t1) TO VERSION 10;
    ```

2. 将分区表t2的p1分区版本设置成10

    ```sql
    ADMIN SET TABLE t2 PARTITION(p1) TO VERSION 10;
    ```

3. 将分区表t3的id为123456的分区版本设置成10，t3是random distribution表。

    ```sql
    ADMIN SET TABLE t3 PARTITION('123456') TO VERSION 10;
    ```
