---
displayed_sidebar: docs
---

# ADMIN REPAIR

## 功能

<<<<<<< HEAD
该语句用于尝试优先修复指定的表或分区。
=======
尝试修复指定的表或分区。

对于存算一体集群中的内表，此语句会尝试优先调度副本修复操作。

对于存算分离集群中的云原生表，当元数据或数据文件丢失时，它会尝试回滚到历史可用版本。请注意，**这可能会导致某些分区丢失最新数据**。
>>>>>>> 9d6586f1d4 ([Doc] Remove Problematic Links (#69829))

:::tip

该操作需要 SYSTEM 级 OPERATE 权限。请参考 [GRANT](../../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```sql
ADMIN REPAIR TABLE table_name[ PARTITION (p1,...)]
```

说明：

1. 该语句仅表示让系统尝试以高优先级修复指定表或分区的分片副本，并不保证能够修复成功。用户可以通过 `ADMIN SHOW REPLICA STATUS;` 命令查看修复情况。
2. 默认的 timeout 是 14400 秒(4 小时)。超时意味着系统将不再以高优先级修复指定表或分区的分片副本。需要重新使用该命令设置。

## 示例

1. 尝试修复指定表

    ```sql
    ADMIN REPAIR TABLE tbl1;
    ```

2. 尝试修复指定分区

    ```sql
    ADMIN REPAIR TABLE tbl1 PARTITION (p1, p2);
    ```
