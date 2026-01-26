---
displayed_sidebar: docs
---

# ADMIN REPAIR

## 功能

该语句用于尝试修复指定的表或分区。

对于存算一体表，会尝试优先调度修复副本。

对于存算分离表，会在元数据或者数据文件丢失时，尝试回退到某个历史可用版本。(注意：可能会导致某些 Tablet 最新数据丢失)

:::tip

该操作需要 SYSTEM 级 OPERATE 权限。请参考 [GRANT](../../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```sql
ADMIN REPAIR TABLE table_name [PARTITION (p1,...)] [PROPERTIES ("key" = "value", ...)]
```

说明：

1. 该语句仅表示让系统尝试以高优先级修复指定表或分区的分片副本，并不保证能够修复成功。用户可以通过 `ADMIN SHOW REPLICA STATUS;` 命令查看修复情况。
2. 默认的 timeout 是 14400 秒(4 小时)。超时意味着系统将不再以高优先级修复指定表或分区的分片副本。需要重新使用该命令设置。
3. 支持通过 `PROPERTIES` 设置修复行为，目前仅支持存算分离表。

** PROPERTIES **

`enforce_consistent_version`：是否强制要求分区内所有 Tablet 回退到一致的版本。默认为 `true`。如果设置为 `true`，则系统会在分区内的历史版本中寻找对所有 Tablet 都有效的统一版本进行回退。保证分区数据版本对齐。如果设置为 `false`，允许分区内每个 Tablet 各自回退到其能找到的最新有效版本，不同 Tablet 的版本可能不一致，但能最大程度保留数据。
`allow_empty_tablet_recovery`：是否允许通过创建空 Tablet 的方式进行恢复。默认为 `false`。仅在 `enforce_consistent_version` 为 `false` 时生效。当部分 Tablet 所有版本的元数据都缺失，但仍有至少一个 Tablet 元数据有效时，如果该参数为 `true`，系统将尝试创建空的 Tablet 来填补缺失的版本。若所有 Tablet 的所有版本元数据都丢失，则无法恢复。

## 示例

1. 尝试修复指定表

    ```sql
    ADMIN REPAIR TABLE tbl1;
    ```

2. 尝试修复指定分区

    ```sql
    ADMIN REPAIR TABLE tbl1 PARTITION (p1, p2);
    ```

3. 尝试修复存算分离表，允许版本不一致，并允许创建空 Tablet 以恢复

    ```sql
    ADMIN REPAIR TABLE cloud_tbl PROPERTIES (
        "enforce_consistent_version" = "false",
        "allow_empty_tablet_recovery" = "true"
    );
    ```
