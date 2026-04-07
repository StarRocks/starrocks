---
displayed_sidebar: docs
---

# ADMIN REPAIR

## 功能

尝试修复指定的表或分区。

对于存算一体集群中的内表，此语句会尝试优先调度副本修复操作。

对于存算分离集群中的云原生表，当元数据或数据文件丢失时，它会尝试回滚到历史可用版本。请注意，**这可能会导致某些分区丢失最新数据**。

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
3. 您可以通过在语句中指定 `PROPERTIES` 来设置修复行为。**目前仅存算分离集群中的云原生表支持 `PROPERTIES`**。

**PROPERTIES**

- `enforce_consistent_version`：是否强制分区内所有分片回滚至一致版本。默认值：`true`。若设置为`true`，系统将在历史中搜索对所有分片有效的版本进行回滚，确保分区内数据版本一致。若设置为`false`，则允许分区内每个分片回滚至其最新可用有效版本。不同分片版本可能不一致，但此方式可最大化数据保留率。
- `allow_empty_tablet_recovery`：是否允许通过创建空分片进行恢复。默认值：`false`。该项仅在 `enforce_consistent_version` 为 `false` 时生效。若设置为 `true`，当某些分片的全部版本元数据缺失但至少存在一个分片的有效元数据时，系统将尝试创建空分片来填补缺失版本。若所有分片的所有版本元数据均丢失，则无法进行恢复。
- `dry_run`：是否仅返回修复计划而不实际执行修复。默认值：`false`。若设置为 `true`，系统将评估每个分区的可修复性并返回修复计划，但不会执行任何实际的元数据回滚操作。返回结果包含以下列：
  - `PartitionId`：分区 ID。
  - `VisibleVersion`：当前可见版本。
  - `RepairStatus`：修复状态，取值包括：`NORMAL`（所有分片正常，无需修复）、`RECOVERABLE`（存在丢失的元数据或数据文件，但可以修复）、`UNRECOVERABLE`（存在丢失的元数据或数据文件，无法修复）、`UNKNOWN`（探测过程中发生异常）。
  - `TabletRecoverInfo`：JSON 数组，列出每个分片将回滚到的版本号（仅在状态为 `RECOVERABLE` 时有值）。
  - `ErrorMsg`：错误信息（仅在状态为 `UNRECOVERABLE` 或 `UNKNOWN` 时有值）。

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

4. 对存算分离表执行 Dry Run，预览修复计划而不实际执行

    ```sql
    ADMIN REPAIR TABLE cloud_tbl PARTITION (p1) PROPERTIES ("dry_run" = "true");
    ```
