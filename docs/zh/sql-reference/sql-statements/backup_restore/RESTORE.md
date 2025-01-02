---
displayed_sidebar: docs
keywords: ['beifen']
---

# RESTORE

恢复指定数据库、表或分区的数据。当前 StarRocks 仅支持恢复 OLAP 类型表。更多信息，请见 备份和恢复。

:::tip
有关备份和恢复的概述，请参阅 [备份和恢复指南](../../../administration/management/Backup_and_restore.md) 中。
:::

数据恢复为异步操作。您可以通过 [SHOW RESTORE](./SHOW_RESTORE.md) 语句查看恢复作业状态，或通过 [CANCEL RESTORE](./CANCEL_RESTORE.md) 语句取消恢复作业。

> **注意**
>
> 单一数据库内，仅可同时执行一个备份或恢复作业，否则系统报错。

## 权限要求

3.0 之前的版本中，拥有 admin_priv 权限才可执行此操作。3.0 及之后的版本中，如需恢复整个数据库，需要拥有 System 级的 REPOSITORY 权限，以及创建数据库、创建表、导入数据的权限；如需恢复特定表，则需要拥有 System 级的 REPOSITORY 权限，以及对特定表的导入权限（INSERT）。例如：

- 授予角色恢复指定表中数据的权限。

    ```SQL
    GRANT REPOSITORY ON SYSTEM TO ROLE recover_tbl;
    GRANT INSERT ON TABLE <table_name> TO ROLE recover_tbl;
    ```

- 授予角色恢复指定数据库下所有数据的权限。

    ```SQL
    GRANT REPOSITORY ON SYSTEM TO ROLE recover_db;
    GRANT INSERT ON ALL TABLES ALL DATABASES TO ROLE recover_db;
    ```

- 授予角色恢复 default_catalog 下全部数据库中数据的权限。

    ```SQL
    GRANT REPOSITORY ON SYSTEM TO ROLE recover_db;
    GRANT CREATE DATABASE ON CATALOG default_catalog TO ROLE recover_db;
    ```

## 语法

```SQL
RESTORE SNAPSHOT <db_name>.<snapshot_name>
FROM <repository_name>
[ ON ( <table_name> [ PARTITION ( <partition_name> [, ...] ) ]
    [ AS <table_alias>] [, ...] ) ]
PROPERTIES ("key"="value", ...)
```

## 参数说明

| **参数**        | **说明**                                                     |
| --------------- | ------------------------------------------------------------ |
| db_name         | 恢复数据至该数据库。                                           |
| snapshot_name   | 数据快照名。                                                 |
| repository_name | 仓库名。                                                    |
| ON              | 需要恢复的表名。如不指定则恢复整个数据库。                   |
| PARTITION       | 需要恢复的分区名。如不指定则恢复对应表的所有分区。您可以通过 [SHOW PARTITIONS](../table_bucket_part_index/SHOW_PARTITIONS.md) 语句查看分区名。 |
| PROPERTIES      | 恢复操作属性。现支持以下属性：<ul><li>`backup_timestamp`：备份时间戳，**必填**。您可以通过 [SHOW SNAPSHOT](./SHOW_SNAPSHOT.md) 查看备份时间戳。</li><li>`replication_num`：指定恢复的表或分区的副本数。默认：`3`。</li><li>`meta_version`：该参数作为临时方案，仅用于恢复旧版本 StarRocks 备份的数据。最新版本的备份数据中已经包含 `meta version`，无需再指定。</li><li>`timeout`：任务超时时间。单位：秒。默认：`86400`。</li></ul> |

## 示例

示例一：从 `example_repo` 仓库中恢复备份 `snapshot_label1` 中的表 `backup_tbl` 至数据库 `example_db`，备份时间戳为 `2018-05-04-16-45-08`。恢复为一个副本。

```SQL
RESTORE SNAPSHOT example_db.snapshot_label1
FROM example_repo
ON ( backup_tbl )
PROPERTIES
(
    "backup_timestamp"="2018-05-04-16-45-08",
    "replication_num" = "1"
);
```

示例二：从 `example_repo` 仓库中恢复备份 `snapshot_label2` 中的表 `backup_tbl` 的分区 `p1` 及 `p2`，以及表 `backup_tbl2` 到数据库 `example_db`，并重命名为 `new_tbl`，备份时间戳为 `2018-05-04-17-11-01`。默认恢复三个副本。

```SQL
RESTORE SNAPSHOT example_db.snapshot_label2
FROM example_repo
ON(
    backup_tbl PARTITION (p1, p2),
    backup_tbl2 AS new_tbl
)
PROPERTIES
(
    "backup_timestamp"="2018-05-04-17-11-01"
);
```

## 注意事项

- 执行全局、数据库级、表级以及分区级备份恢复需要不同权限。
- 单一数据库内，仅可同时执行一个备份或恢复作业。否则，StarRocks 返回错误。
- 因为备份与恢复操作会占用一定系统资源，建议您在集群业务低峰期进行该操作。
- 目前 StarRocks 不支持在备份数据时使用压缩算法。
- 因为数据备份是通过快照的形式完成的，所以在当前数据快照生成之后导入的数据不会被备份。因此，在快照生成至恢复（迁移）作业完成这段期间导入的数据，需要重新导入至集群。建议您在迁移完成后，对新旧两个集群并行导入一段时间，完成数据和业务正确性校验后，再将业务迁移到新的集群。
- 在恢复作业完成前，被恢复表无法被操作。
- Primary Key 表无法被恢复至 v2.5 之前版本的 StarRocks 集群中。
- 您无需在恢复作业前在新集群中创建需要被恢复表。恢复作业将自动创建该表。
- 如果被恢复表与已有表重名，StarRocks 会首先识别已有表的 Schema。如果 Schema 相同，StarRocks 会覆盖写入已有表。如果 Schema 不同，恢复作业失败。您可以通过 `AS` 关键字重新命名被恢复表，或者删除已有表后重新发起恢复作业。
- 如果恢复作业是一次覆盖操作（指定恢复数据到已经存在的表或分区中），那么从恢复作业的 COMMIT 阶段开始，当前集群上被覆盖的数据有可能不能再被还原。此时如果恢复作业失败或被取消，有可能造成之前的数据损坏且无法访问。这种情况下，只能通过再次执行恢复操作，并等待作业完成。因此，我们建议您，如无必要，不要使用覆盖的方式恢复数据，除非确认当前数据已不再使用。覆盖操作会检查快照和已存在的表或分区的元数据是否相同，包括 Schema 和 Rollup 等信息，如果不同则无法执行恢复操作。
- 目前 StarRocks 暂不支持备份恢复逻辑视图。
- 目前 StarRocks 暂不支持备份恢复用户、权限以及资源组配置相关数据。
- StarRocks 不支持备份恢复表之间的 Colocate Join 关系。
