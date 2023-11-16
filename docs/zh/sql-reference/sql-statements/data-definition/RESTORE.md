---
displayed_sidebar: "Chinese"
---

# RESTORE

## 功能

恢复指定数据库、表或分区的数据。当前 StarRocks 仅支持恢复 OLAP 类型表。更多信息，请见 [备份和恢复](../../../administration/Backup_and_restore.md)。

数据恢复为异步操作。您可以通过 [SHOW RESTORE](../data-manipulation/SHOW_RESTORE.md) 语句查看恢复作业状态，或通过 [CANCEL RESTORE](../data-definition/CANCEL_RESTORE.md) 语句取消恢复作业。

> **注意**
>
> - 仅限拥有 ADMIN 权限的用户执行恢复功能。
> - 单一数据库内，仅可同时执行一个备份或恢复作业，否则系统报错。

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
| PARTITION       | 需要恢复的分区名。如不指定则恢复对应表的所有分区。您可以通过 [SHOW PARTITIONS](../data-manipulation/SHOW_PARTITIONS.md) 语句查看分区名。 |
| PROPERTIES      | 恢复操作属性。现支持以下属性：<ul><li>`backup_timestamp`：备份时间戳，**必填**。您可以通过 [SHOW SNAPSHOT](../data-manipulation/SHOW_SNAPSHOT.md) 查看备份时间戳。</li><li>`replication_num`：指定恢复的表或分区的副本数。默认：`3`。</li><li>`meta_version`：该参数作为临时方案，仅用于恢复旧版本 StarRocks 备份的数据。最新版本的备份数据中已经包含 `meta version`，无需再指定。</li><li>`timeout`：任务超时时间。单位：秒。默认：`86400`。</li></ul> |

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
