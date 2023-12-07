---
displayed_sidebar: "Chinese"
---

# BACKUP

## 功能

备份指定数据库、表或分区的数据。当前 StarRocks 仅支持备份 OLAP 类型表。更多信息，请见 [备份和恢复](../../../administration/Backup_and_restore.md)。

数据备份为异步操作。您可以通过 [SHOW BACKUP](../data-manipulation/SHOW_BACKUP.md) 语句查看备份作业状态，或通过 [CANCEL BACKUP](../data-definition/CANCEL_BACKUP.md) 语句取消备份作业。作业成功后，您可以通过 [SHOW SNAPSHOT](../data-manipulation/SHOW_SNAPSHOT.md) 查看特定仓库对应的数据快照信息。

> **注意**
>
> - 仅限拥有 ADMIN 权限的用户执行备份功能。
> - 单一数据库内，仅可同时执行一个备份或恢复作业，否则系统报错。
> - 目前 StarRocks 不支持在备份数据时使用压缩算法。

## 语法

```SQL
BACKUP SNAPSHOT <db_name>.<snapshot_name>
TO <repository_name>
[ ON ( <table_name> [ PARTITION ( <partition_name> [, ...] ) ]
       [, ...] ) ]
[ PROPERTIES ("key"="value" [, ...] ) ]
```

## 参数说明

| **参数**        | **说明**                                                     |
| --------------- | ------------------------------------------------------------ |
| db_name         | 需要备份的数据所属的数据库名。                                   |
| snapshot_name   | 指定数据快照名。全局范围内，快照名不可重复。                      |
| repository_name | 仓库名。您可以通过 [CREATE REPOSITORY](../data-definition/CREATE_REPOSITORY.md) 创建仓库。 |
| ON              | 需要备份的表名。如不指定则备份整个数据库。                         |
| PARTITION       | 需要备份的分区名。如不指定则备份对应表的所有分区。                   |
| PROPERTIES      | 数据快照属性。现支持以下属性：`type`：备份类型。当前仅支持 `FULL`，即全量备份。默认：`FULL`。`timeout`：任务超时时间。单位：秒。默认：`86400`。 |

## 示例

示例一：全量备份 `example_db` 数据库到仓库 `example_repo` 中。

```SQL
BACKUP SNAPSHOT example_db.snapshot_label1
TO example_repo
PROPERTIES ("type" = "full");
```

示例二：全量备份 `example_db` 数据库下的表 `example_tbl` 到仓库 `example_repo` 中。

```SQL
BACKUP SNAPSHOT example_db.snapshot_label2
TO example_repo
ON (example_tbl);
```

示例三：全量备份 `example_db` 数据库中表 `example_tbl` 的 `p1`、`p2` 分区和表 `example_tbl2` 到仓库 `example_repo` 中。

```SQL
BACKUP SNAPSHOT example_db.snapshot_label3
TO example_repo
ON(
    example_tbl PARTITION (p1, p2),
    example_tbl2
);
```
