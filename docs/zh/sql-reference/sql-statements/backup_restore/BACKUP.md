---
displayed_sidebar: docs
keywords: ['beifen']
---

# BACKUP

StarRocks 支持备份及恢复以下对象：

- 内部数据库、表（所有类型和分区策略）和分区
- External Catalog 的元数据（自 v3.4.0 开始支持）
- 同步物化视图和异步物化视图
- 逻辑视图（自 v3.4.0 开始支持）
- UDF（自 v3.4.0 开始支持）

:::tip
有关备份和恢复的概述，请参阅 [备份和恢复指南](../../../administration/management/Backup_and_restore.md) 中。
:::

数据备份为异步操作。您可以通过 [SHOW BACKUP](./SHOW_BACKUP.md) 语句查看备份作业状态，或通过 [CANCEL BACKUP](./CANCEL_BACKUP.md) 语句取消备份作业。作业成功后，您可以通过 [SHOW SNAPSHOT](./SHOW_SNAPSHOT.md) 查看特定仓库对应的数据快照信息。

> **注意**
>
> - StarRocks 存算分离集群不支持数据备份和恢复。
> - 单一数据库内，仅可同时执行一个备份或恢复作业，否则系统报错。
> - 目前 StarRocks 不支持在备份数据时使用压缩算法。

## 权限要求

3.0 之前的版本中，拥有 admin_priv 权限才可执行此操作。3.0 及之后的版本中，如需备份指定数据表或整个数据库，需要拥有 System 级的 REPOSITORY 权限，以及对应表或对应数据库下所有表的 EXPORT 权限。例如：

- 授予角色从指定的表中导出数据的权限。

    ```SQL
    GRANT REPOSITORY ON SYSTEM TO ROLE backup_tbl;
    GRANT EXPORT ON TABLE <table_name> TO ROLE backup_tbl;
    ```

- 授予角色从指定数据下所有表中导出数据的权限。

    ```SQL
    GRANT REPOSITORY ON SYSTEM TO ROLE backup_db;
    GRANT EXPORT ON ALL TABLES IN DATABASE <database_name> TO ROLE backup_db;
    ```

- 授予角色从所有数据库的所有表中导出数据的权限。

    ```SQL
    GRANT REPOSITORY ON SYSTEM TO ROLE backup;
    GRANT EXPORT ON ALL TABLES IN ALL DATABASES TO ROLE backup;
    ```

## 语法（兼容先前版本）

```SQL
BACKUP SNAPSHOT <db_name>.<snapshot_name>
TO <repository_name>
[ ON ( <table_name> [ PARTITION ( <partition_name> [, ...] ) ]
       [, ...] ) ]
[ PROPERTIES ("key"="value" [, ...] ) ]
```

### 参数说明

| **参数**        | **说明**                                                     |
| --------------- | ------------------------------------------------------------ |
| db_name         | 需要备份的数据所属的数据库名。                                   |
| snapshot_name   | 指定数据快照名。全局范围内，快照名不可重复。                      |
| repository_name | 仓库名。您可以通过 [CREATE REPOSITORY](./CREATE_REPOSITORY.md) 创建仓库。 |
| ON              | 需要备份的表名。如不指定则备份整个数据库。                         |
| PARTITION       | 需要备份的分区名。如不指定则备份对应表的所有分区。                   |
| PROPERTIES      | 数据快照属性。现支持以下属性：<ul><li>`type`：备份类型。当前仅支持 `FULL`，即全量备份。默认：`FULL`。</li><li>`timeout`：任务超时时间。单位：秒。默认：`86400`。</li></ul> |

## 语法（自 v3.4.0 起支持）

```SQL
-- 备份 External Catalog 元数据。
BACKUP { ALL EXTERNAL CATALOGS | EXTERNAL CATALOG[S] (<catalog_name> [, ...]) }
[ DATABASE <db_name> ] SNAPSHOT [<db_name>.]<snapshot_name>
TO <repository_name>
[ PROPERTIES ("key"="value" [, ...] ) ]

-- 备份数据库、表、分区、物化视图、逻辑视图或 UDF。

BACKUP [ DATABASE <db_name> ] SNAPSHOT [<db_name>.]<snapshot_name>
TO <repository_name>
[ ON ( backup_object [, ...] )] 
[ PROPERTIES ("key"="value" [, ...] ) ]

backup_object ::= [
    { ALL TABLE[S]             | TABLE[S] <table_name>[, TABLE[S] <table_name> ...] } |
    { ALL MATERIALIZED VIEW[S] | MATERIALIZED VIEW[S] <mv_name>[, MATERIALIZED VIEW[S] <mv_name> ...] } |
    { ALL VIEW[S]              | VIEW[S] <view_name>[, VIEW[S] <view_name> ...] } |
    { ALL FUNCTION[S]          | FUNCTION[S] <udf_name>[, FUNCTION[S] <udf_name> ...] } |
     <table_name> PARTITION (<partition_name>[, ...]) ]
```

### 参数说明

| **参数**         | **说明**                                                     |
| --------------- | ------------------------------------------------------------ |
| ALL EXTERNAL CATALOGS | 备份所有 External Catalog 的元数据。                      |
| catalog_name    | 待备份的 External Catalog 名称。                               |
| DATABASE db_name | 待备份的对象或数据快照所属数据库的名称。只能指定 `DATABASE <db_name>` 或 `<db_name>.` 其中之一。 |
| db_name.        | 待备份的对象或数据快照所属数据库的名称。只能指定 `DATABASE <db_name>` 或 `<db_name>.` 其中之一。 |
| snapshot_name   | 指定数据快照名。全局范围内，快照名不可重复。                        |
| repository_name | 仓库名。您可以通过 [CREATE REPOSITORY](./CREATE_REPOSITORY.md) 创建仓库。 |
| ON              | 待备份的对象。如不指定则备份整个数据库。                           |
| table_name      | 待备份的表的名称。                                             |
| mv_name         | 待备份的物化视图的名称。                                        |
| view_name       | 待备份的逻辑视图的名称。                                        |
| udf_name        | 待备份的 UDF 的名称。                                          |
| PARTITION       | 待备份的分区名。如不指定则备份对应表的所有分区。                    |
| PROPERTIES      | 数据快照属性。现支持以下属性：<ul><li>`type`：备份类型。当前仅支持 `FULL`，即全量备份。默认：`FULL`。</li><li>`timeout`：任务超时时间。单位：秒。默认：`86400`。</li></ul> |

## 示例

### 兼容先前版本语法

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

### 自 v3.4.0 起支持语法

示例一：备份数据库。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_hub_backup TO test_repo;
```

示例二：备份数据库中的表。

```SQL
-- 备份一张表。
BACKUP DATABASE sr_hub SNAPSHOT sr_member_backup
TO test_repo
ON (TABLE sr_member);

-- 备份多张表。
BACKUP DATABASE sr_hub SNAPSHOT sr_core_backup
TO test_repo
ON (TABLE sr_member, TABLE sr_pmc);

-- 备份所有表。
BACKUP DATABASE sr_hub SNAPSHOT sr_all_backup
TO test_repo
ON (ALL TABLES);
```

示例三：备份表的分区。

```SQL
-- 备份一个分区。
BACKUP DATABASE sr_hub SNAPSHOT sr_par_backup
TO test_repo
ON (TABLE sr_member PARTITION (p1));

-- 备份多个分区。
BACKUP DATABASE sr_hub SNAPSHOT sr_par_backup
TO test_repo
ON (TABLE sr_member PARTITION (p1,p2,p3));
```

示例四：备份数据库中的物化视图。

```SQL
-- 备份一张物化视图。
BACKUP DATABASE sr_hub SNAPSHOT sr_mv1_backup
TO test_repo
ON (MATERIALIZED VIEW sr_mv1);

-- 备份多张物化视图。
BACKUP DATABASE sr_hub SNAPSHOT sr_mv2_backup
TO test_repo
ON (MATERIALIZED VIEW sr_mv1, MATERIALIZED VIEW sr_mv2);

-- 备份所有物化视图。
BACKUP DATABASE sr_hub SNAPSHOT sr_mv3_backup
TO test_repo
ON (ALL MATERIALIZED VIEWS);
```

示例五：备份数据库中的逻辑视图。

```SQL
-- 备份一张逻辑视图。
BACKUP DATABASE sr_hub SNAPSHOT sr_view1_backup
TO test_repo
ON (VIEW sr_view1);

-- 备份多张逻辑视图。
BACKUP DATABASE sr_hub SNAPSHOT sr_view2_backup
TO test_repo
ON (VIEW sr_view1, VIEW sr_view2);

-- 备份所有逻辑视图。
BACKUP DATABASE sr_hub SNAPSHOT sr_view3_backup
TO test_repo
ON (ALL VIEWS);
```

示例六：备份数据库中的 UDF。

```SQL
-- 备份一个 UDF。
BACKUP DATABASE sr_hub SNAPSHOT sr_udf1_backup
TO test_repo
ON (FUNCTION sr_udf1);

-- 备份多个 UDF。
BACKUP DATABASE sr_hub SNAPSHOT sr_udf2_backup
TO test_repo
ON (FUNCTION sr_udf1, FUNCTION sr_udf2);

-- 备份所有 UDF。
BACKUP DATABASE sr_hub SNAPSHOT sr_udf3_backup
TO test_repo
ON (ALL FUNCTIONS);
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
- 目前 StarRocks 暂不支持备份恢复用户、权限以及资源组配置相关数据。
- StarRocks 不支持备份恢复表之间的 Colocate Join 关系。
