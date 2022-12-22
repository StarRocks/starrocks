# 备份与恢复

本文介绍如何备份以及恢复 StarRocks 中的数据。

StarRocks 支持将数据以快照文件的形式，通过 Broker 备份到远端存储系统中，或将备份的数据从远端存储系统恢复至任意 StarRocks 集群。通过这个功能，您可以定期为 StarRocks 集群中的数据进行快照备份，或者将数据在不同集群间迁移。

> **注意**
>
> - 仅限拥有 ADMIN 权限的用户执行备份与恢复功能。
> - 单一数据库内，仅可同时执行一个备份或恢复作业。

## 备份数据

StarRocks 支持以数据库、表、或分区为粒度全量备份数据。

当表的数据量很大时，建议您按分区分别执行，以降低失败重试的代价。如果您需要对数据进行定期备份，首先需要在建表时，合理地规划表的分区（例如，按时间进行分区），从而在后期运维过程中，按照分区粒度进行定期的数据备份。

### 创建仓库

备份数据前，您需要在远端存储系统为数据创建仓库。详细使用方法参阅 [CREATE REPOSITORY](../sql-reference/sql-statements/data-definition/CREATE%20REPOSITORY.md)。

以下示例在 Apache™ Hadoop® 集群中创建仓库 `test_repo`。

```SQL
CREATE REPOSITORY test_repo
WITH BROKER
ON LOCATION "hdfs://xxx.xx.xxx.xxxx:xxxx/data/sr_backup"
PROPERTIES("username" = "xxxx");
```

完成数据恢复后，您可以通过 [DROP REPOSITORY](../sql-reference/sql-statements/data-definition/DROP%20REPOSITORY.md) 语句删除仓库。

> **注意**
>
> 目前备份在远端存储系统中的快照数据无法通过 StarRocks 直接删除，您需要手动删除备份在远端存储系统的快照路径。

### 备份数据快照

创建数据快照并将其备份至远端仓库。详细使用方法参阅 [BACKUP](../sql-reference/sql-statements/data-definition/BACKUP.md)。

以下示例在仓库 `test_repo` 中为表 `sr_member` 创建数据快照 `sr_member_backup`。

```SQL
BACKUP SNAPSHOT sr_hub.sr_member_backup
TO test_repo
ON (sr_member);
```

数据备份为异步操作。您可以通过 [SHOW BACKUP](../sql-reference/sql-statements/data-manipulation/SHOW%20BACKUP.md) 语句查看备份作业状态，或通过 [CANCEL BACKUP](../sql-reference/sql-statements/data-definition/CANCEL%20BACKUP.md) 语句取消备份作业。

## 恢复或迁移数据

您可以将备份至远端仓库的数据快照恢复到当前或其他集群，完成数据恢复或迁移。

> **注意**
>
> - 因为数据备份是通过快照的形式完成的，所以在当前数据快照生成之后导入的数据不会被备份。因此，在快照生成至恢复（迁移）作业完成这段期间导入的数据，需要重新导入至集群。建议您在迁移完成后，对新旧两个集群并行导入一段时间，完成数据和业务正确性校验后，再将业务迁移到新的集群。
> - 如果恢复作业是一次覆盖操作（指定恢复数据到已经存在的表或分区中），那么从恢复作业的 COMMIT 阶段开始，当前集群上被覆盖的数据有可能不能再被还原。此时如果恢复作业失败或被取消，有可能造成之前的数据损坏且无法访问。这种情况下，只能通过再次执行恢复操作，并等待作业完成。因此，我们建议您，如无必要，不要使用覆盖的方式恢复数据，除非确认当前数据已不再使用。覆盖操作会检查快照和已存在的表或分区的元数据是否相同，包括 schema 和 Rollup 等信息，如果不同则无法执行恢复操作。

### （可选）在新集群中创建仓库

如需将数据迁徙至其他 StarRocks 集群，您需要在新集群中使用相同**仓库名**和**地址**创建仓库，否则将无法查看先前备份的数据快照。详细信息见 [创建仓库](#创建仓库)。

### 查看数据库快照

开始恢复或迁移前，您可以通过 [SHOW SNAPSHOT](../sql-reference/sql-statements/data-manipulation/SHOW%20SNAPSHOT.md) 查看特定仓库对应的数据快照信息。

以下示例查看仓库 `test_repo` 中的数据快照信息。

```Plain
mysql> SHOW SNAPSHOT ON test_repo;
+------------------+-------------------------+--------+
| Snapshot         | Timestamp               | Status |
+------------------+-------------------------+--------+
| sr_member_backup | 2022-11-21-10-42-26-315 | OK     |
+------------------+-------------------------+--------+
1 row in set (0.01 sec)
```

### 恢复数据快照

通过 [RESTORE](../sql-reference/sql-statements/data-definition/RESTORE.md) 语句将远端仓库中的数据快照恢复至当前或其他 StarRocks 集群以恢复或迁移数据。

以下示例将仓库 `test_repo` 中的数据快照 `sr_member_backup`恢复为表 `sr_member`。

```SQL
RESTORE SNAPSHOT sr_hub.sr_member_backup
FROM test_repo
ON (sr_member)
PROPERTIES ("backup_timestamp"="2022-11-21-10-42-26-315");
```

数据恢复为异步操作。您可以通过 [SHOW RESTORE](../sql-reference/sql-statements/data-manipulation/SHOW%20RESTORE.md) 语句查看恢复作业状态，或通过 [CANCEL RESTORE](../sql-reference/sql-statements/data-definition/CANCEL%20RESTORE.md) 语句取消恢复作业。
