# 备份与恢复

本文介绍如何备份以及恢复 StarRocks 中的数据。

StarRocks 支持将数据以文件的形式，通过 Broker 备份到远端存储系统中。备份的数据可以从远端存储系统恢复至任意 StarRocks 集群。通过这个功能，您可以定期为 StarRocks 集群中的数据进行快照备份，或者将数据在不同集群间迁移。

Broker 是 StarRocks 集群中一种可选进程，主要用于读写 StarRocks 远端存储上的文件和目录。在使用备份与恢复功能前，您需要部署对应远端存储的 Broker。具体部署步骤，参考[部署 Broker](../quick_start/Deploy.md)。

> 注意：
>
> * 目前暂不支持备份与恢复使用主键模型的表。
> * 备份与恢复功能目前只允许拥有 ADMIN 权限的用户执行。
> * 一个 Database 内，只允许有一个正在执行的备份或恢复作业。
> * StarRocks 不支持备份恢复表之间的 Colocate Join 关系。

## 备份数据

当前 StarRocks 支持最小为分区（Partition）粒度的全量备份。当表的数据量很大时，建议按分区分别执行，以降低失败重试的代价。如果您需要对数据进行定期备份，首先需要在建表时，合理地规划表的分区及分桶（例如，按时间进行分区），从而在后期运维过程中，按照分区粒度进行定期的数据备份。

您可以通过查看各个分区的分片数量，以及各个分片的文件版本数量，来预估作业执行时间。

```sql
SHOW PARTITIONS FROM <table_name>;
SHOW TABLET FROM <table_name>; 
```

### 创建仓库

备份数据前，您需要在远端存储系统为数据创建仓库。详细使用方法参阅 [CREATE REPOSITORY](../sql-reference/sql-statements/data-definition/CREATE_REPOSITORY.md)。

```sql
CREATE REPOSITORY repo_name
WITH BROKER broker_name
ON LOCATION repo_location
PROPERTIES ("key"="value", ...);
```

> 说明：PROPERTIES 中需要填写相应远端仓库的登录信息。

您可以通过以下命令删除仓库。

```sql
DROP REPOSITORY <repo_name>;
```

> 注意：目前备份在远端存储系统中的快照数据无法通过 StarRocks 直接删除，用户需要手动删除备份在远端存储系统的快照路径。

### 备份数据

将数据备份至仓库。详细使用方法参阅 [BACKUP](../sql-reference/sql-statements/data-definition/BACKUP.md)。

```sql
BACKUP SNAPSHOT <snapshot_name>
TO repo_name
ON (
table_name [PARTITION (`p1`, ...)], ...
)
PROPERTIES ("key"="value", ...);
```

PROPERTIES：

* `type`：更新模式。默认为 `full`，即全量更新。
* `timeout`：任务超时时间，默认值为 `86400`，即一天。单位为秒。

数据备份为异步操作。您可以通过 [SHOW BACKUP](../sql-reference/sql-statements/data-manipulation/SHOW_BACKUP.md) 语句查看备份作业状态。

```sql
SHOW BACKUP;
```

返回如下：

* JobId：本次备份作业的 ID。
* SnapshotName：用户指定的本次备份作业的名称（Label）。
* DbName：备份作业对应的 Database。
* State：备份作业当前所在阶段：
  * PENDING：作业初始状态。
  * SNAPSHOTING：正在进行快照操作。
  * UPLOAD_SNAPSHOT：快照结束，准备上传。
  * UPLOADING：正在上传快照。
  * SAVE_META：正在本地生成元数据文件。
  * UPLOAD_INFO：上传元数据文件和本次备份作业的信息。
  * FINISHED：备份完成。
  * CANCELLED：备份失败或被取消。
* BackupObjs：本次备份涉及的表和分区的清单。
* CreateTime：作业创建时间。
* SnapshotFinishedTime：快照完成时间。
* UploadFinishedTime：快照上传完成时间。
* FinishedTime：本次作业完成时间。
* UnfinishedTasks：在 SNAPSHOTTING，UPLOADING 等阶段，会有多个子任务在同时进行，这里展示的当前段，未完成子任务的 Task ID。
* TaskErrMsg：如果有子任务执行出错，这里会显示对应子任务的错误信息。
* Status：用于记录在整个作业过程中，可能出现的一些状态信息。
* Timeout：作业的超时时间，单位为秒。

> 说明：只有当 `State` 为 `CANCELLED` 时说明作业被中止。返回 `TaskErrMsg` 并不一定会中止作业，当前作业仍会重试，直至成功或中止。

您可以通过 [CANCEL BACKUP](../sql-reference/sql-statements/data-definition/CANCEL_BACKUP.md) 语句取消备份作业。

```sql
CANCEL BACKUP FROM <db_name>;
```

## 恢复或迁移数据

您可以先将备份至远端仓库的数据恢复到当前或其他集群，完成数据恢复或迁移。

> 注意：
>
> * 因为数据备份是通过快照的形式完成的，所以在备份数据快照生成之后导入的数据不会被备份。因此，在快照生成至恢复（迁移）作业完成这段期间导入的数据，需要重新导入至集群。建议您在迁移完成后，对新旧两个集群并行导入一段时间，完成数据和业务正确性校验后，再将业务迁移到新的集群。
> * 如果恢复作业是一次覆盖操作（指定恢复数据到已经存在的表或分区中），那么从恢复作业的 COMMIT 阶段开始，当前集群上被覆盖的数据有可能不能再被还原。此时如果恢复作业失败或被取消，有可能造成之前的数据已损坏且无法访问。这种情况下，只能通过再次执行恢复操作，并等待作业完成。因此，我们建议您，如无必要，不要使用覆盖的方式恢复数据，除非确认当前数据已不再使用。覆盖操作会检查快照和已存在的表或分区的元数据是否相同，包括 schema 和 Rollup 等信息，如果不同则无法执行恢复操作。

### （可选）在新集群中创建仓库

如需将数据迁徙至其他 StarRocks 集群，您需要在新集群中使用相同**仓库名**和**地址**创建仓库，否则将无法查看先前备份的数据快照。详细信息见 [创建仓库](#创建仓库)。

### 查看备份

开始恢复或迁移前，您可以通过 [SHOW SNAPSHOT](../sql-reference/sql-statements/data-manipulation/SHOW_SNAPSHOT.md) 查看特定仓库对应的数据快照信息。

```sql
SHOW SNAPSHOT ON <repo_name>;
```

返回如下：

* Snapshot：备份时指定的该备份的名称（Label）。
* Timestamp：备份的时间戳。
* Status：该备份是否正常。

如果在命令后指定了 where 子句，则可以显示更详细的备份信息。

* Database：备份时对应的 Database。
* Details：展示了该备份完整的数据目录结构。

### 迁移数据

通过 [RESTORE](../sql-reference/sql-statements/data-definition/RESTORE.md) 语句将远端仓库中的数据快照恢复至当前或其他 StarRocks 集群以恢复或迁移数据。

```sql
RESTORE SNAPSHOT <db_name>.<snapshot_name>
FROM `repository_name`
ON (
`table_name` [PARTITION (`p1`, ...)] [AS `tbl_alias`],
...
)
PROPERTIES ("key"="value", ...);
```

PROPERTIES：

* `backup_timestamp`：恢复对应备份的时间版本，必填。您可以通过 `SHOW SNAPSHOT ON repo_name;` 命令获得该信息。
* `replication_num`：恢复的表或分区的副本数。默认为 `3`。若恢复已存在的表或分区，则副本数必须和已存在表或分区的副本数相同。同时，必须有足够的 host 容纳多个副本。
* `timeout`：任务超时时间，默认值为 `86400`，即一天。单位为秒。
* `meta_version`：使用指定的 `meta_version` 来读取之前备份的元数据。该参数作为临时方案，仅用于恢复老版本 StarRocks 备份的数据。最新版本的备份数据中已经包含 `meta_version`，无需指定。

数据恢复为异步操作。您可以通过 [SHOW RESTORE](../sql-reference/sql-statements/data-manipulation/SHOW_RESTORE.md) 语句查看恢复作业状态。

```sql
SHOW RESTORE;
```

返回如下：

* JobId：本次恢复作业的 ID。
* Label：用户指定的仓库中备份的名称（Label）。
* Timestamp：用户指定的仓库中备份的时间戳。
* DbName：恢复作业对应的 Database。
* State：恢复作业当前所在阶段：

  * PENDING：作业初始状态。
  * SNAPSHOTING：正在进行本地新建表的快照操作。
  * DOWNLOAD：正在发送下载快照任务。
  * DOWNLOADING：快照正在下载。
  * COMMIT：准备生效已下载的快照。
  * COMMITTING：正在生效已下载的快照。
  * FINISHED：恢复完成。
  * CANCELLED：恢复失败或被取消。

* AllowLoad：恢复期间是否允许导入。
* ReplicationNum：恢复指定的副本数。
* RestoreObjs：本次恢复涉及的表和分区的清单。
* CreateTime：作业创建时间。
* MetaPreparedTime：本地元数据生成完成时间。
* SnapshotFinishedTime：本地快照完成时间。
* DownloadFinishedTime：远端快照下载完成时间。
* FinishedTime：本次作业完成时间。
* UnfinishedTasks：在 SNAPSHOTTING，DOWNLOADING，COMMITTING 等阶段，会有多个子任务在同时进行，这里展示的当前阶段，未完成的子任务的 Task ID。
* TaskErrMsg：如果有子任务执行出错，这里会显示对应子任务的错误信息。
* Status：用于记录在整个作业过程中，可能出现的一些状态信息。
* Timeout：作业的超时时间，单位是秒。

您可以通过 [CANCEL RESTORE](../sql-reference/sql-statements/data-definition/CANCEL_RESTORE.md) 语句取消恢复作业。

```sql
CANCEL RESTORE FROM <db_name>;
```
