# 备份与恢复

本文介绍如何备份以及恢复 StarRocks 中的数据，或将数据迁移至新的 StarRocks 集群。

StarRocks 支持将数据以快照文件的形式备份到远端存储系统中，或将备份的数据从远端存储系统恢复至任意 StarRocks 集群。通过这个功能，您可以定期为 StarRocks 集群中的数据进行快照备份，或者将数据在不同 StarRocks 集群间迁移。

StarRocks 支持在以下外部存储系统中备份数据：

- Apache™ Hadoop® （HDFS）集群
- AWS S3
- Google GCS
- 阿里云 OSS
- 腾讯云 COS

## 备份数据

StarRocks 支持以数据库、表、或分区为粒度全量备份数据。

当表的数据量很大时，建议您按分区分别执行，以降低失败重试的代价。如果您需要对数据进行定期备份，建议您在建表时制定[动态分区](../table_design/dynamic_partitioning.md)策略，从而可以在后期运维过程中，仅定期备份新增分区中的数据。

### 创建仓库

仓库用于在远端存储系统中存储备份文件。备份数据前，您需要基于远端存储系统路径在 StarRocks 中创建仓库。您可以在同一集群中创建多个仓库。详细使用方法参阅 [CREATE REPOSITORY](../sql-reference/sql-statements/data-definition/CREATE_REPOSITORY.md)。

- 在 HDFS 集群中创建仓库

以下示例在 Apache™ Hadoop® 集群中创建仓库 `test_repo`。

```SQL
CREATE REPOSITORY test_repo
WITH BROKER
ON LOCATION "hdfs://<hdfs_host>:<hdfs_port>/repo_dir/backup"
PROPERTIES(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
);
```

- 在 AWS S3 中创建仓库

  您可以选择使用 IAM 用户凭证（Access Key 和 Secret Key）、Instance Profile 或者 Assumed Role 作为访问 Amazon S3 的安全凭证。

  - 以下示例以 IAM 用户凭证作为安全凭证在 Amazon S3 存储桶 `bucket_s3` 中创建仓库 `test_repo`。

  ```SQL
  CREATE REPOSITORY test_repo
  WITH BROKER
  ON LOCATION "s3a://bucket_s3/backup"
  PROPERTIES(
      "aws.s3.access_key" = "XXXXXXXXXXXXXXXXX",
      "aws.s3.secret_key" = "yyyyyyyyyyyyyyyyyyyyyyyy",
      "aws.s3.endpoint" = "s3.us-east-1.amazonaws.com"
  );
  ```

  - 以下示例以 Instance Profile 作为安全凭证在 Amazon S3 存储桶 `bucket_s3` 中创建仓库 `test_repo`。

  ```SQL
  CREATE REPOSITORY test_repo
  WITH BROKER
  ON LOCATION "s3a://bucket_s3/backup"
  PROPERTIES(
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-east-1"
  );
  ```

  - 以下示例以 Assumed Role 作为安全凭证在 Amazon S3 存储桶 `bucket_s3` 中创建仓库 `test_repo`。

  ```SQL
  CREATE REPOSITORY test_repo
  WITH BROKER
  ON LOCATION "s3a://bucket_s3/backup"
  PROPERTIES(
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::xxxxxxxxxx:role/yyyyyyyy",
      "aws.s3.region" = "us-east-1"
  );
  ```

> **说明**
>
> StarRocks 仅支持通过 S3A 协议在 AWS S3 中创建仓库。因此，当您在 AWS S3 中创建仓库时，必须在 `ON LOCATION` 参数下将 S3 URI 中的 `s3://` 替换为 `s3a://`。

- 在 Google GCS 中创建仓库

以下示例在 Google GCS 存储桶 `bucket_gcs` 中创建仓库 `test_repo`。

```SQL
CREATE REPOSITORY test_repo
WITH BROKER
ON LOCATION "s3a://bucket_gcs/backup"
PROPERTIES(
    "fs.s3a.access.key" = "xxxxxxxxxxxxxxxxxxxx",
    "fs.s3a.secret.key" = "yyyyyyyyyyyyyyyyyyyy",
    "fs.s3a.endpoint" = "storage.googleapis.com"
);
```

> **说明**
>
> StarRocks 仅支持通过 S3A 协议在 Google GCS 中创建仓库。 因此，当您在 Google GCS 中创建仓库时，必须在 `ON LOCATION` 参数下将 GCS URI 的前缀替换为 `s3a://`。

- 在阿里云 OSS 中创建仓库

以下示例在阿里云 OSS 存储空间 `bucket_oss` 中创建仓库 `test_repo`。

```SQL
CREATE REPOSITORY test_repo
WITH BROKER
ON LOCATION "oss://bucket_oss/backup"
PROPERTIES(
    "fs.oss.accessKeyId" = "xxxxxxxxxxxxxxxxxxxxxxxxxx",
    "fs.oss.accessKeySecret" = "yyyyyyyyyyyyyyyyyyyy",
    "fs.oss.endpoint" = "oss-cn-zhangjiakou-internal.aliyuncs.com"
);
```

- 在腾讯云 COS 中创建仓库

以下示例在腾讯云 COS 存储空间 `bucket_cos` 中创建仓库 `test_repo`。

```SQL
CREATE REPOSITORY test_repo
WITH BROKER
ON LOCATION "cosn://bucket_cos/backup"
PROPERTIES(
    "fs.cosn.userinfo.secretId" = "xxxxxxxxxxxxxxxxx",
    "fs.cosn.userinfo.secretKey" = "yyyyyyyyyyyyyyyy",
    "fs.cosn.bucket.endpoint_suffix" = "cos.ap-beijing.myqcloud.com"
);
```

仓库创建完成后，您可以通过 [SHOW REPOSITORIES](../sql-reference/sql-statements/data-manipulation/SHOW_REPOSITORIES.md) 查看已创建的仓库。完成数据恢复后，您可以通过 [DROP REPOSITORY](../sql-reference/sql-statements/data-definition/DROP_REPOSITORY.md) 语句删除 StarRocks 中的仓库。但备份在远端存储系统中的快照数据目前无法通过 StarRocks 直接删除，您需要手动删除备份在远端存储系统的快照路径。

### 备份数据快照

创建数据仓库后，您可以通过 [BACKUP](../sql-reference/sql-statements/data-definition/BACKUP.md) 命令创建数据快照并将其备份至远端仓库。

以下示例在数据库 `sr_hub` 中为表 `sr_member` 创建数据快照 `sr_member_backup` 并备份至仓库 `test_repo` 中。

```SQL
BACKUP SNAPSHOT sr_hub.sr_member_backup
TO test_repo
ON (sr_member);
```

数据备份为异步操作。您可以通过 [SHOW BACKUP](../sql-reference/sql-statements/data-manipulation/SHOW_BACKUP.md) 语句查看备份作业状态，或通过 [CANCEL BACKUP](../sql-reference/sql-statements/data-definition/CANCEL_BACKUP.md) 语句取消备份作业。

## 恢复或迁移数据

您可以将备份至远端仓库的数据快照恢复到当前或其他 StarRocks 集群，完成数据恢复或迁移。

### （可选）在新集群中创建仓库

如需将数据迁移至其他 StarRocks 集群，您需要在新集群中使用相同**仓库名**和**地址**创建仓库，否则将无法查看先前备份的数据快照。详细信息见 [创建仓库](#创建仓库)。

### 查看数据库快照

开始恢复或迁移前，您可以通过 [SHOW SNAPSHOT](../sql-reference/sql-statements/data-manipulation/SHOW_SNAPSHOT.md) 查看特定仓库对应的数据快照信息。

以下示例查看仓库 `test_repo` 中的数据快照信息。

```Plain
mysql> SHOW SNAPSHOT ON test_repo;
+------------------+-------------------------+--------+
| Snapshot         | Timestamp               | Status |
+------------------+-------------------------+--------+
| sr_member_backup | 2023-02-07-14-45-53-143 | OK     |
+------------------+-------------------------+--------+
1 row in set (1.16 sec)
```

### 恢复数据快照

通过 [RESTORE](../sql-reference/sql-statements/data-definition/RESTORE.md) 语句将远端仓库中的数据快照恢复至当前或其他 StarRocks 集群以恢复或迁移数据。

以下示例将仓库 `test_repo` 中的数据快照 `sr_member_backup`恢复为表 `sr_member`，仅恢复一个数据副本。

```SQL
RESTORE SNAPSHOT sr_hub.sr_member_backup
FROM test_repo
ON (sr_member)
PROPERTIES (
    "backup_timestamp"="2023-02-07-14-45-53-143",
    "replication_num" = "1"
);
```

数据恢复为异步操作。您可以通过 [SHOW RESTORE](../sql-reference/sql-statements/data-manipulation/SHOW_RESTORE.md) 语句查看恢复作业状态，或通过 [CANCEL RESTORE](../sql-reference/sql-statements/data-definition/CANCEL_RESTORE.md) 语句取消恢复作业。

## 配置相关参数

您可以通过在 BE 配置文件 **be.conf** 中修改以下配置项加速备份或还原作业：

| 配置项                   | 说明                                                                             |
| ----------------------- | -------------------------------------------------------------------------------- |
| upload_worker_count     | BE 节点上传任务的最大线程数，用于备份作业。默认值：`1`。增加此配置项的值可以增加上传任务并行度。|
| download_worker_count   | BE 节点下载任务的最大线程数，用于还原作业。默认值：`1`。增加此配置项的值可以增加下载任务并行度。|
| max_download_speed_kbps | BE 节点下载速度上限。默认值：`50000`。单位：KB/s。通常还原作业的下载速度不会超过默认值。如果该速度上限限制了还原作业的性能，您可以根据带宽情况适当增加。|

## 注意事项

- 仅限拥有 ADMIN 权限的用户执行备份与恢复功能。
- 单一数据库内，仅可同时执行一个备份或恢复作业。否则，StarRocks 返回错误。
- 因为备份与恢复操作会占用一定系统资源，建议您在集群业务低峰期进行该操作。
- 目前 StarRocks 不支持在备份数据时使用压缩算法。
- 因为数据备份是通过快照的形式完成的，所以在当前数据快照生成之后导入的数据不会被备份。因此，在快照生成至恢复（迁移）作业完成这段期间导入的数据，需要重新导入至集群。建议您在迁移完成后，对新旧两个集群并行导入一段时间，完成数据和业务正确性校验后，再将业务迁移到新的集群。
- 在恢复作业完成前，被恢复表无法被操作。
- Primary Key 表无法被恢复至 v2.5 之前版本的 StarRocks 集群中。
- 您无需在恢复作业前在新集群中创建需要被恢复表。恢复作业将自动创建该表。
- 如果被恢复表与已有表重名，StarRocks 会首先识别已有表的 Schema。如果 Schema 相同，StarRocks 会覆盖写入已有表。如果 Schema 不同，恢复作业失败。您可以通过 `AS` 关键字重新命名被恢复表，或者删除已有表后重新发起恢复作业。
- 如果恢复作业是一次覆盖操作（指定恢复数据到已经存在的表或分区中），那么从恢复作业的 COMMIT 阶段开始，当前集群上被覆盖的数据有可能不能再被还原。此时如果恢复作业失败或被取消，有可能造成之前的数据损坏且无法访问。这种情况下，只能通过再次执行恢复操作，并等待作业完成。因此，我们建议您，如无必要，不要使用覆盖的方式恢复数据，除非确认当前数据已不再使用。覆盖操作会检查快照和已存在的表或分区的元数据是否相同，包括 Schema 和 Rollup 等信息，如果不同则无法执行恢复操作。
- 在备份或恢复作业中，StarRocks 自动备份或恢复表的 [同步物化视图](../using_starrocks/Materialized_view-single_table.md)。目前 StarRocks 暂不支持备份恢复视图和[异步物化视图](../using_starrocks/Materialized_view.md)。您只能手动备份异步物化视图所对应的实体表，且该实体表无法作为物化视图加速或改写查询。
- 目前 StarRocks 暂不支持备份恢复用户、权限以及资源组配置相关数据。
- StarRocks 不支持备份恢复表之间的 Colocate Join 关系。
