---
displayed_sidebar: docs
---

# 备份与恢复

本文介绍如何备份以及恢复 StarRocks 中的数据，或将数据迁移至新的 StarRocks 集群。

StarRocks 支持将数据以快照文件的形式备份到远端存储系统中，或将备份的数据从远端存储系统恢复至任意 StarRocks 集群。通过这个功能，您可以定期为 StarRocks 集群中的数据进行快照备份，或者将数据在不同 StarRocks 集群间迁移。

从 v3.4.0 开始，StarRocks 进一步增强了备份恢复功能，支持了更多对象，并且重构语法以提高灵活性。

StarRocks 支持在以下外部存储系统中备份数据：

- Apache™ Hadoop® （HDFS）集群
- AWS S3
- Google GCS
- 阿里云 OSS
- 腾讯云 COS
- 华为云 OBS
- MinIO

StarRocks 支持备份以下对象：

- 内部数据库、表（所有类型和分区策略）和分区
- External Catalog 的元数据（自 v3.4.0 开始支持）
- 同步物化视图和异步物化视图
- 逻辑视图（自 v3.4.0 开始支持）
- UDF（自 v3.4.0 开始支持）

> **说明**
>
> StarRocks 存算分离集群不支持数据备份和恢复。

## 创建仓库

仓库用于在远端存储系统中存储备份文件。备份数据前，您需要基于远端存储系统路径在 StarRocks 中创建仓库。您可以在同一集群中创建多个仓库。详细使用方法参阅 [CREATE REPOSITORY](../../sql-reference/sql-statements/backup_restore/CREATE_REPOSITORY.md)。

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
      "aws.s3.region" = "us-east-1"
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
> - StarRocks 仅支持通过 S3A 协议在 Google GCS 中创建仓库。 因此，当您在 Google GCS 中创建仓库时，必须在 `ON LOCATION` 参数下将 GCS URI 的前缀替换为 `s3a://`。
> - 请勿在 Endpoint 地址中指定 `https`。

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

- 在 MinIO 中创建仓库

以下示例在 MinIO 存储空间 `bucket_minio` 中创建仓库 `test_repo`。

```SQL
CREATE REPOSITORY test_repo
WITH BROKER
ON LOCATION "s3://bucket_minio/backup"
PROPERTIES(
    "aws.s3.access_key" = "XXXXXXXXXXXXXXXXX",
    "aws.s3.secret_key" = "yyyyyyyyyyyyyyyyy",
    "aws.s3.endpoint" = "http://minio:9000"
);
```

仓库创建完成后，您可以通过 [SHOW REPOSITORIES](../../sql-reference/sql-statements/backup_restore/SHOW_REPOSITORIES.md) 查看已创建的仓库。完成数据恢复后，您可以通过 [DROP REPOSITORY](../../sql-reference/sql-statements/backup_restore/DROP_REPOSITORY.md) 语句删除 StarRocks 中的仓库。但备份在远端存储系统中的快照数据目前无法通过 StarRocks 直接删除，您需要手动删除备份在远端存储系统的快照路径。

## 备份数据

创建数据仓库后，您可以通过 [BACKUP](../../sql-reference/sql-statements/backup_restore/BACKUP.md) 命令创建数据快照并将其备份至远端仓库。数据备份为异步操作。您可以通过 [SHOW BACKUP](../../sql-reference/sql-statements/backup_restore/SHOW_BACKUP.md) 语句查看备份作业状态，或通过 [CANCEL BACKUP](../../sql-reference/sql-statements/backup_restore/CANCEL_BACKUP.md) 语句取消备份作业。

StarRocks 支持以数据库、表、或分区为粒度全量备份数据。

当表的数据量很大时，建议您按分区分别执行，以降低失败重试的代价。如果您需要对数据进行定期备份，建议您在建表时制定[分区策略](../../table_design/data_distribution/Data_distribution.md#分区)策略，从而可以在后期运维过程中，仅定期备份新增分区中的数据。

### 备份数据库

对数据库执行完全备份将备份数据库中的所有表、物化视图、逻辑视图和 UDF。

以下示例为数据库 `sr_hub` 创建数据快照 `sr_member_backup` 并备份至仓库 `test_repo` 中。

```SQL
-- 自 v3.4.0 起支持。
BACKUP DATABASE sr_hub SNAPSHOT sr_hub_backup
TO test_repo;

-- 兼容先前版本语法。
BACKUP SNAPSHOT sr_hub.sr_hub_backup
TO test_repo;
```

### 备份表

StarRocks 支持备份和恢复所有类型和分区策略的表。对表进行完全备份会备份表中数据以及在其上建立的同步物化视图。

以下示例将数据库 `sr_hub` 中的表 `sr_member` 备份到快照 `sr_member_backup` 中，并将快照上传到仓库 `test_repo`。

```SQL
-- 自 v3.4.0 起支持。
BACKUP DATABASE sr_hub SNAPSHOT sr_member_backup
TO test_repo
ON (TABLE sr_member);

-- 兼容先前版本语法。
BACKUP SNAPSHOT sr_hub.sr_member_backup
TO test_repo
ON (sr_member);
```

以下示例将数据库 `sr_hub` 中的表 `sr_member` 和 `sr_pmc` 备份到快照 `sr_core_backup` 中，并将快照上传到仓库 `test_repo`。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_core_backup
TO test_repo
ON (TABLE sr_member, TABLE sr_pmc);
```

以下示例将数据库 `sr_hub` 中的所有表备份到快照 `sr_all_backup` 中，并将快照上传到仓库 `test_repo`。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_all_backup
TO test_repo
ON (ALL TABLES);
```

### 备份分区

以下示例将数据库 `sr_hub` 中的表 `sr_member` 的分区 `p1` 备份到快照 `sr_par_backup` 中，并将快照上传到仓库 `test_repo`。

```SQL
-- 自 v3.4.0 起支持。
BACKUP DATABASE sr_hub SNAPSHOT sr_par_backup
TO test_repo
ON (TABLE sr_member PARTITION (p1));

-- 兼容先前版本语法。
BACKUP SNAPSHOT sr_hub.sr_par_backup
TO test_repo
ON (sr_member PARTITION (p1));
```

您可以指定多个分区名称，用逗号 (`,`)分隔，批量备份分区。

### 备份物化视图

同步物化视图无需手动备份，备份基表时系统将同时备份其上建立的同步物化视图。

异步物化视图可通过备份其所属数据库一起备份，也可以通过手动备份。

以下示例将数据库 `sr_hub` 中的异步物化视图 `sr_mv1` 备份到快照 `sr_mv1_backup` 中，并将快照上传到仓库 `test_repo`。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_mv1_backup
TO test_repo
ON (MATERIALIZED VIEW sr_mv1);
```

以下示例将数据库 `sr_hub` 中的异步物化视图 `sr_mv1` 和 `sr_mv2` 备份到快照 `sr_mv2_backup` 中，并将快照上传到仓库 `test_repo`。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_mv2_backup
TO test_repo
ON (MATERIALIZED VIEW sr_mv1, MATERIALIZED VIEW sr_mv2);
```

以下示例将数据库 `sr_hub` 中的所有异步物化视图备份到快照 `sr_mv3_backup` 中，并将快照上传到仓库 `test_repo`。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_mv3_backup
TO test_repo
ON (ALL MATERIALIZED VIEWS);
```

### 备份逻辑视图

以下示例将数据库 `sr_hub` 中的逻辑视图 `sr_view1` 备份到快照 `sr_view1_backup` 中，并将快照上传到仓库 `test_repo`。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_view1_backup
TO test_repo
ON (VIEW sr_view1);
```

以下示例将数据库 `sr_hub` 中的逻辑视图 `sr_view1` 和 `sr_view2` 备份到快照 `sr_view2_backup` 中，并将快照上传到仓库 `test_repo`。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_view2_backup
TO test_repo
ON (VIEW sr_view1, VIEW sr_view2);
```

以下示例将数据库 `sr_hub` 中的所有逻辑视图备份到快照 `sr_view3_backup` 中，并将快照上传到仓库 `test_repo`。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_view3_backup
TO test_repo
ON (ALL VIEWS);
```

### 备份 UDF

以下示例将数据库 `sr_hub` 中的 UDF `sr_udf1` 备份到快照 `sr_udf1_backup` 中，并将快照上传到仓库 `test_repo`。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_udf1_backup
TO test_repo
ON (FUNCTION sr_udf1);
```

以下示例将数据库 `sr_hub` 中的 UDF `sr_udf1` 和 `sr_udf2` 备份到快照 `sr_udf2_backup` 中，并将快照上传到仓库 `test_repo`。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_udf2_backup
TO test_repo
ON (FUNCTION sr_udf1, FUNCTION sr_udf2);
```

以下示例将数据库 `sr_hub` 中的所有 UDF 备份到快照 `sr_udf3_backup` 中，并将快照上传到仓库 `test_repo`。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_udf3_backup
TO test_repo
ON (ALL FUNCTIONS);
```

### 备份 External Catalog 元数据

以下示例将 External Catalog `iceberg` 的元数据备份到快照 `iceberg_backup` 中，并将快照上传到仓库 `test_repo`。

```SQL
BACKUP EXTERNAL CATALOG (iceberg) SNAPSHOT iceberg_backup
TO test_repo;
```

以下示例将 External Catalog `iceberg` 和 `hive` 的元数据备份到快照 `iceberg_hive_backup` 中，并将快照上传到仓库 `test_repo`。

```SQL
BACKUP EXTERNAL CATALOGS (iceberg, hive) SNAPSHOT iceberg_hive_backup
TO test_repo;
```
以下示例将所有 External Catalog 的元数据备份到快照 `all_catalog_backup` 中，并将快照上传到仓库 `test_repo`。

```SQL
BACKUP ALL EXTERNAL CATALOGS SNAPSHOT all_catalog_backup
TO test_repo;
```

要取消 External Catalog 的 BACKUP 操作，请执行以下语句：

```SQL
CANCEL BACKUP FOR EXTERNAL CATALOG;
```

## 恢复数据

您可以将备份至远端仓库的数据快照恢复到当前或其他 StarRocks 集群，完成数据恢复或迁移。

**从快照还原数据时，必须指定快照的时间戳。**

通过 [RESTORE](../../sql-reference/sql-statements/backup_restore/RESTORE.md) 语句将远端仓库中的数据快照恢复至当前或其他 StarRocks 集群以恢复或迁移数据。

数据恢复为异步操作。您可以通过 [SHOW RESTORE](../../sql-reference/sql-statements/backup_restore/SHOW_RESTORE.md) 语句查看恢复作业状态，或通过 [CANCEL RESTORE](../../sql-reference/sql-statements/backup_restore/CANCEL_RESTORE.md) 语句取消恢复作业。

### （可选）在新集群中创建仓库

如需将数据迁移至其他 StarRocks 集群，您需要在新集群中使用相同**仓库名**和**地址**创建仓库，否则将无法查看先前备份的数据快照。详细信息见 [创建仓库](#创建仓库)。

### 获取快照时间戳

开始恢复或迁移前，您可以通过 [SHOW SNAPSHOT](../../sql-reference/sql-statements/backup_restore/SHOW_SNAPSHOT.md) 查看特定仓库获取对应数据快照的时间戳。

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

### 恢复数据库

以下示例将快照 `sr_hub_backup` 中的数据库 `sr_hub` 还原到目标群集中的数据库 `sr_hub`。如果快照中不存在该数据库，系统将返回错误。如果目标群集中不存在该数据库，系统将自动创建该数据库。

```SQL
-- 自 v3.4.0 起支持。
RESTORE SNAPSHOT sr_hub_backup
FROM test_repo
DATABASE sr_hub
PROPERTIES("backup_timestamp" = "2024-12-09-10-25-58-842");

-- 兼容先前版本语法。
RESTORE SNAPSHOT sr_hub.sr_hub_backup
FROM `test_repo` 
PROPERTIES("backup_timestamp" = "2024-12-09-10-25-58-842");
```

以下示例将快照 `sr_hub_backup` 中的数据库 `sr_hub` 还原到目标群集中的数据库 `sr_hub_new` 中。如果快照中不存在数据库 `sr_hub`，系统将返回错误。如果目标群集中不存在数据库 `sr_hub_new`，系统将自动创建该数据库。

```SQL
-- 自 v3.4.0 起支持。
RESTORE SNAPSHOT sr_hub_backup
FROM test_repo
DATABASE sr_hub AS sr_hub_new
PROPERTIES("backup_timestamp" = "2024-12-09-10-25-58-842");
```

### 恢复表

以下示例将快照 `sr_member_backup` 中数据库 `sr_hub` 下的表 `sr_member` 恢复到目标群集中数据库 `sr_hub` 下的表 `sr_member`。

```SQL
-- 自 v3.4.0 起支持。
RESTORE SNAPSHOT sr_member_backup
FROM test_repo 
DATABASE sr_hub 
ON (TABLE sr_member) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- 兼容先前版本语法。
RESTORE SNAPSHOT sr_hub.sr_member_backup
FROM test_repo
ON (sr_member)
PROPERTIES ("backup_timestamp"="2024-12-09-10-52-10-940");
```

以下示例将快照 `sr_member_backup` 中数据库 `sr_hub` 下的表 `sr_member` 恢复到目标群集中数据库 `sr_hub_new` 下的表 `sr_member_new`。

```SQL
RESTORE SNAPSHOT sr_member_backup
FROM test_repo 
DATABASE sr_hub  AS sr_hub_new
ON (TABLE sr_member AS sr_member_new) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

以下示例将快照 `sr_core_backup` 中数据库 `sr_hub` 下的两张表 `sr_member` 和 `sr_pmc` 恢复到目标群集中数据库 `sr_hub` 下的 `sr_member` 和 `sr_pmc` 表中。

```SQL
RESTORE SNAPSHOT sr_core_backup
FROM test_repo 
DATABASE sr_hub
ON (TABLE sr_member, TABLE sr_pmc) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

以下示例将快照 `sr_core_backup` 中数据库 `sr_hub` 下所有的表恢复到目标群集中数据库 `sr_hub` 中。

```SQL
RESTORE SNAPSHOT sr_all_backup
FROM test_repo
DATABASE sr_hub
ON (ALL TABLES);
```

以下示例将快照 `sr_core_backup` 中数据库 `sr_hub` 下其中一张表恢复到目标群集中数据库 `sr_hub` 中。

```SQL
RESTORE SNAPSHOT sr_all_backup
FROM test_repo
DATABASE sr_hub
ON (TABLE sr_member) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

### 恢复分区

以下示例将快照 `sr_par_backup` 中数据库 `sr_hub` 下的表 `sr_member` 的分区 `p1` 恢复到目标群集中数据库 `sr_hub` 下的表 `sr_member` 的分区 `p1` 中。

```SQL
-- 自 v3.4.0 起支持。
RESTORE SNAPSHOT sr_par_backup
FROM test_repo
DATABASE sr_hub
ON (TABLE sr_member PARTITION (p1)) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- 兼容先前版本语法。
RESTORE SNAPSHOT sr_hub.sr_par_backup
FROM test_repo
ON (sr_member PARTITION (p1)) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

您可以指定多个分区名称，用逗号 (`,`)分隔，以批量恢复分区。

### 恢复物化视图

以下示例将快照 `sr_mv1_backup` 中数据库 `sr_hub` 下的物化视图 `sr_mv1` 还原到目标群集。

```SQL
RESTORE SNAPSHOT sr_mv1_backup
FROM test_repo
DATABASE sr_hub
ON (MATERIALIZED VIEW sr_mv1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

以下示例将快照 `sr_mv2_backup` 中数据库 `sr_hub` 下的物化视图 `sr_mv1` 和 `sr_mv2` 还原到目标群集。

```SQL
RESTORE SNAPSHOT sr_mv2_backup
FROM test_repo
DATABASE sr_hub
ON (MATERIALIZED VIEW sr_mv1, MATERIALIZED VIEW sr_mv2) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

以下示例将快照 `sr_mv3_backup` 中数据库 `sr_hub` 下的所有物化视图还原到目标群集。

```SQL
RESTORE SNAPSHOT sr_mv3_backup
FROM test_repo
DATABASE sr_hub
ON (ALL MATERIALIZED VIEWS) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

以下示例将快照 `sr_mv3_backup` 中数据库 `sr_hub` 下的其中一张物化视图还原到目标群集。

```SQL
RESTORE SNAPSHOT sr_mv3_backup
FROM test_repo
DATABASE sr_hub
ON (MATERIALIZED VIEW sr_mv1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

:::info

还原后，您可以使用[SHOW MATERIALIZED VIEWS](../../sql-reference/sql-statements/materialized_view/SHOW_MATERIALIZED_VIEW.md) 检查物化视图的状态。

- 如果物化视图处于 Active 状态，则可以直接使用。
- 如果物化视图处于 Inactive 状态，可能是因为其基表尚未还原。在还原所有基表后，您可以使用[ALTER MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/ALTER_MATERIALIZED_VIEW.md) 重新激活物化视图。

:::

### 恢复逻辑视图

以下示例将快照 `sr_view1_backup` 中数据库 `sr_hub` 的逻辑视图 `sr_view1` 还原到目标群集。

```SQL
RESTORE SNAPSHOT sr_view1_backup
FROM test_repo
DATABASE sr_hub
ON (VIEW sr_view1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

以下示例将快照 `sr_view2_backup` 中数据库 `sr_hub` 的逻辑视图 `sr_view1` 和  `sr_view2` 还原到目标群集。

```SQL
RESTORE SNAPSHOT sr_view2_backup
FROM test_repo
DATABASE sr_hub
ON (VIEW sr_view1, VIEW sr_view2) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

以下示例将快照 `sr_view3_backup` 中数据库 `sr_hub` 的所有逻辑视图还原到目标群集。

```SQL
RESTORE SNAPSHOT sr_view3_backup
FROM test_repo
DATABASE sr_hub
ON (ALL VIEWS) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

以下示例将快照 `sr_view3_backup` 中数据库 `sr_hub` 的其中一张逻辑视图还原到目标群集。

```SQL
RESTORE SNAPSHOT sr_view3_backup
FROM test_repo
DATABASE sr_hub
ON (VIEW sr_view1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

### 恢复 UDF

以下示例将快照 `sr_udf1_backup` 中数据库 `sr_hub` 的 UDF `sr_udf1` 还原到目标群集。

```SQL
RESTORE SNAPSHOT sr_udf1_backup
FROM test_repo
DATABASE sr_hub
ON (FUNCTION sr_udf1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

以下示例将快照 `sr_udf2_backup` 中数据库 `sr_hub` 的 UDF `sr_udf1` 和 `sr_udf2` 还原到目标群集。

```SQL
RESTORE SNAPSHOT sr_udf2_backup
FROM test_repo
DATABASE sr_hub
ON (FUNCTION sr_udf1, FUNCTION sr_udf2) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

以下示例将快照 `sr_udf3_backup` 中数据库 `sr_hub` 的所有 UDF 还原到目标群集。

```SQL
RESTORE SNAPSHOT sr_udf3_backup
FROM test_repo
DATABASE sr_hub
ON (ALL FUNCTIONS) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

以下示例将快照 `sr_udf3_backup` 中数据库 `sr_hub` 的其中一个 UDF 还原到目标群集。

```SQL
RESTORE SNAPSHOT sr_udf3_backup
FROM test_repo
DATABASE sr_hub
ON (FUNCTION sr_udf1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

### 恢复 External Catalog 元数据

下面的示例将快照 `iceberg_backup` 中外 External Catalog `iceberg` 的元数据恢复到目标群集，并将其重命名为 `iceberg_new`。

```SQL
RESTORE SNAPSHOT iceberg_backup
FROM test_repo
EXTERNAL CATALOG (iceberg AS iceberg_new) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

下面的示例将快照 `iceberg_hive_backup` 中 External Catalog `iceberg` 和 `hive` 的元数据恢复到目标群集。

```SQL
RESTORE SNAPSHOT iceberg_hive_backup
FROM test_repo 
EXTERNAL CATALOGS (iceberg, hive)
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

下面的示例将快照 `all_catalog_backup` 中所有 External Catalog 的元数据恢复到目标群集。

```SQL
RESTORE SNAPSHOT all_catalog_backup
FROM test_repo 
ALL EXTERNAL CATALOGS
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

要取消 External Catalog 的 RESTORE 操作，请执行以下语句：

```SQL
CANCEL RESTORE FOR EXTERNAL CATALOG;
```

## 配置相关参数

您可以通过在 BE 配置文件 **be.conf** 中修改以下配置项加速备份或还原作业：

| 配置项                         | 说明                                                                              |
| ----------------------------- | -------------------------------------------------------------------------------- |
| make_snapshot_worker_count    | BE 节点快照任务的最大线程数，用于备份作业。默认值：`5`。增加此配置项的值可以增加快照任务并行度。|
| release_snapshot_worker_count | BE 节点释放快照任务的最大线程数，用于备份作业异常清理。默认值：`5`。增加此配置项的值可以增加释放快照任务并行度。|
| upload_worker_count           | BE 节点上传任务的最大线程数，用于备份作业。默认值：`0`。`0` 表示设置线程数为 BE 所在机器的 CPU 核数。增加此配置项的值可以增加上传任务并行度。|
| download_worker_count         | BE 节点下载任务的最大线程数，用于恢复作业。默认值：`0`。`0` 表示设置线程数为 BE 所在机器的 CPU 核数。增加此配置项的值可以增加下载任务并行度。|

## 注意事项

- 执行全局、数据库级、表级以及分区级备份恢复需要不同权限。详细内容，请参考 [基于使用场景创建自定义角色](../user_privs/User_privilege.md#基于使用场景创建自定义角色)。
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
