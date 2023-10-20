# Back up and restore data

This topic describes how to back up and restore data in StarRocks.

StarRocks supports backing up data as snapshots into a remote storage system, and restoring data from snapshots to any StarRocks clusters.

> **CAUTION**
>
> - StarRocks does not support backing up and restoring Primary Key model table.
> - Only users with the ADMIN privilege can back up or restore data.
> - In each database, only one running BACKUP or RESTORE job is allowed each time.

## Back up data

StarRocks supports full backup on granular level of table, or partition.

If you have stored a large quantity of data in a table, we recommend that you back up and restore data by partition to minimize the losses caused by possible job failures. If you need to back up data at a regular interval, you can strategize a partition plan (by a certain time interval, for example) for your table, and back up new partitions each time.

### Create a repository

Before backing up data, you need to create a repository to store data in a remote storage system. For detailed instructions, see [CREATE REPOSITORY](../sql-reference/sql-statements/data-definition/CREATE_REPOSITORY.md).

The following example creates a repository named `test_repo` in an Apache™ Hadoop® cluster.

```SQL
CREATE REPOSITORY test_repo
WITH BROKER  broker_name
ON LOCATION "hdfs://xxx.xx.xxx.xxxx:xxxx/data/sr_backup"
PROPERTIES("username" = "xxxx");
```

After restoring data, you can delete the repository using [DROP REPOSITORY](../sql-reference/sql-statements/data-definition/DROP_REPOSITORY.md).

> **CAUTION**
>
> Data snapshots backed up in the remote storage system cannot be deleted through StarRocks. You need to delete them manually in the remote storage system.

### Back up a data snapshot

Create a data snapshot and back up it in the remote repository. For detailed instructions, see [BACKUP](../sql-reference/sql-statements/data-definition/BACKUP.md).

The following example creates a data snapshot `sr_member_backup` for the table `sr_member` in `test_repo`.

```SQL
BACKUP SNAPSHOT sr_hub.sr_member_backup
TO test_repo
ON (sr_member);
```

BACKUP is an asynchronous operation. You can check the status of a BACKUP job status using [SHOW BACKUP](../sql-reference/sql-statements/data-manipulation/SHOW_BACKUP.md), or cancel a BACKUP job using [CANCEL BACKUP](../sql-reference/sql-statements/data-definition/CANCEL_BACKUP.md).

## Restore or migrate data

You can restore the data snapshot backed up in the remote storage system to the current or other StarRocks clusters to complete data restoration or migration.

> **CAUTION**
>
> - Because data are backed up as snapshots, the data loaded after the snapshot is generated is not included in the snapshot. Therefore, if any data is loaded into the old cluster during the period between snapshot generation and RESTORE job completion, you also need to load the data into the cluster that data is restored into. It is recommended that you load data into both clusters in parallel for a period of time after the data migration is complete, and then migrate your application to the new cluster after verifying the correctness of the data and services.
> - If the RESTORE job overwrites an existing database, table, or partition, the overwritten data cannot be restored after the job enters the COMMIT phase of the recovery job. If the RESTORE job fails or is canceled at this point, the data may be corrupted and inaccessible. In this case, you can only perform the RESTORE operation again and wait for the job to complete. Therefore, we recommend that you do not restore data by overwriting unless your are sure that the current data is no longer used. The overwrite operation will check metadata consistency between the snapshot and the existing database, table, or partition. If inconsistency is detected, the RESTORE operation cannot be performed.

### (Optional) Create the repository in the new cluster

To migrate data to another StarRocks cluster, you need to create a repository with the same **repository name** and **location** in the new cluster, otherwise you will not be able to view the previously backed up data snapshots. See [creating a repository](#create-a-repository) for details.

### Check the snapshot

Before restoring data, you can check the snapshots in a specified repository using [SHOW SNAPSHOT](../sql-reference/sql-statements/data-manipulation/SHOW_SNAPSHOT.md).

The following example checks the snapshot information in`test_repo`.

```Plain
mysql> SHOW SNAPSHOT ON test_repo;
+------------------+-------------------------+--------+
| Snapshot         | Timestamp               | Status |
+------------------+-------------------------+--------+
| sr_member_backup | 2022-11-21-10-42-26-315 | OK     |
+------------------+-------------------------+--------+
1 row in set (0.01 sec)
```

### Restore data via the snapshot

Restore data snapshots in the remote storage system to current or other StarRocks clusters to restore or migrate data using [RESTORE](../sql-reference/sql-statements/data-definition/RESTORE.md).

The following example restores the data snapshot `sr_member_backup` in `test_repo` on the table `sr_member`.

```SQL
RESTORE SNAPSHOT sr_hub.sr_member_backup
FROM test_repo
ON (sr_member)
PROPERTIES ("backup_timestamp"="2022-11-21-10-42-26-315");
```

RESTORE is an asynchronous operation. You can check the status of a RESTORE job using [SHOW RESTORE](../sql-reference/sql-statements/data-manipulation/SHOW_RESTORE.md), or cancel a RESTORE job using [CANCEL RESTORE](../sql-reference/sql-statements/data-definition/CANCEL_RESTORE.md).
