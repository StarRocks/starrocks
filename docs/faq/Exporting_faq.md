# Data Export

## Alibaba cloud OSS backup and restore

StarRocks supports backing up data to alicloud OSS / AWS S3 (or object storage compatible with S3 protocol). Suppose there are two StarRocks clusters, namely DB1 cluster and DB2 cluster. We need to back up the data in DB1 to alicloud OSS and then restore it to DB2 when necessary. The general process of backup and recovery is as follows:

### Create a cloud repository

Execute SQL in DB1 and DB2 respectively:

```sql
CREATE REPOSITORY `repository name`
WITH BROKER `broker_name`
ON LOCATION "oss://bucket name/path"
PROPERTIES
(
"fs.oss.accessKeyId" = "xxx",
"fs.oss.accessKeySecret" = "yyy",
"fs.oss.endpoint" = "oss-cn-beijing.aliyuncs.com"
);
```

a. Both DB1 and DB2 need to be created, and the created REPOSITORY name should be the same. View the repository:

```sql
SHOW REPOSITORIES;
```

b. broker_ name needs to fill in the broker name in a cluster. View BrokerName:

```sql
SHOW BROKER;
```

c. The path after fs.oss.endpoint does not need to have a bucket name.

### Backup data table

BACKUP the tables to be backed up to the cloud repository in DB1. Execute SQL in DB1:

```sql
BACKUP SNAPSHOT [db_name].{snapshot_name}
TO `repository_name`
ON (
`table_name` [PARTITION (`p1`, ...)],
...
)
PROPERTIES ("key"="value", ...);
```

```plain text
PROPERTIES currently supports the following properties:
"type" = "full"：indicates that this is a full update (default).
"timeout" = "3600"：task timeout. The default is one day. The unit is seconds.
```

StarRocks does not support full database backup at present. We need to specify the tables or partitions to be backed up ON (...), and these tables or partitions will be backed up in parallel.

View the backup tasks in progress (note that only one backup task can be performed at the same time):

```sql
SHOW BACKUP FROM db_name;
```

After the backup is completed, you can check whether the backup data in the OSS already exists (unnecessary backups need to be deleted in the OSS):

```sql
SHOW SNAPSHOT ON OSS repository name; 
```

### Data restore

For data restore in DB2, there is no need to create a table structure to be restored in DB2. It will be created automatically during the Restore operation. Perform restore SQL:

```sql
RESTORE SNAPSHOT [db_name].{snapshot_name}
FROMrepository_name``
ON (
    'table_name' [PARTITION ('p1', ...)] [AS 'tbl_alias'],
    ...
)
PROPERTIES ("key"="value", ...);
```

View the restore progress:

```sql
SHOW RESTORE;
```
