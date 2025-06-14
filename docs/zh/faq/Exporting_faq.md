---
displayed_sidebar: docs
---

# 导出

## 阿里云OSS备份与还原

StarRocks支持备份数据到阿里云OSS/AWS S3（或者兼容S3协议的对象存储）等。假设有两个StarRocks集群，分别DB1集群和DB2集群，我们需要将DB1中的数据备份到阿里云OSS，然后在需要的时候恢复到DB2，备份及恢复大致流程如下：

### 创建云端仓库

在DB1和DB2中分别执行SQL：

```sql
CREATE REPOSITORY `仓库名称`
WITH BROKER `broker_name`
ON LOCATION "oss://存储桶名称/路径"
PROPERTIES
(
"fs.oss.accessKeyId" = "xxx",
"fs.oss.accessKeySecret" = "yyy",
"fs.oss.endpoint" = "oss-cn-beijing.aliyuncs.com"
);
```

a. DB1和DB2都需要创建，且创建的REPOSITORY仓库名称要相同，仓库查看：

```sql
SHOW REPOSITORIES;
```

b. broker_name需要填写一个集群中的broker名称，BrokerName查看：

```sql
SHOW BROKER;
```

c. fs.oss.endpoint后的路径不需要带存储桶名。

### 备份数据表

在DB1中将需要进行备份的表，BACKUP到云端仓库。在DB1中执行SQL：

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
PROPERTIES 目前支持以下属性：
"type" = "full"：表示这是一次全量更新（默认）。
"timeout" = "3600"：任务超时时间，默认为一天，单位秒。
```

StarRocks目前不支持全数据库的备份，我们需要在ON (……)指定需要备份的表或分区，这些表或分区将并行的进行备份。
查看正在进行中的备份任务（注意同时进行的备份任务只能有一个）：

```sql
SHOW BACKUP FROM db_name;
```

备份完成后，可以查看OSS中备份数据是否已经存在（不需要的备份需在OSS中删除）：

```sql
SHOW SNAPSHOT ON OSS仓库名; 
```

### 数据还原

在DB2中进行数据还原，DB2中不需要创建需恢复的表结构，在进行Restore操作过程中会自动创建。执行还原SQL：

```sql
RESTORE SNAPSHOT [db_name].{snapshot_name}
FROMrepository_name``
ON (
    'table_name' [PARTITION ('p1', ...)] [AS 'tbl_alias'],
    ...
)
PROPERTIES ("key"="value", ...);
```

查看还原进度：

```sql
SHOW RESTORE;
```
