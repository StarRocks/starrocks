# EXPORT

## 功能

该语句用于将指定表的数据导出到指定位置。

这是一个异步操作，任务提交成功后返回结果。执行后可使用 [SHOW EXPORT](../../../sql-reference/sql-statements/data-manipulation/SHOW_EXPORT.md) 命令查看进度。

> **注意**
>
> 导出操作需要目标表的 EXPORT 权限。如果您的用户账号没有 EXPORT 权限，请参考 [GRANT](../account-management/GRANT.md) 给用户赋权。

## 语法

```sql
EXPORT TABLE <table_name>
[PARTITION (<partition_name>[, ...])]
[(<column_name>[, ...])]
TO <export_path>
[opt_properties]
WITH BROKER
[broker_properties]
```

## 参数说明

- `table_name`

  待导出数据所在的表。目前支持导出 `engine` 为 `olap` 或 `mysql` 的表。

- `partition_name`

  要导出的分区。如不指定则默认导出表中所有分区的数据。

- `column_name`

  要导出的列。列的导出顺序可以和源表结构 (Schema) 不同。如不指定则默认导出表中所有列的数据。

- `export_path`

  导出的目标路径。如果是目录，需要以斜杠 (/) 结尾。否则最后一个斜杠后面的部分会作为导出文件的前缀。如不指定文件名前缀，则默认文件名前缀为 `data_`。

- `opt_properties`

  导出相关的属性配置。

  语法：

  ```sql
  [PROPERTIES ("<key>"="<value>", ...)]
  ```

  配置项：

  | **配置项**         | **描述**                                                     |
  | ---------------- | ------------------------------------------------------------ |
  | column_separator | 指定导出文件的列分隔符。默认值：`\t`。                       |
  | line_delimiter   | 指定导出文件的行分隔符。默认值：`\n`。                       |
  | load_mem_limit   | 指定导出任务在单个 BE 节点上的内存使用上限。单位：字节。默认内存使用上限为 2 GB。 |
  | timeout          | 指定导出任务的超时时间。单位：秒。默认值：`86400`（1 天）。  |
  | include_query_id | 指定导出文件名中是否包含 `query_id`。取值范围：`true` 和 `false`。默认值：`true`。`true` 表示包含，`false` 表示不包含。 |

- `WITH BROKER`

  在 v2.4 及以前版本，您需要在导出语句中通过 `WITH BROKER "<broker_name>"` 来指定使用哪个 Broker。自 v2.5 起，您不再需要指定 `broker_name`，但继续保留 `WITH BROKER` 关键字。参见[从 使用 EXPORT 导出数据 > 背景信息](../../../unloading/Export.md#背景信息)。

- `broker_properties`

  用于提供访问数据源的鉴权信息。数据源不同，需要提供的鉴权信息也不同，参考 [BROKER LOAD](../../../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)。

## 示例

### 将表中所有数据导出到 HDFS

将 `testTbl` 表中的所有数据导出到 HDFS 集群的指定路径下。

```sql
EXPORT TABLE testTbl 
TO "hdfs://<hdfs_host>:<hdfs_port>/a/b/c/" 
WITH BROKER
(
    "username"="xxx",
    "password"="yyy"
);
```

### 将表中部分分区的数据导出到 HDFS

将 `testTbl` 表中 `p1` 和 `p2` 分区的数据导出到 HDFS 集群的指定路径下。

```sql
EXPORT TABLE testTbl
PARTITION (p1,p2) 
TO "hdfs://<hdfs_host>:<hdfs_port>/a/b/c/" 
WITH BROKER
(
    "username"="xxx",
    "password"="yyy"
);
```

### 指定分隔符

将 `testTbl` 表中的所有数据导出到 HDFS 集群的指定路径下，以 `,` 作为导出文件的列分隔符。

```sql
EXPORT TABLE testTbl 
TO "hdfs://<hdfs_host>:<hdfs_port>/a/b/c/" 
PROPERTIES
(
    "column_separator"=","
) 
WITH BROKER
(
    "username"="xxx",
    "password"="yyy"
);
```

将 `testTbl` 表中的所有数据导出到 HDFS 集群的指定路径下，以 Hive 默认分隔符 `\x01` 作为导出文件的列分隔符。

```sql
EXPORT TABLE testTbl 
TO "hdfs://<hdfs_host>:<hdfs_port>/a/b/c/" 
PROPERTIES
(
    "column_separator"="\\x01"
) 
WITH BROKER;
```

### 指定导出文件名前缀

将 `testTbl` 表中的所有数据导出到 HDFS 集群的指定路径下，以 `testTbl_` 作为导出文件的文件名前缀。

```sql
EXPORT TABLE testTbl 
TO "hdfs://<hdfs_host>:<hdfs_port>/a/b/c/testTbl_" 
WITH BROKER;
```

### 导出数据到 Alibaba Cloud OSS

将 `testTbl` 表中的所有数据导出到 Alibaba Cloud OSS 存储桶的指定路径下。

```sql
EXPORT TABLE testTbl 
TO "oss://oss-package/export/"
WITH BROKER
(
    "fs.oss.accessKeyId" = "xxx",
    "fs.oss.accessKeySecret" = "yyy",
    "fs.oss.endpoint" = "oss-cn-zhangjiakou-internal.aliyuncs.com"
);
```

### 导出数据到 Tencent Cloud COS

将 `testTbl` 表中的所有数据导出到 Tencent Cloud COS 存储桶的指定路径下。

```sql
EXPORT TABLE testTbl 
TO "cosn://cos-package/export/"
WITH BROKER
(
    "fs.cosn.userinfo.secretId" = "xxx",
    "fs.cosn.userinfo.secretKey" = "yyy",
    "fs.cosn.bucket.endpoint_suffix" = "cos.ap-beijing.myqcloud.com"
);
```

### 导出数据到 AWS S3

将 `testTbl` 表中的所有数据导出到 AWS S3 存储桶的指定路径下。

```sql
EXPORT TABLE testTbl 
TO "s3a://s3-package/export/"
WITH BROKER
(
    "aws.s3.access_key" = "xxx",
    "aws.s3.secret_key" = "yyy",
    "aws.s3.region" = "zzz"
);
```

### 导出数据到 Huawei Cloud OBS 上

将 `testTbl` 表中的所有数据导出到 Huawei Cloud OBS 存储桶的指定路径下。

```sql
EXPORT TABLE testTbl 
TO "obs://obs-package/export/"
WITH BROKER
(
    "fs.obs.access.key" = "xxx",
    "fs.obs.secret.key" = "yyy",
    "fs.obs.endpoint" = "obs.cn-east-3.myhuaweicloud.com"
);
```

> **说明**
>
> 导出数据至华为云 OBS 时，需要先下载[依赖库](https://github.com/huaweicloud/obsa-hdfs/releases/download/v45/hadoop-huaweicloud-2.8.3-hw-45.jar)添加到 **$BROKER_HOME/lib/** 路径中并重启 Broker。
