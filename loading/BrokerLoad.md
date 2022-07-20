# Broker Load

Broker Load 通过 Broker 程序访问和读取外部云存储系统上的数据，并利用自身的计算资源对数据进行预处理和导入。Broker Load 是一种异步的导入方式，需要通过 MySQL 协议异步创建导入作业。创建导入作业后，StarRocks 会异步地执行导入作业。您可以通过 SHOW LOAD 语句查看导入作业的结果。

Broker Load 适用于源数据保存在外部云存储系统、且数据量为几十到上百 GB 的业务场景。

## 基本原理

用户在提交导入任务后，FE 会生成对应的 Plan 并根据目前 BE 的个数和文件的大小，将 Plan 分给多个 BE 执行，每个 BE 执行一部分导入任务。BE 在执行过程中会通过 Broker 拉取数据，在对数据预处理之后将数据导入系统。所有 BE 均完成导入后，由 FE 最终判断导入是否成功。

下图展示了 Broker Load 的主要流程：

![Broker Load 原理图](/assets/4.2.2-1.png)

## 支持的数据格式

Broker Load 支持如下数据格式：

- CSV
- ORC
- Parquet

## 支持的外部云存储系统

Broker Load 支持从如下外部云存储系统导入数据：

- HDFS
- Amazon S3
- 阿里云 OSS
- 腾讯云 COS

## 基本操作

### Broker 部署

Broker Load 需要借助 Broker 程序程访问外部云存储系统。因此，使用 Broker Load 前，您需要提前部署好 Broker 程序。

Broker 程序的部署可参考[部署 Broker 节点](/administration/deploy_broker.md)。

### 创建导入作业

下面举例说明从各种外部存储系统导入数据的操作。更多示例和参数说明，请参考 [BROKER LOAD](/sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md)。

#### 从 HDFS 导入数据

```SQL
LOAD LABEL db1.label1
(
    DATA INFILE("hdfs://abc.com:8888/user/starRocks/test/ml/file1")
    INTO TABLE tbl1
    COLUMNS TERMINATED BY ","
    (tmp_c1, tmp_c2)
    SET
    (
        id=tmp_c2,
        name=tmp_c1
    ),

    DATA INFILE("hdfs://abc.com:8888/user/starRocks/test/ml/file2")
    INTO TABLE tbl2
    COLUMNS TERMINATED BY ","
    (col1, col2)
    where col1 > 1
)
WITH BROKER 'broker'
(
    "username" = "hdfs_username",
    "password" = "hdfs_password"
)
PROPERTIES
(
    "timeout" = "3600"
);
```

从 HDFS 导入数据时，您可以：

- 通过一个事务把数据导入到多张目标表中。

- 使用 `column_list` 关键字指定源文件和目标文件中的列的对应关系。

- 使用 `SET` 关键字指定将源文件中的列按照函数进行转化以后，再导入到目标表中。

- 使用 WHERE 子句对数据进行过滤。

#### 从 Amazon S3 导入 CSV 格式的数据

```SQL
LOAD LABEL example_db.label14
(
    DATA INFILE("s3a://my_bucket/input/file.csv")
    INTO TABLE `my_table`
    (k1, k2, k3)
)
WITH BROKER my_broker
(
    "fs.s3a.access.key" = "xxxxxxxxxxxxxxxxxxxx",
    "fs.s3a.secret.key" = "yyyyyyyyyyyyyyyyyyyy",
    "fs.s3a.endpoint" = "s3-ap-northeast-1.amazonaws.com"
)
```

注意：

- S3 存储空间的文件地址前缀为 `s3a://`。

- 必须提供 S3 存储空间的 Key 作为访问鉴权。

#### 从 阿里云 OSS 导入数据

```SQL
LOAD LABEL example_db.label12
(
    DATA INFILE("oss://my_bucket/input/file.csv")
    INTO TABLE `my_table`
    (k1, k2, k3)
)
WITH BROKER my_broker
(
    "fs.oss.accessKeyId" = "xxxxxxxxxxxxxxxxxxxxxxxxxx",
    "fs.oss.accessKeySecret" = "yyyyyyyyyyyyyyyyyyyy",
    "fs.oss.endpoint" = "oss-cn-zhangjiakou-internal.aliyuncs.com"
)
```

#### 从腾讯云 COS 导入 CSV 格式的数据

```SQL
LOAD LABEL example_db.label13
(
    DATA INFILE("cosn://my_bucket/input/file.csv")
    INTO TABLE `my_table`
    (k1, k2, k3)
)
WITH BROKER my_broker
(
    "fs.cosn.userinfo.secretId" = "xxxxxxxxxxxxxxxxx",
    "fs.cosn.userinfo.secretKey" = "yyyyyyyyyyyyyyyy",
    "fs.cosn.bucket.endpoint_suffix" = "cos.ap-beijing.myqcloud.com"
)
```

### 查看导入作业

您可以在 SHOW LOAD 语句中指定 `LABEL` 来查询对应导入作业的执行状态，如下所示：

```Plain
mysql> show load where label = 'label1'\G
*************************** 1. row ***************************
         JobId: 76391
         Label: label1
         State: FINISHED
      Progress: ETL:N/A; LOAD:100%
          Type: BROKER
       EtlInfo: unselected.rows=4; dpp.abnorm.ALL=15; dpp.norm.ALL=28133376
      TaskInfo: cluster:N/A; timeout(s):10800; max_filter_ratio:5.0E-5
      ErrorMsg: N/A
    CreateTime: 2019-07-27 11:46:42
  EtlStartTime: 2019-07-27 11:46:44
 EtlFinishTime: 2019-07-27 11:46:44
 LoadStartTime: 2019-07-27 11:46:44
LoadFinishTime: 2019-07-27 11:50:16
           URL: http://192.168.1.1:8040/api/_load_error_log?file=__shard_4/error_log_insert_stmt_4bb00753932c491a-a6da6e2725415317_4bb00753932c491a_a6da6e2725415317
    JobDetails: {"Unfinished backends":{"9c3441027ff948a0-8287923329a2b6a7":[10002]},"ScannedRows":2390016,"TaskNumber":1,"All backends":{"9c3441027ff948a0-8287923329a2b6a7":[10002]},"FileNumber":1,"FileSize":1073741824}
```

在 SHOW LOAD 语句返回的结果集中:

- `State` 表示导入作业当前所处的阶段。
  
  在 Broker Load 导入过程中，有如下两个表示导入中的阶段：

  - `PENDING`：导入作业正在等待执行中。
  - `LOADING`：导入作业正在执行中。
  
  导入作业的最终阶段有如下两个：

  - `FINISHED`：导入作业成功。
  - `CANCELLED`：导入作业失败。
  
  当导入作业处于以上两个阶段时，表示导入完成。

- `Type` 表示导入作业的导入方式。对 Broker Load 导入， `type` 参数的取值为 `BROKER`。
- Broker Load 导入由于没有 **ETL** 阶段，所以 `EtlStartTime`、`EtlFinishTime` 和 `LoadStartTime` 取值显示相同。

更多参数说明，请参考 [SHOW LOAD](/sql-reference/sql-statements/data-manipulation/SHOW%20LOAD.md)。

### 取消导入作业

当 Broker Load 作业状态不为 **CANCELLED** 或 **FINISHED** 时，您可以通过 CANCEL LOAD 语句来取消该导入作业。取消时需要指定该导入作业的 `Label`。具体语法，请参见 [CANCEL LOAD](/sql-reference/sql-statements/data-manipulation/CANCEL%20LOAD.md)。

## 相关配置

Broker Load 支持并行处理导入作业中的子任务。

一个导入作业可以拆分成一个或多个子任务，各子任务在作业对应的事务里并行执行。子任务的数量影响导入的每批次数据量的大小、以及子任务的执行并行度。建议您根据业务需求，将作业拆分合适数量的子任务。拆分由 `LOAD` 语句中的`data_desc` 来决定，比如：

- 如果多个 `data_desc` 对应导入多张不同的表，则每个表的导入会拆分成一个子任务。
- 如果多个 `data_desc` 对应导入同一张表的不同分区，则每个分区也会拆分成一个子任务。

每个子任务还会拆分成一个或者多个实例，然后将这些实例平均分配到 BE 上并行执行。实例的拆分由以下 [FE 配置](/administration/Configuration.md)决定：

- `min_bytes_per_broker_scanner`：单个实例处理的最小数据量，默认为 64 MB。
- `max_broker_concurrency`：单个子任务允许的最大并发实例数，默认为 100 个。
- `load_parallel_instance_num`：单个 BE 上每个作业允许的并发实例数，默认为 1 个。
  实例的总数 = min（导入文件总大小/单个实例处理的最小数据量，单个子任务允许的最大并发实例数，单个 BE 上允许的并发实例数 * BE 总数）

一般情况下，一个导入作业只有一个 `data_desc`，只会拆分成一个子任务。子任务会拆分成与 BE 总数相等的实例，然后分配到所有 BE 上并行执行。

## 常见问题

请参见 [Broker Load 常见问题](/faq/loading/Broker_load_faq.md)。
