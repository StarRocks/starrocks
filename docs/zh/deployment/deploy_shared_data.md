# 部署使用 StarRocks 存算分离集群

本文介绍如何部署和使用 StarRocks 存算分离集群。

StarRocks 存算分离集群采用了存储计算分离架构，特别为云存储设计。在存算分离的模式下，StarRocks 将数据存储在兼容 S3 协议的对象存储（例如 AWS S3、OSS 以及 MinIO）或 HDFS 中，而本地盘作为热数据缓存，用以加速查询。通过存储计算分离架构，您可以降低存储成本并且优化资源隔离。除此之外，集群的弹性扩展能力也得以加强。在查询命中缓存的情况下，存算分离集群的查询性能与存算一体集群性能一致。

相对存算一体架构，StarRocks 的存储计算分离架构提供以下优势：

- 廉价且可无缝扩展的存储。
- 弹性可扩展的计算能力。由于数据不存储在 CN 节点中，因此集群无需进行跨节点数据迁移或 Shuffle 即可完成扩缩容。
- 热数据的本地磁盘缓存，用以提高查询性能。
- 可调整缓存热数据的生存时间 (TTL)，系统自动删除过期缓存数据，节省本地磁盘存储空间。
- 可选异步导入数据至对象存储，提高导入效率。

StarRocks 存算分离集群架构如下：

![Shared-data Architecture](../assets/share_data_arch.png)

该功能从 3.0 版本开始支持。

## 部署 StarRocks 存算分离集群

StarRocks 存算分离集群的部署方式与存算一体集群的部署方式类似，但存算分离集群需要部署 CN 节点而非 BE 节点。本小节仅列出部署 StarRocks 存算分离集群时需要添加到 FE 和 CN 配置文件 **fe.conf** 和 **cn.conf** 中的额外配置项。有关部署 StarRocks 集群的详细说明，请参阅 [部署 StarRocks](/deployment/deploy_manually.md)。

### 配置存算分离集群 FE 节点

在启动 FE 之前，在 FE 配置文件 **fe.conf** 中添加以下配置项：

| **配置项**                          | **描述**                                                     |
| ----------------------------------- | ------------------------------------------------------------ |
| run_mode                            | StarRocks 集群的运行模式。有效值：`shared_data` 和 `shared_nothing` (默认)。`shared_data` 表示在存算分离模式下运行 StarRocks。`shared_nothing` 表示在存算一体模式下运行 StarRocks。<br />**注意**<br />StarRocks 集群不支持存算分离和存算一体模式混合部署。<br />请勿在集群部署完成后更改 `run_mode`，否则将导致集群无法再次启动。不支持从存算一体集群转换为存算分离集群，反之亦然。 |
| cloud_native_meta_port              | 云原生元数据服务监听端口。默认值：`6090`。                   |
| cloud_native_storage_type           | 您使用的存储类型。有效值：`S3`（默认）和 `HDFS`。如果您将此项指定为 `S3`，则必须添加以 `aws_s3` 为前缀的配置项。如果将此项指定为 `HDFS`，则只需指定 `cloud_native_hdfs_url`。 |
| cloud_native_hdfs_url               | HDFS 存储的 URL，例如 `hdfs://127.0.0.1:9000/user/xxx/starrocks/`。 |
| aws_s3_path                         | 用于存储数据的 S3 存储空间路径，由 S3 存储桶的名称及其下的子路径（如有）组成，如 `testbucket/subpath`。 |
| aws_s3_region                       | 需访问的 S3 存储空间的地区，如 `us-west-2`。                 |
| aws_s3_endpoint                     | 访问 S3 存储空间的连接地址，如 `https://s3.us-west-2.amazonaws.com`。 |
| aws_s3_use_aws_sdk_default_behavior | 是否使用 AWS SDK 默认的认证凭证。有效值：`true` 和 `false` (默认)。 |
| aws_s3_use_instance_profile         | 是否使用 Instance Profile 或 Assumed Role 作为安全凭证访问 S3。有效值：`true` 和 `false` (默认)。<ul><li>如果您使用 IAM 用户凭证（Access Key 和 Secret Key）访问 S3，则需要将此项设为 `false`，并指定 `aws_s3_access_key` 和 `aws_s3_secret_key`。</li><li>如果您使用 Instance Profile 访问 S3，则需要将此项设为 `true`。</li><li>如果您使用 Assumed Role 访问 S3，则需要将此项设为 `true`，并指定 `aws_s3_iam_role_arn`。</li><li>如果您使用外部 AWS 账户通过 Assumed Role 认证访问 S3，则需要额外指定 `aws_s3_external_id`。</li></ul> |
| aws_s3_access_key                   | 访问 S3 存储空间的 Access Key。                              |
| aws_s3_secret_key                   | 访问 S3 存储空间的 Secret Key。                              |
| aws_s3_iam_role_arn                 | 有访问 S3 存储空间权限 IAM Role 的 ARN。                     |
| aws_s3_external_id                  | 用于跨 AWS 账户访问 S3 存储空间的外部 ID。                   |

- 如果您使用 HDFS 存储，请添加以下配置项：

  ```Plain
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = HDFS
  cloud_native_hdfs_url = <hdfs_url>
  ```

- 如果您使用 AWS S3：

  - 如果您使用 AWS SDK 默认的认证凭证，请添加以下配置项：

    ```Plain
    run_mode = shared_data
    cloud_native_meta_port = <meta_port>
    cloud_native_storage_type = S3

    # 如 testbucket/subpath
    aws_s3_path = <s3_path>

    # 如 us-west-2
    aws_s3_region = <region>

    # 如 https://s3.us-west-2.amazonaws.com
    aws_s3_endpoint = <endpoint_url>

    aws_s3_use_aws_sdk_default_behavior = true
    ```

  - 如果您使用 IAM user-based 认证，请添加以下配置项：

    ```Plain
    run_mode = shared_data
    cloud_native_meta_port = <meta_port>
    cloud_native_storage_type = S3
    aws_s3_path = <s3_path>
    aws_s3_region = <region>
    aws_s3_endpoint = <endpoint_url>
    aws_s3_access_key = <access_key>
    aws_s3_secret_key = <secret_key>
    ```

  - 如果您使用 Instance Profile 认证，请添加以下配置项：

    ```Plain
    run_mode = shared_data
    cloud_native_meta_port = <meta_port>
    cloud_native_storage_type = S3

    # 如 testbucket/subpath
    aws_s3_path = <s3_path>

    # 如 us-west-2
    aws_s3_region = <region>

    # 如 https://s3.us-west-2.amazonaws.com
    aws_s3_endpoint = <endpoint_url>

    aws_s3_use_instance_profile = true
    ```

  - 如果您使用 Assumed Role 认证，请添加以下配置项：

    ```Plain
    run_mode = shared_data
    cloud_native_meta_port = <meta_port>
    cloud_native_storage_type = S3

    # 如 testbucket/subpath
    aws_s3_path = <s3_path>

    # 如 us-west-2
    aws_s3_region = <region>

    # 如 https://s3.us-west-2.amazonaws.com
    aws_s3_endpoint = <endpoint_url>

    aws_s3_use_instance_profile = true
    aws_s3_iam_role_arn = <role_arn>
    ```

  - 如果您使用外部 AWS 账户通过 Assumed Role 认证，请添加以下配置项：

    ```Plain
    run_mode = shared_data
    cloud_native_meta_port = <meta_port>
    cloud_native_storage_type = S3

    # 如 testbucket/subpath
    aws_s3_path = <s3_path>

    # 如 us-west-2
    aws_s3_region = <region>

    # 如 https://s3.us-west-2.amazonaws.com
    aws_s3_endpoint = <endpoint_url>

    aws_s3_use_instance_profile = true
    aws_s3_iam_role_arn = <role_arn>
    aws_s3_external_id = <external_id>
    ```

- 如果您使用 GCP Cloud Storage：

  ```Plain
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = S3

  # 如 testbucket/subpath
  aws_s3_path = <s3_path>

  # 例如：us-east-1
  aws_s3_region = <region>

  # 例如：https://storage.googleapis.com
  aws_s3_endpoint = <endpoint_url>

  aws_s3_access_key = <access_key>
  aws_s3_secret_key = <secret_key>
  ```

- 如果您使用阿里云 OSS：

  ```Plain
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = S3

  # 如 testbucket/subpath
  aws_s3_path = <s3_path>

  # 例如：cn-zhangjiakou
  aws_s3_region = <region>

  # 例如：https://oss-cn-zhangjiakou-internal.aliyuncs.com
  aws_s3_endpoint = <endpoint_url>

  aws_s3_access_key = <access_key>
  aws_s3_secret_key = <secret_key>
  ```

- 如果您使用华为云 OBS：

  ```Plain
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = S3

  # 如 testbucket/subpath
  aws_s3_path = <s3_path>

  # 例如：cn-north-4
  aws_s3_region = <region>

  # 例如：https://obs.cn-north-4.myhuaweicloud.com
  aws_s3_endpoint = <endpoint_url>

  aws_s3_access_key = <access_key>
  aws_s3_secret_key = <secret_key>
  ```

- 如果您使用腾讯云 COS：

  ```Plain
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = S3

  # 如 testbucket/subpath
  aws_s3_path = <s3_path>

  # 例如：ap-beijing
  aws_s3_region = <region>

  # 例如：https://cos.ap-beijing.myqcloud.com
  aws_s3_endpoint = <endpoint_url>

  aws_s3_access_key = <access_key>
  aws_s3_secret_key = <secret_key>
  ```

- 如果您使用火山引擎 TOS：

  ```Plain
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = S3

  # 如 testbucket/subpath
  aws_s3_path = <s3_path>

  # 例如：cn-beijing
  aws_s3_region = <region>

  # 例如：https://tos-s3-cn-beijing.ivolces.com
  aws_s3_endpoint = <endpoint_url>

  aws_s3_access_key = <access_key>
  aws_s3_secret_key = <secret_key>
  ```

- 如果您使用金山云：

  ```Plain
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = S3

  # 如 testbucket/subpath
  aws_s3_path = <s3_path>

  # 例如：BEIJING
  aws_s3_region = <region>
  
  # 注意请使用三级域名, 金山云不支持二级域名
  # 例如：jeff-test.ks3-cn-beijing.ksyuncs.com
  aws_s3_endpoint = <endpoint_url>

  aws_s3_access_key = <access_key>
  aws_s3_secret_key = <secret_key>
  ```

- 如果您使用 MinIO：

  ```Plain
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = S3

  # 如 testbucket/subpath
  aws_s3_path = <s3_path>

  # 例如：us-east-1
  aws_s3_region = <region>

  # 例如：http://172.26.xx.xxx:39000
  aws_s3_endpoint = <endpoint_url>

  aws_s3_access_key = <access_key>
  aws_s3_secret_key = <secret_key>
  ```

- 如果您使用 Ceph S3：

  ```Plain
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = S3

  # 如 testbucket/subpath
  aws_s3_path = <s3_path>
  
  # 例如：http://172.26.xx.xxx:7480
  aws_s3_endpoint = <endpoint_url>

  aws_s3_access_key = <access_key>
  aws_s3_secret_key = <secret_key>
  ```

### 配置存算分离集群 CN 节点

**在启动 CN 之前**，在 CN 配置文件 **cn.conf** 中添加以下配置项：

```Plain
starlet_port = <starlet_port>
storage_root_path = <storage_root_path>
```

| **配置项**              | **描述**                 |
| ---------------------- | ------------------------ |
| starlet_port | 存算分离模式下，用于 CN 心跳服务的端口。默认值：`9070`。 |
| storage_root_path      | 本地缓存数据依赖的存储目录以及该存储介质的类型，多块盘配置使用分号（;）隔开。如果为 SSD 磁盘，需在路径后添加 `,medium:ssd`，如果为 HDD 磁盘，需在路径后添加 `,medium:hdd`。例如：`/data1,medium:hdd;/data2,medium:ssd`。默认值：`${STARROCKS_HOME}/storage`。 |

> **说明**
>
> 本地缓存数据将存储在 **`<storage_root_path>/starlet_cache`** 路径下。

## 使用 StarRocks 存算分离集群

StarRocks 存算分离集群的使用也类似于普通 StarRocks 集群。

### 创建表

连接到 StarRocks 存算分离集群后，您需要创建数据库，并在数据库中创建表。目前，StarRocks 存算分离集群支持以下数据模型：

- 明细模型（Duplicate Key）
- 聚合模型（Aggregate Key）
- 更新模型（Unique Key）

> **说明**
>
> 目前，StarRocks 存算分离集群暂不支持**主键模型**（Primary Key）。

以下示例创建数据库 `cloud_db`，并基于明细模型创建表 `detail_demo`，启用本地磁盘缓存，将缓存过期时间设置为 30 天，并禁用异步数据导入：

```SQL
CREATE DATABASE cloud_db;
USE cloud_db;
CREATE TABLE IF NOT EXISTS detail_demo (
    recruit_date  DATE           NOT NULL COMMENT "YYYY-MM-DD",
    region_num    TINYINT        COMMENT "range [-128, 127]",
    num_plate     SMALLINT       COMMENT "range [-32768, 32767] ",
    tel           INT            COMMENT "range [-2147483648, 2147483647]",
    id            BIGINT         COMMENT "range [-2^63 + 1 ~ 2^63 - 1]",
    password      LARGEINT       COMMENT "range [-2^127 + 1 ~ 2^127 - 1]",
    name          CHAR(20)       NOT NULL COMMENT "range char(m),m in (1-255) ",
    profile       VARCHAR(500)   NOT NULL COMMENT "upper limit value 65533 bytes",
    ispass        BOOLEAN        COMMENT "true/false"
)
DUPLICATE KEY(recruit_date, region_num)
DISTRIBUTED BY HASH(recruit_date, region_num)
PROPERTIES (
    "enable_storage_cache" = "true",
    "storage_cache_ttl" = "2592000",
    "enable_async_write_back" = "false"
);
```

> 注意
>
> 自 2.5.7 版本起，StarRocks 支持在建表和新增分区时自动设置分桶数量 (BUCKETS)，您无需手动设置分桶数量。更多信息，请参见 [确定分桶数量](../table_design/Data_distribution.md#确定分桶数量)。

除了常规表 PROPERTIES 之外，您还需要在创建表时指定以下 PROPERTIES：

| **属性**                | **描述**                                                     |
| ----------------------- | ------------------------------------------------------------ |
| enable_storage_cache    | 是否启用本地磁盘缓存。默认值：`true`。<ul><li>当该属性设置为 `true` 时，数据会同时导入对象存储（或 HDFS）和本地磁盘（作为查询加速的缓存）。</li><li>当该属性设置为 `false` 时，数据仅导入到对象存储中。</li></ul>**说明**<br />如需启用本地磁盘缓存，必须在 CN 配置项 `storage_root_path` 中指定磁盘目录。 |
| storage_cache_ttl       | 启用本地磁盘缓存后，StarRocks 在本地磁盘中缓存热数据的存活时间。过期数据将从本地磁盘中删除。如果将该值设置为 `-1`，则缓存数据不会过期。默认值：`2592000`（30 天）。<br />**注意**<br />当禁用本地磁盘缓存时，您无需设置该配置项。如果您禁用了本地磁盘缓存，并且将此项设置为除 `0` 以外的值，StarRocks 将出现未知行为。 |
| enable_async_write_back | 是否允许数据异步写入对象存储。默认值：`false`。<ul><li>当该属性设置为 `true` 时，导入任务在数据写入本地磁盘缓存后立即返回成功，数据将异步写入对象存储。允许数据异步写入可以提升导入性能，但如果系统发生故障，可能会存在一定的数据可靠性风险。</li><li>当该属性设置为 `false` 时，只有在数据同时写入对象存储和本地磁盘缓存后，导入任务才会返回成功。禁用数据异步写入保证了更高的可用性，但会导致较低的导入性能。</li></ul> |

### 查看表信息

您可以通过 `SHOW PROC "/dbs/<db_id>"` 查看特定数据库中的表的信息。详细信息，请参阅 [SHOW PROC](../sql-reference/sql-statements/Administration/SHOW_PROC.md)。

示例：

```Plain
mysql> SHOW PROC "/dbs/xxxxx";
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+------------------------------+
| TableId | TableName   | IndexNum | PartitionColumnName | PartitionNum | State  | Type         | LastConsistencyCheckTime | ReplicaCount | PartitionType | StoragePath                  |
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+------------------------------+
| 12003   | detail_demo | 1        | NULL                | 1            | NORMAL | CLOUD_NATIVE | NULL                     | 8            | UNPARTITIONED | s3://xxxxxxxxxxxxxx/1/12003/ |
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+------------------------------+
```

StarRocks 存算分离集群中表的 `Type` 为 `CLOUD_NATIVE`。`StoragePath` 字段为表在对象存储中的路径。

### 向 StarRocks 存算分离集群导入数据

StarRocks 存算分离集群支持 StarRocks 提供的所有导入方式。详细信息，请参阅 [导入总览](../loading/Loading_intro.md)。

### 在 StarRocks 存算分离集群查询

StarRocks 存算分离集群支持 StarRocks 提供的所有查询方式。详细信息，请参阅 [SELECT](../sql-reference/sql-statements/data-manipulation/SELECT.md)。
