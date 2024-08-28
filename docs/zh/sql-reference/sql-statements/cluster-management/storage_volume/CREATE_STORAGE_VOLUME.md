---
displayed_sidebar: docs
---

# CREATE STORAGE VOLUME

## 功能

为远程存储系统创建存储卷。该功能自 v3.1 起支持。

存储卷由远程存储系统的属性和认证信息组成。您可以在 [StarRocks 存算分离集群](../../../../deployment/shared_data/shared_data.mdx)中创建数据库和云原生表时引用存储卷。

> **注意**
>
> - 仅拥有 SYSTEM 级 CREATE STORAGE VOLUME 权限的用户可以执行该操作。
> - 如果您需要基于 HDFS 创建存储卷，建议您不要随意修改 **HADOOP_CONF** 和 **core-site.xml/hdfs-site.xml**。如果以上文件中的参数与创建 Storage Volume 的参数存在差异，可能导致系统发生未知行为。

## 语法

```SQL
CREATE STORAGE VOLUME [IF NOT EXISTS] <storage_volume_name>
TYPE = { S3 | HDFS | AZBLOB }
LOCATIONS = ('<remote_storage_path>')
[ COMMENT '<comment_string>' ]
PROPERTIES
("key" = "value",...)
```

## 参数说明

| **参数**            | **说明**                                                     |
| ------------------- | ------------------------------------------------------------ |
| storage_volume_name | 存储卷的名称。请注意，您无法创建名为 `builtin_storage_volume` 的存储卷，因为该名称被用于创建内置存储卷。有关 storage volume 的命名要求，参见[系统限制](../../../System_limit.md)。 |
| TYPE                | 远程存储系统的类型。有效值：`S3` 、`AZBLOB` 和 `HDFS`。`S3` 代表AWS S3 或与 S3 协议兼容的存储系统。`AZBLOB` 代表 Azure Blob Storage（自 v3.1.1 起支持）。`HDFS` 代表 HDFS 集群。 |
| LOCATIONS           | 远程存储系统的位置。格式如下：<ul><li>AWS S3 或与 S3 协议兼容的存储系统：`s3://<s3_path>`。`<s3_path>` 必须为绝对路径，如 `s3://testbucket/subpath`。请注意，如果要为存储卷启用 [分区前缀](#分区前缀) 功能，您只能指定存储桶名称，不允许指定子路径。</li><li>Azure Blob Storage: `azblob://<azblob_path>`。`<azblob_path>` 必须为绝对路径，如 `azblob://testcontainer/subpath`。</li><li>HDFS：`hdfs://<host>:<port>/<hdfs_path>`。`<hdfs_path>` 必须为绝对路径，如 `hdfs://127.0.0.1:9000/user/xxx/starrocks`。</li><li>WebHDFS：`webhdfs://<host>:<http_port>/<hdfs_path>`，其中 `<http_port>` 为 NameNode 的 HTTP 端口。`<hdfs_path>` 必须为绝对路径，如 `webhdfs://127.0.0.1:50070/user/xxx/starrocks`。</li><li>ViewFS：`viewfs://<ViewFS_cluster>/<viewfs_path>`，其中 `<ViewFS_cluster>` 为 ViewFS 集群名。`<viewfs_path>` 必须为绝对路径，如 `viewfs://myviewfscluster/user/xxx/starrocks`。</li></ul> |
| COMMENT             | 存储卷的注释。                                               |
| PROPERTIES          | `"key" = "value"` 形式的参数对，用以指定访问远程存储系统的属性和认证信息。有关详细信息，请参阅 [PROPERTIES](#properties)。 |

### PROPERTIES

下表列出了存储卷所有可用的属性。这些属性的使用说明在列表后提供，从 [认证信息](#认证信息) 和 [特性](#特性) 两个方面，基于不同场景进行分类。

| **属性**                            | **描述**                                                     |
| ----------------------------------- | ------------------------------------------------------------ |
| enabled                             | 是否启用当前存储卷。默认值：`false`。已禁用的存储卷无法被引用。 |
| aws.s3.region                       | 需访问的 S3 存储空间的地区，如 `us-west-2`。                 |
| aws.s3.endpoint                     | 访问 S3 存储空间的连接地址，如 `https://s3.us-west-2.amazonaws.com`。[Preview] 自 v3.3.0 起，支持 Amazon S3 Express One Zone Storage，如 `https://s3express.us-west-2.amazonaws.com`。 |
| aws.s3.use_aws_sdk_default_behavior | 是否使用 AWS SDK 默认的认证凭证。有效值：`true` 和 `false` (默认)。 |
| aws.s3.use_instance_profile         | 是否使用 Instance Profile 或 Assumed Role 作为安全凭证访问 S3。有效值：`true` 和 `false` (默认)。<ul><li>如果您使用 IAM 用户凭证（Access Key 和 Secret Key）访问 S3，则需要将此项设为 `false`，并指定 `aws.s3.access_key` 和 `aws.s3.secret_key`。</li><li>如果您使用 Instance Profile 访问 S3，则需要将此项设为 `true`。</li><li>如果您使用 Assumed Role 访问 S3，则需要将此项设为 `true`，并指定 `aws.s3.iam_role_arn`。</li><li>如果您使用外部 AWS 账户通过 Assumed Role 认证访问 S3，则需要将此项设为 `true`，并额外指定 `aws.s3.iam_role_arn` 和 `aws.s3.external_id`。</li></ul> |
| aws.s3.access_key                   | 访问 S3 存储空间的 Access Key。                              |
| aws.s3.secret_key                   | 访问 S3 存储空间的 Secret Key。                              |
| aws.s3.iam_role_arn                 | 有访问 S3 存储空间权限 IAM Role 的 ARN。                     |
| aws.s3.external_id                  | 用于跨 AWS 账户访问 S3 存储空间的外部 ID。                   |
| azure.blob.endpoint   | Azure Blob Storage 的链接地址，如 `https://test.blob.core.windows.net`。 |
| azure.blob.shared_key | 访问 Azure Blob Storage 的共享密钥（Shared Key）。           |
| azure.blob.sas_token  | 访问 Azure Blob Storage 的共享访问签名（SAS）。              |
| hadoop.security.authentication                        | 指定认证方式。有效值：`simple`（默认） 和 `kerberos`。`simple` 表示简单认证，即 Username。`kerberos` 表示 Kerberos 认证。 |
| username                                              | 用于访问 HDFS 集群中 NameNode 节点的用户名。                      |
| hadoop.security.kerberos.ticket.cache.path            | 用于指定 kinit 生成的 Ticket Cache 文件的路径。                   |
| dfs.nameservices                                      | 自定义 HDFS 集群的名称。                                        |
| dfs.ha.namenodes.`<ha_cluster_name\>`                   | 自定义 NameNode 的名称，多个名称以逗号 (,) 分隔，双引号内不允许出现空格。其中 `<ha_cluster_name>` 为 `dfs.nameservices` 中自定义的 HDFS 服务的名称。 |
| dfs.namenode.rpc-address.`<ha_cluster_name\>`.`<NameNode\>` | 指定 NameNode 的 RPC 地址信息。 其中 `<NameNode>` 表示 `dfs.ha.namenodes.<ha_cluster_name>` 中自定义 NameNode 的名称。 |
| dfs.client.failover.proxy.provider                    | 指定客户端连接的 NameNode 的提供者，默认为 `org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider`。 |
| fs.viewfs.mounttable.`<ViewFS_cluster\>`.link./`<viewfs_path\>` | 需要挂载的 ViewFS 集群路径，多个路径以逗号 (,) 分隔。其中 `<ViewFS_cluster>` 为 `LOCATIONS` 中自定义的 ViewFS 集群名。 |
| aws.s3.enable_partitioned_prefix    | 是否启用存储卷的分区前缀功能。默认值：`false`。有关此功能的更多信息，请参阅 [分区前缀](#分区前缀)。 |
| aws.s3.num_partitioned_prefix       | 要为该存储卷创建的前缀数量。默认值：`256`。有效范围：[4, 1024]。|

#### 认证信息

##### AWS S3

- 如果您使用 AWS SDK 默认的认证凭证，请设置以下属性：

  ```SQL
  "enabled" = "{ true | false }",
  "aws.s3.region" = "<region>",
  "aws.s3.endpoint" = "<endpoint_url>",
  "aws.s3.use_aws_sdk_default_behavior" = "true"
  ```

- 如果您使用 IAM user-based 认证，请设置以下属性：

  ```SQL
  "enabled" = "{ true | false }",
  "aws.s3.region" = "<region>",
  "aws.s3.endpoint" = "<endpoint_url>",
  "aws.s3.use_aws_sdk_default_behavior" = "false",
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<access_key>",
  "aws.s3.secret_key" = "<secrete_key>"
  ```

- 如果您使用 Instance Profile 认证，请设置以下属性：

  ```SQL
  "enabled" = "{ true | false }",
  "aws.s3.region" = "<region>",
  "aws.s3.endpoint" = "<endpoint_url>",
  "aws.s3.use_aws_sdk_default_behavior" = "false",
  "aws.s3.use_instance_profile" = "true"
  ```

- 如果您使用 Assumed Role 认证，请设置以下属性：

  ```SQL
  "enabled" = "{ true | false }",
  "aws.s3.region" = "<region>",
  "aws.s3.endpoint" = "<endpoint_url>",
  "aws.s3.use_aws_sdk_default_behavior" = "false",
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<role_arn>"
  ```

- 如果您使用外部 AWS 账户通过 Assumed Role 认证，请设置以下属性：

  ```SQL
  "enabled" = "{ true | false }",
  "aws.s3.region" = "<region>",
  "aws.s3.endpoint" = "<endpoint_url>",
  "aws.s3.use_aws_sdk_default_behavior" = "false",
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<role_arn>",
  "aws.s3.external_id" = "<external_id>"
  ```

##### GCS

如果您使用 GCP Cloud Storage，请设置以下属性：

```SQL
"enabled" = "{ true | false }",

-- 例如：us-east-1
"aws.s3.region" = "<region>",

-- 例如：https://storage.googleapis.com
"aws.s3.endpoint" = "<endpoint_url>",

"aws.s3.access_key" = "<access_key>",
"aws.s3.secret_key" = "<secrete_key>"
```

##### 阿里云 OSS

如果您使用阿里云 OSS，请设置以下属性：

```SQL
"enabled" = "{ true | false }",

-- 例如：cn-zhangjiakou
"aws.s3.region" = "<region>",

-- 例如：https://oss-cn-zhangjiakou-internal.aliyuncs.com
"aws.s3.endpoint" = "<endpoint_url>",

"aws.s3.access_key" = "<access_key>",
"aws.s3.secret_key" = "<secrete_key>"
```

##### 华为云 OBS

如果您使用华为云 OBS，请设置以下属性：

```SQL
"enabled" = "{ true | false }",

-- 例如：cn-north-4
"aws.s3.region" = "<region>",

-- 例如：https://obs.cn-north-4.myhuaweicloud.com
"aws.s3.endpoint" = "<endpoint_url>",

"aws.s3.access_key" = "<access_key>",
"aws.s3.secret_key" = "<secrete_key>"
```

##### 腾讯云 COS

如果您使用腾讯云 COS，请设置以下属性：

```SQL
"enabled" = "{ true | false }",

-- 例如：ap-beijing
"aws.s3.region" = "<region>",

-- 例如：https://cos.ap-beijing.myqcloud.com
"aws.s3.endpoint" = "<endpoint_url>",

"aws.s3.access_key" = "<access_key>",
"aws.s3.secret_key" = "<secrete_key>"
```

##### 火山引擎 TOS

如果您使用火山引擎 TOS，请设置以下属性：

```SQL
"enabled" = "{ true | false }",

-- 例如：cn-beijing
"aws.s3.region" = "<region>",

-- 例如：https://tos-s3-cn-beijing.ivolces.com
"aws.s3.endpoint" = "<endpoint_url>",

"aws.s3.access_key" = "<access_key>",
"aws.s3.secret_key" = "<secrete_key>"
```

##### 金山云

如果您使用金山云，请设置以下属性：

```SQL
"enabled" = "{ true | false }",

-- 例如：BEIJING
"aws.s3.region" = "<region>",

-- 注意请使用三级域名, 金山云不支持二级域名
-- 例如：jeff-test.ks3-cn-beijing.ksyuncs.com
"aws.s3.endpoint" = "<endpoint_url>",

"aws.s3.access_key" = "<access_key>",
"aws.s3.secret_key" = "<secrete_key>"
```

##### MinIO

如果您使用 MinIO，请设置以下属性：

```SQL
"enabled" = "{ true | false }",

-- 例如：us-east-1
"aws.s3.region" = "<region>",

-- 例如：http://172.26.xx.xxx:39000
"aws.s3.endpoint" = "<endpoint_url>",

"aws.s3.access_key" = "<access_key>",
"aws.s3.secret_key" = "<secrete_key>"
```

##### Ceph S3

如果您使用 Ceph S3，请设置以下属性：

```SQL
"enabled" = "{ true | false }",

-- 例如：http://172.26.xx.xxx:7480
"aws.s3.endpoint" = "<endpoint_url>",

"aws.s3.access_key" = "<access_key>",
"aws.s3.secret_key" = "<secrete_key>"
```

##### Azure Blob Storage

StarRocks 自 v3.1.1 起支持基于 Azure Blob Storage 创建存储卷。

- 如果您使用共享密钥（Shared Key）认证，请设置以下 PROPERTIES：

  ```SQL
  "enabled" = "{ true | false }",
  "azure.blob.endpoint" = "<endpoint_url>",
  "azure.blob.shared_key" = "<shared_key>"
  ```

- 如果您使用共享访问签名（SAS）认证，请设置以下 PROPERTIES：

  ```SQL
  "enabled" = "{ true | false }",
  "azure.blob.endpoint" = "<endpoint_url>",
  "azure.blob.sas_token" = "<sas_token>"
  ```

> **注意**
>
> 创建 Azure Blob Storage Account 时必须禁用分层命名空间。

##### HDFS

- 如果您不使用认证接入 HDFS，请设置以下属性：

  ```SQL
  "enabled" = "{ true | false }"
  ```

- 如果您使用简单认证 (Username) 接入 HDFS（自 v3.2 起支持），请设置以下属性：

  ```SQL
  "enabled" = "{ true | false }",
  "hadoop.security.authentication" = "simple",
  "username" = "<hdfs_username>"
  ```

- 如果您使用 Kerberos Ticket Cache 认证接入 HDFS（自 v3.2 起支持），请设置以下属性：

  ```SQL
  "enabled" = "{ true | false }",
  "hadoop.security.authentication" = "kerberos",
  "hadoop.security.kerberos.ticket.cache.path" = "<ticket_cache_path>"
  ```

  > **注意**
  >
  > - 该设置仅为强制系统使用 KeyTab 通过 Kerberos 访问 HDFS。请确保每个 BE 或 CN 节点都能够访问 KeyTab 文件。同时，还需要确保正确设置 **/etc/krb5.conf** 文件。
  > - Ticket cache 由外部的 kinit 工具生成，请确保有 crontab 或类似的定期任务来刷新 Ticket。

- 如果您的 HDFS 集群启用了 NameNode HA 配置（自 v3.2 起支持），请额外设置以下属性：

  ```SQL
  "dfs.nameservices" = "<ha_cluster_name>",
  "dfs.ha.namenodes.<ha_cluster_name>" = "<NameNode1>,<NameNode2> [, ...]",
  "dfs.namenode.rpc-address.<ha_cluster_name>.<NameNode1>" = "<hdfs_host>:<hdfs_port>",
  "dfs.namenode.rpc-address.<ha_cluster_name>.<NameNode2>" = "<hdfs_host>:<hdfs_port>",
  [...]
  "dfs.client.failover.proxy.provider.<ha_cluster_name>" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
  ```

  更多信息，请参考 [HDFS HA 文档](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithNFS.html)。

  - 如果您使用 WebHDFS（自 v3.2 起支持），请设置以下属性：

  ```SQL
  "enabled" = "{ true | false }"
  ```

  更多信息，请参考 [WebHDFS 文档](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)。

- 如果您使用 Hadoop ViewFS（自 v3.2 起支持），请设置以下属性：

  ```SQL
  -- 请将 <ViewFS_cluster> 替换为 ViewFS 集群名。
  "fs.viewfs.mounttable.<ViewFS_cluster>.link./<viewfs_path_1>" = "hdfs://<hdfs_host_1>:<hdfs_port_1>/<hdfs_path_1>",
  "fs.viewfs.mounttable.<ViewFS_cluster>.link./<viewfs_path_2>" = "hdfs://<hdfs_host_2>:<hdfs_port_2>/<hdfs_path_2>",
  [, ...]
  ```

  更多信息，请参考 [ViewFS 文档](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ViewFs.html)。

#### 特性

##### 分区前缀

自 v3.2.4 起，StarRocks 支持基于兼容 S3 的对象存储系统创建带有分区前缀功能的存储卷。当启用此功能时，StarRocks 会将数据存储在存储桶下多个使用统一前缀的分区（子路径）中。由于存储桶的 QPS 或吞吐量限制是基于单个分区设置的，此举可以轻松提高 StarRocks 对存储桶中数据文件的读写性能。

如需启用此功能，除了上述与凭证相关的参数之外，您还需要设置以下属性：

```SQL
"aws.s3.enable_partitioned_prefix" = "{ true | false }",
"aws.s3.num_partitioned_prefix" = "<INT>"
```

:::note
- 分区前缀功能仅支持兼容 S3 对象存储系统，即存储卷的 `TYPE` 必须为 `S3`。
- 存储卷的 `LOCATIONS` 只能包含存储桶名称，例如，`s3://testbucket`。不允许在存储桶名称后指定子路径。
- 存储卷创建成功后，以上两个属性均无法修改。
- 使用 FE 配置文件 **fe.conf** 创建存储卷无法启用此功能。
:::

## 示例

示例一：为 AWS S3 存储空间 `defaultbucket` 创建存储卷 `my_s3_volume`，使用 IAM user-based 认证，并启用该存储卷。

```SQL
CREATE STORAGE VOLUME my_s3_volume
TYPE = S3
LOCATIONS = ("s3://defaultbucket/test/")
PROPERTIES
(
    "aws.s3.region" = "us-west-2",
    "aws.s3.endpoint" = "https://s3.us-west-2.amazonaws.com",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "xxxxxxxxxx",
    "aws.s3.secret_key" = "yyyyyyyyyy"
);
```

示例二：为 HDFS 创建存储卷 `my_hdfs_volume`，并启用该存储卷。

```SQL
CREATE STORAGE VOLUME my_hdfs_volume
TYPE = HDFS
LOCATIONS = ("hdfs://127.0.0.1:9000/sr/test/")
PROPERTIES
(
    "enabled" = "true"
);
```

示例三：使用简单验证为 HDFS 创建存储卷 `hdfsvolumehadoop`。

```sql
CREATE STORAGE VOLUME hdfsvolumehadoop
TYPE = HDFS
LOCATIONS = ("hdfs://127.0.0.1:9000/sr/test/")
PROPERTIES(
    "hadoop.security.authentication" = "simple",
    "username" = "starrocks"
);
```

示例四：使用 Kerberos Ticket Cache 认证接入 HDFS 并创建存储卷 `hdfsvolkerberos`。

```sql
CREATE STORAGE VOLUME hdfsvolkerberos
TYPE = HDFS
LOCATIONS = ("hdfs://127.0.0.1:9000/sr/test/")
PROPERTIES(
    "hadoop.security.authentication" = "kerberos",
    "hadoop.security.kerberos.ticket.cache.path" = "/path/to/ticket/cache/path"
);
```

示例五：为启用了 NameNode HA 配置的 HDFS 集群创建存储卷 `hdfsvolha`。

```sql
CREATE STORAGE VOLUME hdfsvolha
TYPE = HDFS
LOCATIONS = ("hdfs://myhacluster/data/sr")
PROPERTIES(
    "dfs.nameservices" = "myhacluster",
    "dfs.ha.namenodes.myhacluster" = "nn1,nn2,nn3",
    "dfs.namenode.rpc-address.myhacluster.nn1" = "machine1.example.com:8020",
    "dfs.namenode.rpc-address.myhacluster.nn2" = "machine2.example.com:8020",
    "dfs.namenode.rpc-address.myhacluster.nn3" = "machine3.example.com:8020",
    "dfs.namenode.http-address.myhacluster.nn1" = "machine1.example.com:9870",
    "dfs.namenode.http-address.myhacluster.nn2" = "machine2.example.com:9870",
    "dfs.namenode.http-address.myhacluster.nn3" = "machine3.example.com:9870",
    "dfs.client.failover.proxy.provider.myhacluster" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
);
```

示例六：为 WebHDFS 创建存储卷 `webhdfsvol`。

```sql
CREATE STORAGE VOLUME webhdfsvol
TYPE = HDFS
LOCATIONS = ("webhdfs://namenode:9870/data/sr");
```

示例七：使用 Hadoop ViewFS 创建存储卷 `viewfsvol`。

```sql
CREATE STORAGE VOLUME viewfsvol
TYPE = HDFS
LOCATIONS = ("viewfs://clusterX/data/sr")
PROPERTIES(
    "fs.viewfs.mounttable.clusterX.link./data" = "hdfs://nn1-clusterx.example.com:8020/data",
    "fs.viewfs.mounttable.clusterX.link./project" = "hdfs://nn2-clusterx.example.com:8020/project"
);
```

## 相关 SQL

- [ALTER STORAGE VOLUME](./ALTER_STORAGE_VOLUME.md)
- [DROP STORAGE VOLUME](./DROP_STORAGE_VOLUME.md)
- [SET DEFAULT STORAGE VOLUME](./SET_DEFAULT_STORAGE_VOLUME.md)
- [DESC STORAGE VOLUME](./DESC_STORAGE_VOLUME.md)
- [SHOW STORAGE VOLUMES](./SHOW_STORAGE_VOLUMES.md)
