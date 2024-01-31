---
displayed_sidebar: "Chinese"
---

# CREATE STORAGE VOLUME

## 功能

为远程存储系统创建存储卷。该功能自 v3.1 起支持。

存储卷由远程存储系统的属性和认证信息组成。您可以在 [StarRocks 存算分离集群](../../../deployment/shared_data/s3.md)中创建数据库和云原生表时引用存储卷。

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
| storage_volume_name | 存储卷的名称。请注意，您无法创建名为 `builtin_storage_volume` 的存储卷，因为该名称被用于创建内置存储卷。有关 storage volume 的命名要求，参见[系统限制](../../../reference/System_limit.md)。 |
| TYPE                | 远程存储系统的类型。有效值：`S3` 、`AZBLOB` 和 `HDFS`。`S3` 代表AWS S3 或与 S3 协议兼容的存储系统。`AZBLOB` 代表 Azure Blob Storage（自 v3.1.1 起支持）。`HDFS` 代表 HDFS 集群。 |
| LOCATIONS           | 远程存储系统的位置。格式如下：<ul><li>AWS S3 或与 S3 协议兼容的存储系统：`s3://<s3_path>`。`<s3_path>` 必须为绝对路径，如 `s3://testbucket/subpath`。</li><li>Azure Blob Storage: `azblob://<azblob_path>`。`<azblob_path>` 必须为绝对路径，如 `azblob://testcontainer/subpath`。</li><li>HDFS：`hdfs://<host>:<port>/<hdfs_path>`。`<hdfs_path>` 必须为绝对路径，如 `hdfs://127.0.0.1:9000/user/xxx/starrocks`。</li><li>WebHDFS：`webhdfs://<host>:<http_port>/<hdfs_path>`，其中 `<http_port>` 为 NameNode 的 HTTP 端口。`<hdfs_path>` 必须为绝对路径，如 `webhdfs://127.0.0.1:50070/user/xxx/starrocks`。</li><li>ViewFS：`viewfs://<ViewFS_cluster>/<viewfs_path>`，其中 `<ViewFS_cluster>` 为 ViewFS 集群名。`<viewfs_path>` 必须为绝对路径，如 `viewfs://myviewfscluster/user/xxx/starrocks`。</li></ul> |
| COMMENT             | 存储卷的注释。                                               |
| PROPERTIES          | `"key" = "value"` 形式的参数对，用以指定访问远程存储系统的属性和认证信息。有关详细信息，请参阅 [PROPERTIES](#properties)。 |

## PROPERTIES

- 如果您使用 AWS S3：

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

- 如果您使用 GCP Cloud Storage，请设置以下属性：

  ```SQL
  "enabled" = "{ true | false }",
  
  -- 例如：us-east-1
  "aws.s3.region" = "<region>",
  
  -- 例如：https://storage.googleapis.com
  "aws.s3.endpoint" = "<endpoint_url>",
  
  "aws.s3.access_key" = "<access_key>",
  "aws.s3.secret_key" = "<secrete_key>"
  ```

- 如果您使用阿里云 OSS，请设置以下属性：

  ```SQL
  "enabled" = "{ true | false }",
  
  -- 例如：cn-zhangjiakou
  "aws.s3.region" = "<region>",
  
  -- 例如：https://oss-cn-zhangjiakou-internal.aliyuncs.com
  "aws.s3.endpoint" = "<endpoint_url>",
  
  "aws.s3.access_key" = "<access_key>",
  "aws.s3.secret_key" = "<secrete_key>"
  ```

- 如果您使用华为云 OBS，请设置以下属性：

  ```SQL
  "enabled" = "{ true | false }",
  
  -- 例如：cn-north-4
  "aws.s3.region" = "<region>",
  
  -- 例如：https://obs.cn-north-4.myhuaweicloud.com
  "aws.s3.endpoint" = "<endpoint_url>",
  
  "aws.s3.access_key" = "<access_key>",
  "aws.s3.secret_key" = "<secrete_key>"
  ```

- 如果您使用腾讯云 COS，请设置以下属性：

  ```SQL
  "enabled" = "{ true | false }",
  
  -- 例如：ap-beijing
  "aws.s3.region" = "<region>",
  
  -- 例如：https://cos.ap-beijing.myqcloud.com
  "aws.s3.endpoint" = "<endpoint_url>",
  
  "aws.s3.access_key" = "<access_key>",
  "aws.s3.secret_key" = "<secrete_key>"
  ```

- 如果您使用火山引擎 TOS，请设置以下属性：

  ```SQL
  "enabled" = "{ true | false }",
  
  -- 例如：cn-beijing
  "aws.s3.region" = "<region>",
  
  -- 例如：https://tos-s3-cn-beijing.ivolces.com
  "aws.s3.endpoint" = "<endpoint_url>",
  
  "aws.s3.access_key" = "<access_key>",
  "aws.s3.secret_key" = "<secrete_key>"
  ```

- 如果您使用金山云，请设置以下属性：

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

- 如果您使用 MinIO，请设置以下属性：

  ```SQL
  "enabled" = "{ true | false }",
  
  -- 例如：us-east-1
  "aws.s3.region" = "<region>",
  
  -- 例如：http://172.26.xx.xxx:39000
  "aws.s3.endpoint" = "<endpoint_url>",
  
  "aws.s3.access_key" = "<access_key>",
  "aws.s3.secret_key" = "<secrete_key>"
  ```

- 如果您使用 Ceph S3，请设置以下属性：

  ```SQL
  "enabled" = "{ true | false }",
  
  -- 例如：http://172.26.xx.xxx:7480
  "aws.s3.endpoint" = "<endpoint_url>",
  
  "aws.s3.access_key" = "<access_key>",
  "aws.s3.secret_key" = "<secrete_key>"
  ```

  | **属性**                            | **描述**                                                     |
  | ----------------------------------- | ------------------------------------------------------------ |
  | enabled                             | 是否启用当前存储卷。默认值：`false`。已禁用的存储卷无法被引用。 |
  | aws.s3.region                       | 需访问的 S3 存储空间的地区，如 `us-west-2`。                 |
  | aws.s3.endpoint                     | 访问 S3 存储空间的连接地址，如 `https://s3.us-west-2.amazonaws.com`。 |
  | aws.s3.use_aws_sdk_default_behavior | 是否使用 AWS SDK 默认的认证凭证。有效值：`true` 和 `false` (默认)。 |
  | aws.s3.use_instance_profile         | 是否使用 Instance Profile 或 Assumed Role 作为安全凭证访问 S3。有效值：`true` 和 `false` (默认)。<ul><li>如果您使用 IAM 用户凭证（Access Key 和 Secret Key）访问 S3，则需要将此项设为 `false`，并指定 `aws.s3.access_key` 和 `aws.s3.secret_key`。</li><li>如果您使用 Instance Profile 访问 S3，则需要将此项设为 `true`。</li><li>如果您使用 Assumed Role 访问 S3，则需要将此项设为 `true`，并指定 `aws.s3.iam_role_arn`。</li><li>如果您使用外部 AWS 账户通过 Assumed Role 认证访问 S3，则需要将此项设为 `true`，并额外指定 `aws.s3.iam_role_arn` 和 `aws.s3.external_id`。</li></ul> |
  | aws.s3.access_key                   | 访问 S3 存储空间的 Access Key。                              |
  | aws.s3.secret_key                   | 访问 S3 存储空间的 Secret Key。                              |
  | aws.s3.iam_role_arn                 | 有访问 S3 存储空间权限 IAM Role 的 ARN。                     |
  | aws.s3.external_id                  | 用于跨 AWS 账户访问 S3 存储空间的外部 ID。                   |

- 如果您使用 Azure Blob Storage（自 v3.1.1 起支持）：

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

  | **属性**              | **描述**                                                     |
  | --------------------- | ------------------------------------------------------------ |
  | enabled               | 是否启用当前存储卷。默认值：`false`。已禁用的存储卷无法被引用。 |
  | azure.blob.endpoint   | Azure Blob Storage 的链接地址，如 `https://test.blob.core.windows.net`。 |
  | azure.blob.shared_key | 访问 Azure Blob Storage 的共享密钥（Shared Key）。           |
  | azure.blob.sas_token  | 访问 Azure Blob Storage 的共享访问签名（SAS）。              |

- 如果您使用 HDFS 存储：

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

    | **属性**                                              | **描述**                                                     |
    | ----------------------------------------------------- | ------------------------------------------------------------ |
    | enabled                                               | 是否启用当前存储卷。默认值：`false`。已禁用的存储卷无法被引用。       |
    | hadoop.security.authentication                        | 指定认证方式。有效值：`simple`（默认） 和 `kerberos`。`simple` 表示简单认证，即 Username。`kerberos` 表示 Kerberos 认证。 |
    | username                                              | 用于访问 HDFS 集群中 NameNode 节点的用户名。                      |
    | hadoop.security.kerberos.ticket.cache.path            | 用于指定 kinit 生成的 Ticket Cache 文件的路径。                   |
    | dfs.nameservices                                      | 自定义 HDFS 集群的名称。                                        |
    | dfs.ha.namenodes.`<ha_cluster_name\>`                   | 自定义 NameNode 的名称，多个名称以逗号 (,) 分隔，双引号内不允许出现空格。其中 `<ha_cluster_name>` 为 `dfs.nameservices` 中自定义的 HDFS 服务的名称。 |
    | dfs.namenode.rpc-address.`<ha_cluster_name\>`.`<NameNode\>` | 指定 NameNode 的 RPC 地址信息。 其中 `<NameNode>` 表示 `dfs.ha.namenodes.<ha_cluster_name>` 中自定义 NameNode 的名称。 |
    | dfs.client.failover.proxy.provider                    | 指定客户端连接的 NameNode 的提供者，默认为 `org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider`。 |
    | fs.viewfs.mounttable.`<ViewFS_cluster\>`.link./`<viewfs_path\>` | 需要挂载的 ViewFS 集群路径，多个路径以逗号 (,) 分隔。其中 `<ViewFS_cluster>` 为 `LOCATIONS` 中自定义的 ViewFS 集群名。 |

<!--

    | kerberos_keytab_content                               | 用于指定 Kerberos 中 keytab 文件的内容经过 Base64 编码之后的内容。该参数跟 `kerberos_keytab` 参数二选一配置。 |
    | password                                              | 用于访问 HDFS 集群中 NameNode 节点的密码。                   |
    | kerberos_principal                                    | 用于指定 Kerberos 的用户或服务 (Principal)。每个 Principal 在 HDFS 集群内唯一，由如下三部分组成：<ul><li>`username` 或 `servicename`：HDFS 集群中用户或服务的名称。</li><li>`instance`：HDFS 集群要认证的节点所在服务器的名称，用来保证用户或服务全局唯一。比如，HDFS 集群中有多个 DataNode 节点，各节点需要各自独立认证。</li><li>`realm`：域，必须全大写。</li></ul>示例：`nn/zelda1@ZELDA.COM`。 |

-->

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
