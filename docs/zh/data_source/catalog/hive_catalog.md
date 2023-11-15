# Hive catalog

本文介绍如何创建 Hive catalog，以及需要做哪些相应的配置。

Hive catalog 是一个外部数据目录 (external catalog)。StarRocks 2.3 及以上版本支持通过该目录直接查询 Apache Hive™ 集群中的数据，无需数据导入或创建外部表。

此外，您还可以基于 Hive Catalog ，结合 [INSERT INTO](../../sql-reference/sql-statements/data-manipulation/insert.md) 能力来实现数据转换和导入。

## 使用限制

- StarRocks 支持查询如下格式的 Hive 数据：Parquet、ORC 和 CSV。
- StarRocks 支持查询如下类型的 Hive 数据：TINYINT、SMALLINT、DATE、BOOLEAN、INTEGER、BIGINT、TIMESTAMP、STRING、VARCHAR、CHAR、DOUBLE、FLOAT、DECIMAL 和 ARRAY。注意查询命中不支持的数据类型会报错，不支持的数据类型包括：INTERVAL、BINARY、MAP、 STRUCT 和 UNION。
- StarRocks 2.4 及以上版本支持使用 [DESC](/sql-reference/sql-statements/Utility/DESCRIBE.md) 语句查看 Hive 表结构。查看时，不支持的数据类型会显示成`unknown`。

## 前提条件

在创建 Hive catalog 前，您需要在 StarRocks 中进行相应的配置，以便能够访问 Hive 的存储系统和元数据服务。StarRocks 当前支持的 Hive 存储系统包括：HDFS、Amazon S3、阿里云对象存储 OSS 和腾讯云对象存储 COS；支持的 Hive 元数据服务为 Hive metastore。

### HDFS

如使用 HDFS 作为存储系统，则需要在 StarRocks 中做如下配置。

- （可选）设置 StarRocks 访问 HDFS 和 Hive metastore 的用户名。 您可以在每个 FE 的 **fe/conf/hadoop_env.sh** 和每个 BE 的 **be/conf/hadoop_env.sh** 文件中通过配置 `HADOOP_USERNAME` 来设置该用户名，设置后重启各个 FE 和 BE 生效。如不设置，则默认使用 FE 和 BE 进程的用户名进行访问。一个 StarRocks 集群仅支持配置一个用户名。

- 查询时，StarRocks 的 FE 和 BE 都会通过 HDFS 客户端访问 HDFS。一般情况下，StarRocks 会按照默认配置来启动 HDFS 客户端，无需手动配置。但在以下场景中，需要进行手动配置：
  - 如 HDFS 开启了 HA（高可用）模式，则需要将 HDFS 集群中的 **hdfs-site.xml** 文件放到每一个 FE 的 **$FE_HOME/conf** 下以及每个 BE 的 **$BE_HOME/conf** 下。  
  - 如 HDFS 配置了 ViewFs，则需要将 HDFS 集群中的 **core-site.xml** 文件放到每一个 FE 的 **$FE_HOME/conf** 下以及每个 BE 的 **$BE_HOME/conf** 下。

> **注意**
>
> 如果查询时因为域名无法识别而访问失败 (unknown host)，则需要将 HDFS 节点域名和其 IP 的映射关系配置到 **/etc/hosts** 路径中。

### Kerberos 认证

如 HDFS 或 Hive metastore 开启了 Kerberos 认证，则需要在 StarRocks 中做如下配置。

- 在每个 FE 和 每个 BE 机器上执行 `kinit -kt keytab_path principal` 命令从 Key Distribution Center (KDC) 获取到 Ticket Granting Ticket。注意使用该命令访问 KDC 具有时效性，所以需要使用 cron 定期执行该命令。执行命令的用户需要有访问 Hive metastore 和 HDFS 的权限。
- 在每个 FE 的 **$FE_HOME/conf/fe.conf** 和每个 BE 的 **$BE_HOME/conf/be.conf** 文件中设置 `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"`。其中 `/etc/krb5.conf` 是 **krb5.conf** 文件的路径，可修改。

### Amazon S3

如使用 Amazon S3 作为存储系统，则需要在 StarRocks 中做如下配置。

1. 在每个 FE 的 **$FE_HOME/conf/core-site.xml** 文件中添加如下配置。

      ```XML
      <configuration>
          <property>
              <name>fs.s3a.impl</name>
              <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
          </property>
          <property>
              <name>fs.AbstractFileSystem.s3a.impl</name>
              <value>org.apache.hadoop.fs.s3a.S3A</value>
          </property>
          <property>
              <name>fs.s3a.access.key</name>
              <value>******</value>
          </property>
          <property>
              <name>fs.s3a.secret.key</name>
              <value>******</value>
          </property>
          <property>
              <name>fs.s3a.endpoint</name>
              <value>******</value>
          </property>
          <property>
              <name>fs.s3a.connection.maximum</name>
              <value>500</value>
          </property>
      </configuration>
      ```

    配置项说明：

      | **配置项**                | **说明**                                                     |
      | ------------------------- | ------------------------------------------------------------ |
      | fs.s3a.access.key         | AWS 根用户或 IAM 用户的 access key ID （即访问密钥 ID）。获取方式，请参见[了解并获取您的 AWS 凭证](https://docs.aws.amazon.com/zh_cn/general/latest/gr/aws-sec-cred-types.html)。 |
      | fs.s3a.secret.key         | AWS 根用户或 IAM 用户的 secret access key（即秘密访问密钥）。获取方式，请参见[了解并获取您的 AWS 凭证](https://docs.aws.amazon.com/zh_cn/general/latest/gr/aws-sec-cred-types.html)。 |
      | fs.s3a.endpoint           | Amazon S3 服务所在地域的 endpoint，例如`s3.us-west-2.amazonaws.com`即为美国西部（俄勒冈）的 endpoint。您可以根据 endpoint 与地域的对应关系进行查找，详情参见 [Amazon Simple Storage Service 终端节点和配额](https://docs.aws.amazon.com/zh_cn/general/latest/gr/s3.html)。 |
      | fs.s3a.connection.maximum | Amazon S3 的最大连接数， 默认值为 500。如查询时有报错 `Timeout waiting for connection from poll`，可适当调高该参数。 |

2. 在每个 BE 的 **$BE_HOME/conf/be.conf** 文件中添加如下配置项。

      | **配置项**                       | **说明**                                                     |
      | -------------------------------- | ------------------------------------------------------------ |
      | object_storage_access_key_id     | AWS 根用户或 IAM 用户的 access key ID，取值和 `fs.s3a.access.key` 相同。 |
      | object_storage_secret_access_key | AWS 根用户或 IAM 用户的 secret access key，取值和 `fs.s3a.secret.key` 相同。 |
      | object_storage_endpoint          | Amazon S3 服务所在地域的 endpoint，取值和 `fs.s3a.endpoint` 相同。 |

3. 重启所有 FE 和 BE。

### 腾讯云对象存储 COS

如使用 COS 作为存储系统，则需要在 StarRocks 中做如下配置。

1. 下载[依赖库](https://cdn-thirdparty.starrocks.com/hive_s3_jar.tar.gz)并将其添加到每个 FE 的 **$FE_HOME/lib/** 路径下。

2. 在每个 FE 的 **$FE_HOME/conf/core-site.xml** 文件中添加如下配置。

    ```XML
    <configuration>
        <property>
            <name>fs.s3a.impl</name>
            <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
        </property>
        <property>
            <name>fs.AbstractFileSystem.s3a.impl</name>
            <value>org.apache.hadoop.fs.s3a.S3A</value>
        </property>
        <property>
            <name>fs.s3a.access.key</name>
            <value>******</value>
        </property>
        <property>
            <name>fs.s3a.secret.key</name>
            <value>******</value>
        </property>
        <property>
            <name>fs.s3a.endpoint</name>
            <value>******</value>
        </property>
        <property>
            <name>fs.s3a.connection.maximum</name>
            <value>500</value>
        </property>
    </configuration>
    ```

    配置项说明：

    | **配置项**                | **说明**                                                     |
    | ------------------------- | ------------------------------------------------------------ |
    | fs.s3a.access.key         | COS 永久密钥的 SecretId。获取方式，请参见[使用永久密钥访问 COS](https://cloud.tencent.com/document/product/436/68282)。 |
    | fs.s3a.secret.key         | COS 永久密钥的 SecretKey。获取方式，请参见[使用永久密钥访问 COS](https://cloud.tencent.com/document/product/436/68282)。 |
    | fs.s3a.endpoint           | COS 存储桶所在地域对应的 endpoint（即访问域名），您可以根据 endpoint 与地域的对应关系进行查找，详情参见[地域和访问域名](https://cloud.tencent.com/document/product/436/6224)。 |
    | fs.s3a.connection.maximum | COS 的最大连接数， 默认值为 500。如果查询过程中有报错 `Timeout waiting for connection from poll`，可适当调高该连接数。 |

3. 在每个 BE 的 **$BE_HOME/conf/be.conf** 文件中添加如下配置项。

      | **配置项**                       | **说明**                                                     |
      | -------------------------------- | ------------------------------------------------------------ |
      | object_storage_access_key_id     | COS 永久密钥的 SecretId，取值和 `fs.s3a.access.key` 相同。     |
      | object_storage_secret_access_key | COS 永久密钥的 SecretKey，取值和 `fs.s3a.secret.key` 相同。   |
      | object_storage_endpoint          | COS 存储桶所在地域对应的 endpoint，取值和 `fs.s3a.endpoint` 相同。 |
      | object_storage_region            | COS 存储桶所在的地域简称。详细说明，请参见[地域和访问域名](https://cloud.tencent.com/document/product/436/6224)。 |

4. 重启所有 FE 和 BE。

### 阿里云对象存储 OSS

如使用 OSS 作为存储系统，则需要在 StarRocks 中做如下配置。

1. 在每个 FE 的 **$FE_HOME/conf/core-site.xml** 文件中添加如下配置。

    ```XML
    <configuration>
        <property>
            <name>fs.oss.impl</name>
            <value>org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem</value>
        </property>
        <property>
            <name>fs.AbstractFileSystem.oss.impl</name>
            <value>com.aliyun.emr.fs.oss.OSS</value>
        </property>
        <property>
            <name>fs.oss.accessKeyId</name>
            <value>*****</value>
        </property>
        <property>
            <name>fs.oss.accessKeySecret</name>
            <value>*****</value>
        </property>
        <property>
            <name>fs.oss.endpoint</name>
            <value>*****</value>
        </property>
    </configuration>
    ```

    配置项说明：

    | **配置项**             | **说明**                                                     |
    | ---------------------- | ------------------------------------------------------------ |
    | fs.oss.accessKeyId     | 阿里云账号或 RAM 用户的 AccessKey ID。获取方式，请参见 [AccessKey](https://www.alibabacloud.com/help/zh/object-storage-service/latest/developer-guide-terms#section-u3j-nmt-tdb)。 |
    | fs.oss.accessKeySecret | 阿里云账号或 RAM 用户的 AccessKey Secret。获取方式，请参见 [AccessKey](https://www.alibabacloud.com/help/zh/object-storage-service/latest/developer-guide-terms#section-u3j-nmt-tdb)。 |
    | fs.oss.endpoint        | OSS bucket 所在地域对应的外网 endpoint。 您可以通过以下方式查询 endpoint：根据 endpoint 与地域的对应关系进行查找，详情参见[访问域名和数据中心](https://help.aliyun.com/document_detail/31837.htm#concept-zt4-cvy-5db)。登录 [OSS 管理控制台](https://oss.console.aliyun.com/index?spm=a2c4g.11186623.0.0.11d24772leoEEg#/)，并进入 bucket 概览页。一个 bucket 域名的后缀部分即为该 bucket 的外网 endpoint。例如，一个 bucket 域名为 examplebucket.oss-cn-hangzhou.aliyuncs.com，那么 oss-cn-hangzhou.aliyuncs.com 即为该 bucket 的外网 endpoint。 |

2. 在每个 BE 的 **$BE_HOME/conf/be.conf** 中添加如下配置。

      | **配置项**                       | **说明**                                                     |
      | -------------------------------- | ------------------------------------------------------------ |
      | object_storage_access_key_id     | 阿里云账号或 RAM 用户的 AccessKey ID，取值和 `fs.oss.accessKeyId` 相同。 |
      | object_storage_secret_access_key | 阿里云账号或 RAM 用户的 AccessKey Secret，取值和 `fs.oss.accessKeySecret` 相同。 |
      | object_storage_endpoint          | OSS bucket 所在地域对应的外网 endpoint，取值和 `fs.oss.endpoint` 相同。 |

3. 重启所有 FE 和 BE。

## 创建 Hive catalog

以上相关配置完成后，即可创建 Hive catalog，语法如下。

```SQL
CREATE EXTERNAL CATALOG catalog_name 
PROPERTIES ("key"="value", ...);
```

参数说明：

- `catalog_name`：Hive catalog 的名称，必选参数。<br />命名要求如下：
  - 必须由字母(a-z或A-Z)、数字(0-9)或下划线(_)组成，且只能以字母开头。
  - 总长度不能超过 64 个字符。

- `PROPERTIES`：Hive catalog 的属性，必选参数。<br />支持配置如下：

    | **属性**            | **必选** | **说明**                                                     |
    | ------------------- | -------- | ------------------------------------------------------------ |
    | type                | 是       | 数据源类型，取值为 `hive`。                                   |
    | hive.metastore.uris | 是       | Hive metastore 的 URI。格式为 `thrift://<Hive metastore的IP地址>:<端口号>`，端口号默认为 9083。 |

> **注意**
>
> 查询前，需要将 Hive metastore 节点域名和其 IP 的映射关系配置到 **/etc/hosts** 路径中，否则查询时可能会因为域名无法识别而访问失败。

## 使用 Catalog 查询 Hive 数据

创建完 Hive catalog 后即可查询 Hive 集群中的数据。详细信息，请参见[查询外部数据](../catalog/query_external_data.md)。

## 元数据同步

StarRocks 需要利用 Hive 表的元数据来进行查询规划，因此请求访问 Hive 元数据服务的时间直接影响了查询所消耗的时间。为了降低这种影响，StarRocks 提供了元数据同步功能，即将 Hive 表元数据（包括分区统计信息和分区的数据文件信息）缓存在 StarRocks 中并维护更新。当前支持的同步方式有两种：

- 异步更新：StarRocks 默认的元数据同步方式，无需做额外的配置即可使用。异步更新需要满足两个条件才可以自动触发更新（具体见原理），这意味着缓存的元数据不会一直维持在最新的状态，在部分场景下，需要手动进行更新。
- 自动增量更新：如开启该同步方式，则需要分别在 Hive metastore 和 StarRocks 中进行相应的配置。开启后， StarRocks 内缓存的元数据会一直在维持最新的状态，无需进行手动更新。

### 元数据异步更新

#### 原理

如查询命中 Hive 表的某个分区，StarRocks 会自动异步缓存该分区的元数据。缓存的元数据采用的是“懒更新策略”，即如果查询命中该分区，且距离上一次更新已经超过默认间隔时间，那么 StarRocks 会异步更新缓存分区元数据，否则不会更新。更新的默认间隔时间由 `hive_meta_cache_refresh_interval_s` 参数控制，默认值为 `7200`，单位：秒。您可在每个 FE 的 **fe.conf** 文件中设置该参数，设置后重启各个 FE 生效。

如超过默认间隔时间，该分区元数据依旧没有更新，则默认缓存的分区元数据失效。在下次查询时，会重新缓存该分区元数据。元数据缓存失效的时间由 `hive_meta_cache_ttl_s` 参数控制，默认值为 `86400`，单位：秒。您可在每个 FE 的 **fe.conf** 文件中设置该参数，设置后重启各个 FE 生效。

#### 示例

有一张 Hive 表 `table1`，其包含 4 个分区：`p1`、`p2`、`p3` 和 `p4`。如查询命中分区 `p1`，那么 StarRocks 会自动异步缓存 `p1` 的元数据。如维护更新的间隔时间为 1 小时，则后续更新有以下几种情况：

- 如查询命中 `p1`，且当前时间距离上一次更新超过 1 小时，StarRocks 会异步更新缓存的 `p1` 元数据。
- 如查询命中 `p1`，且当前时间距离上一次更新没有超过 1 小时，StarRocks 不会异步更新缓存的 `p1` 元数据。

#### 手动更新

要查询最新的 Hive 数据，需保证 StarRocks 缓存的 Hive 元数据也更至最新。如当前时间距离上一次更新还没有超过默认间隔时间，则可手动更新元数据后再进行查询，具体如下：

- 若 Hive 表结构发生变更（例如增减分区或增减列），可执行如下语句将该变更同步到 StarRocks 中。

    ```SQL
    REFRESH EXTERNAL TABLE [external_catalog.][db_name.]table_name;
    ```

- 若 Hive 表中的某些分区发生数据更新（例如数据导入），可执行如下语句将该变更同步到 StarRocks 中。

    ```SQL
    REFRESH EXTERNAL TABLE [external_catalog.][db_name.]table_name
    [PARTITION ('partition_name', ...)];
    ```

有关 REFRESH EXTERNAL TABEL 语句的参数说明和示例，请参见 [REFRESH EXTERNAL TABEL](/sql-reference/sql-statements/data-definition/REFRESH_EXTERNAL_TABLE.md)。

### 元数据自动增量更新

元数据自动增量更新即通过让 StarRocks 的 FE 节点定时读取 Hive metastore 的 event 来感知 Hive 表元数据的变更情况（例如增减分区、增减列或分区内数据变更），并自动更新 StarRocks 内缓存的 Hive 表元数据。开启该机制的操作步骤如下。

> 注意：开启该机制后，异步更新机制将不再生效。

#### 步骤一：为 Hive metastore 配置 event listener

Event listener 可以对 Hive metastore 中的 event（例如增减分区、增减列或数据更新）进行监听。当前，Hive metastore 2.x 和 3.x 版本均支持配置 event listener，以下为针对 Hive Metastore 3.1.2 版本的配置。将如下配置添加到 **$HiveMetastore/conf/hive-site.xml** 文件中，并重启 Hive metastore。

```XML
<property>
    <name>hive.metastore.event.db.notification.api.auth</name>
    <value>false</value>
</property>
<property>
    <name>hive.metastore.notifications.add.thrift.objects</name>
    <value>true</value>
</property>
<property>
    <name>hive.metastore.alter.notifications.basic</name>
    <value>false</value>
</property>
<property>
    <name>hive.metastore.dml.events</name>
    <value>true</value>
</property>
<property>
    <name>hive.metastore.transactional.event.listeners</name>
    <value>org.apache.hive.hcatalog.listener.DbNotificationListener</value>
</property>
<property>
    <name>hive.metastore.event.db.listener.timetolive</name>
    <value>172800s</value>
</property>
<property>
    <name>hive.metastore.server.max.message.size</name>
    <value>858993459</value>
</property>
```

您可以在 FE 日志中搜索 `event id` 来查看 event listener 是否配置成功。如未成功，则 `event id` 为 `0`。

#### 步骤二：StarRocks 开启自动增量元数据更新

在每个 FE 的 **$FE_HOME/conf/fe.conf** 中配置如下参数以读取 event。

| **参数**                           | **说明**                                                    |
| ---------------------------------- | ----------------------------------------------------------- |
| enable_hms_events_incremental_sync | 是否开启元数据自动增量同步功能，取值包括：<ul><li>`TRUE`：表示开启，为默认值。</li> <li>`FALSE`：表示未开启。</li></ul>|
| hms_events_polling_interval_ms     | StarRocks 读取 event 的间隔时间，默认值为 `5000`，单位：毫秒。    |
| hms_events_batch_size_per_rpc      | StarRocks 每次读取 event 的最大数量，默认值为 `500`。        |
| enable_hms_parallel_process_evens  | 是否并行处理读取的 event ，取值包括：<ul><li>`TRUE`：表示并行处理，为默认值。</li><li>`FALSE`：表示不并行处理。</li></ul>  |
| hms_process_events_parallel_num    | 每次处理 event 的并发数，默认值为 `4`。                      |

## 相关操作

- 如要查看有关创建 external catalog 的示例， 请参见 [CREATE EXTERNAL CATALOG](/sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md)。
- 如要看查看当前集群中的所有 catalog， 请参见 [SHOW CATALOGS](/sql-reference/sql-statements/data-manipulation/SHOW_CATALOGS.md)。
- 如要删除指定 external catalog， 请参见 [DROP CATALOG](/sql-reference/sql-statements/data-definition/DROP_CATALOG.md)。
