---
displayed_sidebar: docs
toc_max_heading_level: 5
keywords: ['iceberg']
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import QSTip from '../../../_assets/commonMarkdown/quickstart-iceberg-tip.mdx'
import IcebergCatalogIcebergRestSecurityLink from '../../../_assets/commonMarkdown/iceberg_catalog_iceberg_rest_security_link.mdx'

# Iceberg catalog

<QSTip />

Iceberg catalog 是一种外部 catalog，从 StarRocks v2.4 开始支持。使用 Iceberg catalog，您可以：

- 直接查询存储在 Iceberg 中的数据，无需手动创建表。
- 使用 [INSERT INTO](../../../sql-reference/sql-statements/loading_unloading/INSERT.md) 或异步物化视图（从 v2.5 开始支持）来处理存储在 Iceberg 中的数据，并将数据导入到 StarRocks 中。
- 在 StarRocks 上执行操作以创建或删除 Iceberg 数据库和表，或使用 [INSERT INTO](../../../sql-reference/sql-statements/loading_unloading/INSERT.md) 将 StarRocks 表中的数据下沉到 Parquet 格式的 Iceberg 表中（此功能从 v3.1 开始支持）。

为了确保在 Iceberg 集群上的 SQL 工作负载成功，您的 StarRocks 集群必须能够访问 Iceberg 集群的存储系统和元存储。StarRocks 支持以下存储系统和元存储：

- 分布式文件系统（HDFS）或对象存储，如 AWS S3、Microsoft Azure Storage、Google GCS 或其他 S3 兼容的存储系统（例如 MinIO）

- 元存储如 Hive metastore、AWS Glue 或 Tabular

:::note

- 如果选择 AWS S3 作为存储，可以使用 HMS 或 AWS Glue 作为元存储。如果选择其他存储系统，则只能使用 HMS 作为元存储。
- 如果选择 Tabular 作为元存储，则需要使用 Iceberg REST catalog。

:::

## 使用注意事项

使用 StarRocks 查询 Iceberg 数据时，请注意以下几点：

| **文件格式** | **压缩格式**                                   | **Iceberg 表版本**                                           |
| --------------- | ---------------------------------------------- | ------------------------------------------------------------ |
| Parquet         | SNAPPY, LZ4, ZSTD, GZIP, 和 NO_COMPRESSION      | <ul><li>v1 表：支持。 </li><li>v2 表：从 StarRocks v3.1 开始支持，其中对这些 v2 表的查询支持位置删除。在 v3.1.10、v3.2.5、v3.3 及其更高版本中，对 v2 表的查询还支持等值删除。 </li></ul> |
| ORC             | ZLIB, SNAPPY, LZO, LZ4, ZSTD, 和 NO_COMPRESSION | <ul><li>v1 表：支持。 </li><li>v2 表：从 StarRocks v3.0 开始支持，其中对这些 v2 表的查询支持位置删除。在 v3.1.8、v3.2.3、v3.3 及其更高版本中，对 v2 表的查询还支持等值删除。 </li></ul> |

## 集成准备

在创建 Iceberg catalog 之前，请确保您的 StarRocks 集群可以与 Iceberg 集群的存储系统和元存储集成。

---

### 存储

选择与您的存储类型匹配的选项卡：

<Tabs groupId="storage">
<TabItem value="AWS" label="AWS S3" default>

如果您的 Iceberg 集群使用 AWS S3 作为存储或 AWS Glue 作为元存储，请选择合适的身份验证方法并进行必要的准备，以确保您的 StarRocks 集群可以访问相关的 AWS 云资源。

推荐以下身份验证方法：

- 实例配置文件
- 假设角色
- IAM 用户

在上述三种身份验证方法中，实例配置文件是最广泛使用的。

有关更多信息，请参见 [AWS IAM 中的身份验证准备](../../../integrations/authenticate_to_aws_resources.md#preparations)。

</TabItem>

<TabItem value="HDFS" label="HDFS" >

如果选择 HDFS 作为存储，请按以下步骤配置您的 StarRocks 集群：

- （可选）设置用于访问 HDFS 集群和 Hive metastore 的用户名。默认情况下，StarRocks 使用 FE 和 BE 或 CN 进程的用户名来访问 HDFS 集群和 Hive metastore。您还可以通过在每个 FE 的 **fe/conf/hadoop_env.sh** 文件的开头以及每个 BE 的 **be/conf/hadoop_env.sh** 文件或每个 CN 的 **cn/conf/hadoop_env.sh** 文件的开头添加 `export HADOOP_USER_NAME="<user_name>"` 来设置用户名。设置完这些文件中的用户名后，重启每个 FE 和每个 BE 或 CN 以使参数设置生效。每个 StarRocks 集群只能设置一个用户名。
- 当您查询 Iceberg 数据时，StarRocks 集群的 FEs 和 BEs 或 CNs 使用 HDFS 客户端访问您的 HDFS 集群。在大多数情况下，您不需要配置 StarRocks 集群即可实现此目的，StarRocks 使用默认配置启动 HDFS 客户端。您只需在以下情况下配置 StarRocks 集群：

  - 您的 HDFS 集群启用了高可用性（HA）：将 HDFS 集群的 **hdfs-site.xml** 文件添加到每个 FE 的 **$FE_HOME/conf** 路径以及每个 BE 的 **$BE_HOME/conf** 路径或每个 CN 的 **$CN_HOME/conf** 路径。
  - 您的 HDFS 集群启用了文件系统视图（ViewFs）：将 HDFS 集群的 **core-site.xml** 文件添加到每个 FE 的 **$FE_HOME/conf** 路径以及每个 BE 的 **$BE_HOME/conf** 路径或每个 CN 的 **$CN_HOME/conf** 路径。

:::tip

如果在发送查询时返回未知主机错误，您必须将 HDFS 集群节点的主机名和 IP 地址映射添加到 **/etc/hosts** 路径。

:::

---

#### Kerberos 身份验证

如果您的 HDFS 集群或 Hive metastore 启用了 Kerberos 身份验证，请按以下步骤配置您的 StarRocks 集群：

- 在每个 FE 和每个 BE 或 CN 上运行 `kinit -kt keytab_path principal` 命令，从密钥分发中心（KDC）获取票证授予票证（TGT）。要运行此命令，您必须具有访问 HDFS 集群和 Hive metastore 的权限。请注意，使用此命令访问 KDC 是时间敏感的。因此，您需要使用 cron 定期运行此命令。
- 将 `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` 添加到每个 FE 的 **$FE_HOME/conf/fe.conf** 文件以及每个 BE 的 **$BE_HOME/conf/be.conf** 文件或每个 CN 的 **$CN_HOME/conf/cn.conf** 文件中。在此示例中，`/etc/krb5.conf` 是 **krb5.conf** 文件的保存路径。您可以根据需要修改路径。

</TabItem>

</Tabs>

---

## 创建 Iceberg catalog

### 语法

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "iceberg",
    [SecurityParams],
    MetastoreParams,
    StorageCredentialParams,
    MetadataRelatedParams
)
```

---

### 参数

#### catalog_name

Iceberg catalog 的名称。命名约定如下：

- 名称可以包含字母、数字（0-9）和下划线（_）。必须以字母开头。
- 名称区分大小写，长度不能超过 1023 个字符。

#### comment

Iceberg catalog 的描述。此参数是可选的。

#### type

数据源的类型。将值设置为 `iceberg`。

#### SecurityParams

关于 StarRocks 如何管理 Catalog 数据访问的参数。

<IcebergCatalogIcebergRestSecurityLink />

##### catalog.access.control

数据访问控制策略。有效值：

- `native`（默认）：使用 StarRocks 内置的数据访问控制系统。
- `allowall`：所有数据访问检查均委托给 Catalog 本身处理。
- `ranger`：数据访问检查委托给 Apache Ranger 处理。

#### MetastoreParams

关于 StarRocks 如何与数据源的元存储集成的一组参数。选择与您的元存储类型匹配的选项卡：

<Tabs groupId="metastore">
<TabItem value="HIVE" label="Hive metastore" default>

##### Hive metastore

如果选择 Hive metastore 作为数据源的元存储，请按以下方式配置 `MetastoreParams`：

```SQL
"iceberg.catalog.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

:::note

在查询 Iceberg 数据之前，必须将 Hive metastore 节点的主机名和 IP 地址映射添加到 `/etc/hosts` 路径。否则，StarRocks 在启动查询时可能无法访问 Hive metastore。

:::

下表描述了您需要在 `MetastoreParams` 中配置的参数。

##### iceberg.catalog.type

必需：是
描述：您用于 Iceberg 集群的元存储类型。将值设置为 `hive`。

##### hive.metastore.uris

必需：是
描述：Hive metastore 的 URI。格式：`thrift://<metastore_IP_address>:<metastore_port>`。<br />如果 Hive metastore 启用了高可用性（HA），您可以指定多个 metastore URI，并用逗号（`,`）分隔，例如 `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。

</TabItem>
<TabItem value="GLUE" label="AWS Glue">

##### AWS Glue

如果选择 AWS Glue 作为数据源的元存储，仅在选择 AWS S3 作为存储时支持，请采取以下措施之一：

- 要选择基于实例配置文件的身份验证方法，请按以下方式配置 `MetastoreParams`：

  ```SQL
  "iceberg.catalog.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- 要选择基于假设角色的身份验证方法，请按以下方式配置 `MetastoreParams`：

  ```SQL
  "iceberg.catalog.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.iam_role_arn" = "<iam_role_arn>",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- 要选择基于 IAM 用户的身份验证方法，请按以下方式配置 `MetastoreParams`：

  ```SQL
  "iceberg.catalog.type" = "glue",
  "aws.glue.use_instance_profile" = "false",
  "aws.glue.access_key" = "<iam_user_access_key>",
  "aws.glue.secret_key" = "<iam_user_secret_key>",
  "aws.glue.region" = "<aws_s3_region>"
  ```

AWS Glue 的 `MetastoreParams`：

###### iceberg.catalog.type

必需：是
描述：您用于 Iceberg 集群的元存储类型。将值设置为 `glue`。

###### aws.glue.use_instance_profile

必需：是
描述：指定是否启用基于实例配置文件的身份验证方法和基于假设角色的身份验证方法。有效值：`true` 和 `false`。默认值：`false`。

###### aws.glue.iam_role_arn

必需：否
描述：在 AWS Glue Data Catalog 上具有权限的 IAM 角色的 ARN。如果使用基于假设角色的身份验证方法访问 AWS Glue，则必须指定此参数。

###### aws.glue.region

必需：是
描述：您的 AWS Glue Data Catalog 所在的区域。例如：`us-west-1`。

###### aws.glue.access_key

必需：否
描述：您的 AWS IAM 用户的访问密钥。如果使用基于 IAM 用户的身份验证方法访问 AWS Glue，则必须指定此参数。

###### aws.glue.secret_key

必需：否
描述：您的 AWS IAM 用户的秘密密钥。如果使用基于 IAM 用户的身份验证方法访问 AWS Glue，则必须指定此参数。

###### aws.glue.catalog_id

必需：否
描述：要使用的 AWS Glue Data Catalog 的 ID。未指定时，使用当前 AWS 账户的 Data Catalog。当需要访问其他 AWS 账户中的 Glue Data Catalog（跨账户访问）时，必须指定此参数。

有关如何选择访问 AWS Glue 的身份验证方法以及如何在 AWS IAM 控制台中配置访问控制策略的信息，请参见 [访问 AWS Glue 的身份验证参数](../../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue)。

</TabItem>
<TabItem value="REST" label="REST">

##### REST

:::note
有关为 S3 表创建 Iceberg REST catalog 的详细说明，请参见 [为 AWS S3 表创建 Iceberg REST Catalog](./iceberg_rest_s3.md)。
:::

如果使用 REST 作为元存储，必须将元存储类型指定为 REST（`"iceberg.catalog.type" = "rest"`）。按以下方式配置 `MetastoreParams`：

```SQL
"iceberg.catalog.type" = "rest",
"iceberg.catalog.uri" = "<rest_server_api_endpoint>",
"iceberg.catalog.security" = "oauth2",
"iceberg.catalog.oauth2.credential" = "<credential>",
"iceberg.catalog.warehouse" = "<identifier_or_path_to_warehouse>"
```

REST catalog 的 `MetastoreParams`：

###### iceberg.catalog.type

必需：是
描述：您用于 Iceberg 集群的元存储类型。将值设置为 `rest`。

###### iceberg.catalog.uri

必需：是
描述：REST 服务端点的 URI。例如：`https://api.tabular.io/ws`。

###### iceberg.catalog.view-endpoints-supported

必需：否

描述：是否使用视图端点支持与视图相关的操作，当早期版本的 REST 服务不在 `CatalogConfig` 中返回端点时使用。此参数用于与早期版本的 REST 服务器的向后兼容。默认值：`false`。

###### iceberg.catalog.security

必需：否

描述：要使用的授权协议类型。默认值：`NONE`。有效值：`OAUTH2` 和 `JWT`。当设置为 `OAUTH2` 时，需要指定 `iceberg.catalog.oauth2.token` 或 `iceberg.catalog.oauth2.credential`。当设置为 `JWT` 时，可省略  `token` 或 `credential`，需要使用 `JWT` 方式登录 StarRocks 集群。StarRocks 会使用登陆用户的 JWT 令牌访问 Catalog。

###### iceberg.catalog.oauth2.token

必需：否

描述：用于与服务器交互的承载令牌。`OAUTH2` 授权协议需要 `token` 或 `credential`。例如：`AbCdEf123456`。

###### iceberg.catalog.oauth2.credential

必需：否

描述：用于在与服务器的 OAuth2 客户端凭证流中交换令牌的凭证。`OAUTH2` 授权协议需要 `token` 或 `credential`。例如：`AbCdEf123456`。

###### iceberg.catalog.oauth2.scope

必需：否

描述：与 REST Catalog 通信时使用的范围。仅在使用 `credential` 时适用。

###### iceberg.catalog.oauth2.server-uri

必需：否

描述：从 OAuth2 服务器检索访问令牌的端点。

###### iceberg.catalog.vended-credentials-enabled

必需：否

描述：是否使用 REST 后端提供的凭证进行文件系统访问。默认值：`true`。

###### iceberg.catalog.warehouse

必需：否
描述：Iceberg catalog 的仓库位置或标识符。例如：`s3://my_bucket/warehouse_location` 或 `sandbox`。

##### iceberg.catalog.rest.nested-namespace-enabled

必需：否

描述：是否支持查询嵌套命名空间下的对象。默认值：`false`。

##### iceberg.catalog.rest.view-endpoints-enabled

必需：否

描述：是否启用视图端点以支持视图相关操作。如果设置为 `false`，将禁用视图操作（如 `getView`）。默认值：`true`。

以下示例创建了一个名为 `tabular` 的 Iceberg catalog，使用 Tabular 作为元存储：

```SQL
CREATE EXTERNAL CATALOG tabular
PROPERTIES
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.uri" = "https://api.tabular.io/ws",
    "iceberg.catalog.oauth2.credential" = "t-5Ii8e3FIbT9m0:aaaa-3bbbbbbbbbbbbbbbbbbb",
    "iceberg.catalog.warehouse" = "sandbox"
);
```
以下示例创建了一个名为 `smith_polaris` 的 Iceberg catalog，使用 Polaris 作为元存储：

```sql
CREATE EXTERNAL CATALOG smith_polaris 
PROPERTIES (   
    "iceberg.catalog.uri"  = "http://xxx.xx.xx.xxx:8181/api/catalog", 
    "type"  =  "iceberg",   
    "iceberg.catalog.type"  =  "rest",   
    "iceberg.catalog.warehouse" = "starrocks_catalog",
    "iceberg.catalog.security" = "oauth2",
    "iceberg.catalog.oauth2.credential" = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "iceberg.catalog.oauth2.scope"='PRINCIPAL_ROLE:ALL'
 );

# `ns1.ns2.tpch_namespace` 是一个嵌套命名空间
create table smith_polaris.`ns1.ns2.tpch_namespace`.tbl (c1 string);

mysql> select * from smith_polaris.`ns1.ns2.tpch_namespace`.tbl;
+------+
| c1   |
+------+
| 1    |
| 2    |
| 3    |
+------+
3 rows in set (0.34 sec)
```

以下示例创建了一个名为 `r2` 的 Iceberg catalog，使用 Cloudflare R2 Data Catalog 作为元存储：

```SQL
CREATE EXTERNAL CATALOG r2
PROPERTIES
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.uri" = "<r2_catalog_uri>",
    "iceberg.catalog.security" = "oauth2",
    "iceberg.catalog.oauth2.token" = "<r2_api_token>",
    "iceberg.catalog.warehouse" = "<r2_warehouse_name>"
);

SET CATALOG r2;

CREATE DATABASE testdb;

SHOW DATABASES FROM r2;

+--------------------+
| Database           |
+--------------------+
| information_schema |
| testdb             |
+--------------------+
2 rows in set (0.66 sec)
```

`<r2_warehouse_name>`、`<r2_api_token>` 和 `<r2_catalog_uri>` 的值可以从 [Cloudflare Dashboard 中获取，详细信息请参见此处](https://developers.cloudflare.com/r2/data-catalog/get-started/#prerequisites)。

</TabItem>

<TabItem value="JDBC" label="JDBC">

##### JDBC

如果选择 JDBC 作为数据源的元存储，请按以下方式配置 `MetastoreParams`：

```SQL
"iceberg.catalog.type" = "jdbc",
"iceberg.catalog.uri" = "<jdbc_uri>",
"iceberg.catalog.warehouse" = "<warehouse_location>"
```

下表描述了您需要在 `MetastoreParams` 中配置的参数。

##### iceberg.catalog.type

必需：是

描述：您用于 Iceberg 集群的元存储类型。将值设置为 `jdbc`。

##### iceberg.catalog.uri

必需：是

描述：您的数据库的 URI。格式：`jdbc:[mysql\|postgresql]://<DB_IP_address>:<DB_PORT>/<DB_NAME>`。

###### iceberg.catalog.warehouse

必需：是

描述：Iceberg catalog 的仓库位置或标识符。例如：`s3://my_bucket/warehouse_location`。

##### iceberg.catalog.jdbc.user

必需：否

描述：数据库的用户名。

##### iceberg.catalog.jdbc.password

必需：否

描述：数据库的密码。

##### iceberg.catalog.jdbc.init-catalog-tables

必需：否

描述：是否在 `iceberg.catalog.uri` 指定的数据库中创建用于存储元数据的表 `iceberg_namespace_properties` 和 `iceberg_tables`，默认值为`false`。当`iceberg.catalog.uri` 指定的数据库中尚未创建上述两张表时需要指定为`true`。

以下示例创建了一个名为 `iceberg_jdbc` 的 Iceberg catalog，并使用 JDBC 作为元存储：

```SQL
CREATE EXTERNAL CATALOG iceberg_jdbc
PROPERTIES
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "jdbc",
    "iceberg.catalog.warehouse" = "s3://my_bucket/warehouse_location",
    "iceberg.catalog.uri" = "jdbc:mysql://ip:port/db_name",
    "iceberg.catalog.jdbc.user" = "username",
    "iceberg.catalog.jdbc.password" = "password",
    "aws.s3.endpoint" = "<s3_endpoint>",
    "aws.s3.access_key" = "<iam_user_access_key>",
    "aws.s3.secret_key" = "<iam_user_secret_key>"
);
```
若使用MySQL或其他自定义的JDBC驱动程序，需要将相应的JAR包放置于 `fe/lib` 和 `be/lib/jni-packages` 目录下。

</TabItem>

</Tabs>

---

#### `StorageCredentialParams`

关于 StarRocks 如何与您的存储系统集成的一组参数。此参数集是可选的。

注意以下几点：

- 如果您使用 HDFS 作为存储，则无需配置 `StorageCredentialParams`，可以跳过此部分。如果您使用 AWS S3、其他 S3 兼容的存储系统、Microsoft Azure Storage 或 Google GCS 作为存储，则必须配置 `StorageCredentialParams`。

- 如果您使用 Tabular 作为元存储，则无需配置 `StorageCredentialParams`，可以跳过此部分。如果您使用 HMS 或 AWS Glue 作为元存储，则必须配置 `StorageCredentialParams`。

选择与您的存储类型匹配的选项卡：

<Tabs groupId="storage">
<TabItem value="AWS" label="AWS S3" default>

##### AWS S3

如果选择 AWS S3 作为 Iceberg 集群的存储，请采取以下措施之一：

- 要选择基于实例配置文件的身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- 要选择基于假设角色的身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- 要选择基于 IAM 用户的身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- 要选择基于 REST Catalog 的 Vended Credential（自 v4.0 起支持），请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "aws.s3.region" = "<aws_s3_region>"
  ```

AWS S3 的 `StorageCredentialParams`：

###### aws.s3.use_instance_profile

- 必需：是
- 描述：指定是否启用基于实例配置文件的身份验证方法和基于假设角色的身份验证方法。有效值：`true` 和 `false`。默认值：`false`。

###### aws.s3.iam_role_arn

- 必需：否
- 描述：在您的 AWS S3 存储桶上具有权限的 IAM 角色的 ARN。如果使用基于假设角色的身份验证方法访问 AWS S3，则必须指定此参数。

###### aws.s3.region

- 必需：是
- 描述：您的 AWS S3 存储桶所在的区域。例如：`us-west-1`。

###### aws.s3.access_key

- 必需：否
- 描述：您的 IAM 用户的访问密钥。如果使用基于 IAM 用户的身份验证方法访问 AWS S3，则必须指定此参数。

###### aws.s3.secret_key

- 必需：否
- 描述：您的 IAM 用户的秘密密钥。如果使用基于 IAM 用户的身份验证方法访问 AWS S3，则必须指定此参数。

有关如何选择访问 AWS S3 的身份验证方法以及如何在 AWS IAM 控制台中配置访问控制策略的信息，请参见 [访问 AWS S3 的身份验证参数](../../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3)。

</TabItem>

<TabItem value="HDFS" label="HDFS" >

使用 HDFS 存储时跳过存储凭证。

</TabItem>

<TabItem value="MINIO" label="MinIO" >

##### S3 兼容的存储系统

从 v2.5 开始，Iceberg catalog 支持 S3 兼容的存储系统。

如果选择 S3 兼容的存储系统，例如 MinIO，作为 Iceberg 集群的存储，请按以下方式配置 `StorageCredentialParams` 以确保成功集成：

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

MinIO 和其他 S3 兼容系统的 `StorageCredentialParams`：

###### aws.s3.enable_ssl

- 必需：是
- 描述：指定是否启用 SSL 连接。<br />有效值：`true` 和 `false`。默认值：`true`。

###### aws.s3.enable_path_style_access

- 必需：是
- 描述：指定是否启用路径样式访问。<br />有效值：`true` 和 `false`。默认值：`false`。对于 MinIO，您必须将值设置为 `true`。<br />路径样式 URL 使用以下格式：`https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例如，如果您在美国西部（俄勒冈）区域创建了一个名为 `DOC-EXAMPLE-BUCKET1` 的存储桶，并且您想要访问该存储桶中的 `alice.jpg` 对象，可以使用以下路径样式 URL：`https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。

###### aws.s3.endpoint

- 必需：是
- 描述：用于连接到您的 S3 兼容存储系统而不是 AWS S3 的端点。

###### aws.s3.access_key

- 必需：是
- 描述：您的 IAM 用户的访问密钥。

###### aws.s3.secret_key

- 必需：是
- 描述：您的 IAM 用户的秘密密钥。

</TabItem>

<TabItem value="AZURE" label="Microsoft Azure Blob Storage" >

##### Microsoft Azure Storage

从 v3.0 开始，Iceberg catalog 支持 Microsoft Azure Storage。

###### Azure Blob Storage

如果选择 Blob Storage 作为 Iceberg 集群的存储，请采取以下措施之一：

- 要选择共享密钥身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

- 要选择 SAS 令牌身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

- 要选择基于 REST Catalog 的 Vended Credential（自 v4.0 起支持），则无需配置 `StorageCredentialParams`。

Microsoft Azure 的 `StorageCredentialParams`：

###### azure.blob.storage_account

- 必需：是
- 描述：您的 Blob Storage 账户的用户名。

###### azure.blob.shared_key

- 必需：是
- 描述：您的 Blob Storage 账户的共享密钥。

###### azure.blob.account_name

- 必需：是
- 描述：您的 Blob Storage 账户的用户名。

###### azure.blob.container

- 必需：是
- 描述：存储数据的 blob 容器的名称。

###### azure.blob.sas_token

- 必需：是
- 描述：用于访问您的 Blob Storage 账户的 SAS 令牌。

###### Azure Data Lake Storage Gen1

如果选择 Data Lake Storage Gen1 作为 Iceberg 集群的存储，请采取以下措施之一：

- 要选择托管服务身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

或：

- 要选择服务主体身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

###### Azure Data Lake Storage Gen2

如果选择 Data Lake Storage Gen2 作为 Iceberg 集群的存储，请采取以下措施之一：

- 要选择托管身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  或：

- 要选择共享密钥身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  或：

- 要选择服务主体身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

- 要选择基于 REST Catalog 的 Vended Credential（自 v4.0 起支持），则无需配置 `StorageCredentialParams`。

</TabItem>

<TabItem value="GCS" label="Google GCS" >

##### Google GCS

从 v3.0 开始，Iceberg catalog 支持 Google GCS。

如果选择 Google GCS 作为 Iceberg 集群的存储，请采取以下措施之一：

- 要选择基于 VM 的身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

- 要选择基于服务账户的身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

- 要选择基于模拟的身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  - 使 VM 实例模拟服务账户：

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

  - 使服务账户（暂时命名为元服务账户）模拟另一个服务账户（暂时命名为数据服务账户）：

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

- 要选择基于 REST Catalog 的 Vended Credential（自 v4.0 起支持），则无需配置 `StorageCredentialParams`。

Google GCS 的 `StorageCredentialParams`：

###### gcp.gcs.service_account_email

- 默认值： ""
- 示例： "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)"
- 描述：在创建服务账户时生成的 JSON 文件中的电子邮件地址。

###### gcp.gcs.service_account_private_key_id

- 默认值： ""
- 示例： "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"
- 描述：在创建服务账户时生成的 JSON 文件中的私钥 ID。

###### gcp.gcs.service_account_private_key

- 默认值： ""
- 示例： "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  
- 描述：在创建服务账户时生成的 JSON 文件中的私钥。

###### gcp.gcs.impersonation_service_account

- 默认值： ""  
- 示例： "hello"  
- 描述：您要模拟的服务账户。

</TabItem>

</Tabs>

---

#### MetadataRelatedParams

关于 StarRocks Iceberg 元数据缓存的一组参数。此参数集是可选的。

从 v3.3.3 开始，StarRocks 支持 [周期性元数据刷新策略](#附录-a-周期性元数据刷新策略)。在大多数情况下，您可以忽略设置以下参数，不需要调整其中的策略参数，因为这些参数的默认值已经为您提供了开箱即用的性能。您可以使用系统变量 [`plan_mode`](../../../sql-reference/System_variable.md#plan_mode) 调整 Iceberg 元数据解析模式。

| **参数**                                 | **默认值**           | **描述**                                              |
| :-------------------------------------------- | :-------------------- | :----------------------------------------------------------- |
| enable_iceberg_metadata_cache                 | true                  | 是否缓存 Iceberg 相关的元数据，包括表缓存、分区名称缓存以及 Manifest 中的数据文件缓存和删除数据文件缓存。 |
| iceberg_manifest_cache_with_column_statistics | false                 | 是否缓存列的统计信息。                  |
| refresh_iceberg_manifest_min_length           | 2 * 1024 * 1024       | 触发数据文件缓存刷新的最小 Manifest 文件长度。 |
| iceberg_data_file_cache_memory_usage_ratio    | 0.1                   | Data File Manifest 缓存的最大内存使用率。从 v3.5.6 版本开始支持。 |
| iceberg_delete_file_cache_memory_usage_ratio  | 0.1                   | Delete File Manifest 缓存的最大内存使用率。从 v3.5.6 版本开始支持。 |
| iceberg_table_cache_refresh_interval_sec      | 60                    | 触发 Iceberg 表缓存异步刷新操作的时间间隔（单位：秒）。从 v3.5.7 版本开始支持。 |

从 v3.4 起，StarRocks 在没有主动触发收集 Iceberg 表统计信息的情况下，可以通过设置以下参数读取 Iceberg 的元数据来获取 Iceberg 表的统计信息。

| **参数**                                       | **默认值**             | **描述**                       |
| :-------------------------------------------- | :-------------------- | :----------------------------- |
| enable_get_stats_from_external_metadata       | false                 | 是否允许系统从 Iceberg 元数据中获取统计信息。当此项设置为 `true` 时，您可以通过会话变量 [`enable_get_stats_from_external_metadata`](../../../sql-reference/System_variable.md#enable_get_stats_from_external_metadata) 进一步控制要收集的统计信息类型。 |

### 示例

以下示例创建了一个名为 `iceberg_catalog_hms` 或 `iceberg_catalog_glue` 的 Iceberg catalog，具体取决于您使用的元存储类型，以便从您的 Iceberg 集群中查询数据。选择与您的存储类型匹配的选项卡：

<Tabs groupId="storage">
<TabItem value="AWS" label="AWS S3" default>

#### AWS S3

##### 如果选择基于实例配置文件的凭证

- 如果在 Iceberg 集群中使用 Hive metastore，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

- 如果在 Amazon EMR Iceberg 集群中使用 AWS Glue，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_glue
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### 如果选择基于假设角色的凭证

- 如果在 Iceberg 集群中使用 Hive metastore，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

- 如果在 Amazon EMR Iceberg 集群中使用 AWS Glue，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_glue
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.iam_role_arn" = "arn:aws:iam::081976408565:role/test_glue_role",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### 如果选择基于 IAM 用户的凭证

- 如果在 Iceberg 集群中使用 Hive metastore，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_access_key>",
      "aws.s3.region" = "us-west-2"
  );
  ```

- 如果在 Amazon EMR Iceberg 集群中使用 AWS Glue，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_glue
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "glue",
      "aws.glue.use_instance_profile" = "false",
      "aws.glue.access_key" = "<iam_user_access_key>",
      "aws.glue.secret_key" = "<iam_user_secret_key>",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_secret_key>",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### 如果选择 Vended Credential

如果基于 REST Catalog 使用 Vended Credential，请运行如下命令：

```SQL
CREATE EXTERNAL CATALOG polaris_s3
PROPERTIES
(
    "type" = "iceberg",
    "iceberg.catalog.uri" = "http://xxx:xxx/api/catalog",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.rest.nested-namespace-enabled"="true",
    "iceberg.catalog.security" = "oauth2",
    "iceberg.catalog.oauth2.credential" = "xxxxx:xxxx",
    "iceberg.catalog.oauth2.scope"='PRINCIPAL_ROLE:ALL',
    "iceberg.catalog.warehouse" = "iceberg_catalog",
    "aws.s3.region" = "us-west-2"
);
```

</TabItem>

<TabItem value="HDFS" label="HDFS" >

#### HDFS

如果您使用 HDFS 作为存储，请运行如下命令：

```SQL
CREATE EXTERNAL CATALOG iceberg_catalog_hms
PROPERTIES
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
);
```

</TabItem>

<TabItem value="MINIO" label="MinIO" >

#### S3 兼容的存储系统

以 MinIO 为例。运行如下命令：

```SQL
CREATE EXTERNAL CATALOG iceberg_catalog_hms
PROPERTIES
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "aws.s3.enable_ssl" = "true",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.endpoint" = "<s3_endpoint>",
    "aws.s3.access_key" = "<iam_user_access_key>",
    "aws.s3.secret_key" = "<iam_user_secret_key>"
);
```
</TabItem>

<TabItem value="AZURE" label="Microsoft Azure Blob Storage" >

#### Microsoft Azure Storage

##### Azure Blob Storage

- 如果选择共享密钥身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
  );
  ```

- 如果选择 SAS 令牌身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.container" = "<blob_container_name>",
      "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
  );
  ```

- 如果基于 REST Catalog 使用 Vended Credential，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG polaris_azure
  PROPERTIES (   
      "type"  =  "iceberg",   
      "iceberg.catalog.uri"  = "http://xxx:xxx/api/catalog",
      "iceberg.catalog.type"  =  "rest",
      "iceberg.catalog.rest.nested-namespace-enabled"="true", 
      "iceberg.catalog.security" = "oauth2",
      "iceberg.catalog.oauth2.credential" = "xxxxx:xxxx",
      "iceberg.catalog.oauth2.scope"='PRINCIPAL_ROLE:ALL',
      "iceberg.catalog.warehouse" = "iceberg_catalog"
  );
  ```

##### Azure Data Lake Storage Gen1

- 如果选择托管服务身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.use_managed_service_identity" = "true"    
  );
  ```

- 如果选择服务主体身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.oauth2_client_id" = "<application_client_id>",
      "azure.adls1.oauth2_credential" = "<application_client_credential>",
      "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  );
  ```

##### Azure Data Lake Storage Gen2

- 如果选择托管身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_use_managed_identity" = "true",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
  );
  ```

- 如果选择共享密钥身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.storage_account" = "<storage_account_name>",
      "azure.adls2.shared_key" = "<shared_key>"     
  );
  ```

- 如果选择服务主体身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_client_id" = "<service_client_id>",
      "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
      "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  );
  ```

- 如果基于 REST Catalog 使用 Vended Credential，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG polaris_azure
  PROPERTIES (   
      "type"  =  "iceberg",   
      "iceberg.catalog.uri"  = "http://xxx:xxx/api/catalog",
      "iceberg.catalog.type"  =  "rest",
      "iceberg.catalog.rest.nested-namespace-enabled"="true", 
      "iceberg.catalog.security" = "oauth2",
      "iceberg.catalog.oauth2.credential" = "xxxxx:xxxx",
      "iceberg.catalog.oauth2.scope"='PRINCIPAL_ROLE:ALL',
      "iceberg.catalog.warehouse" = "iceberg_catalog"
  );
  ```

</TabItem>

<TabItem value="GCS" label="Google GCS" >

#### Google GCS

- 如果选择基于 VM 的身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.use_compute_engine_service_account" = "true"    
  );
  ```

- 如果选择基于服务账户的身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.service_account_email" = "<google_service_account_email>",
      "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
      "gcp.gcs.service_account_private_key" = "<google_service_private_key>"    
  );
  ```

- 如果选择基于模拟的身份验证方法：

  - 如果您让 VM 实例模拟服务账户，请运行如下命令：

    ```SQL
    CREATE EXTERNAL CATALOG iceberg_catalog_hms
    PROPERTIES
    (
        "type" = "iceberg",
        "iceberg.catalog.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.use_compute_engine_service_account" = "true",
        "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"    
    );
    ```

  - 如果您让服务账户模拟另一个服务账户，请运行如下命令：

    ```SQL
    CREATE EXTERNAL CATALOG iceberg_catalog_hms
    PROPERTIES
    (
        "type" = "iceberg",
        "iceberg.catalog.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.service_account_email" = "<google_service_account_email>",
        "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
        "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
        "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    );
    ```

- 如果基于 REST Catalog 使用 Vended Credential，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG polaris_gcp
  PROPERTIES (   
      "type"  =  "iceberg",   
      "iceberg.catalog.uri"  = "http://xxx:xxx/api/catalog",
      "iceberg.catalog.type"  =  "rest",
      "iceberg.catalog.rest.nested-namespace-enabled"="true", 
      "iceberg.catalog.security" = "oauth2",
      "iceberg.catalog.oauth2.credential" = "xxxxx:xxxx",
      "iceberg.catalog.oauth2.scope"='PRINCIPAL_ROLE:ALL',
      "iceberg.catalog.warehouse" = "iceberg_catalog"
  );
  ```

</TabItem>

</Tabs>

---

## 使用您的 catalog

### 查看 Iceberg catalog

您可以使用 [SHOW CATALOGS](../../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) 查询当前 StarRocks 集群中的所有 catalog：

```SQL
SHOW CATALOGS;
```

您还可以使用 [SHOW CREATE CATALOG](../../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) 查询外部 catalog 的创建语句。以下示例查询名为 `iceberg_catalog_glue` 的 Iceberg catalog 的创建语句：

```SQL
SHOW CREATE CATALOG iceberg_catalog_glue;
```

---

### 切换到 Iceberg Catalog 及其中的数据库

您可以使用以下方法之一切换到 Iceberg catalog 及其中的数据库：

- 使用 [SET CATALOG](../../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) 指定当前会话中的 Iceberg catalog，然后使用 [USE](../../../sql-reference/sql-statements/Database/USE.md) 指定活动数据库：

  ```SQL
  -- 切换到当前会话中的指定 catalog：
  SET CATALOG <catalog_name>
  -- 指定当前会话中的活动数据库：
  USE <db_name>
  ```

- 直接使用 [USE](../../../sql-reference/sql-statements/Database/USE.md) 切换到 Iceberg catalog 及其中的数据库：

  ```SQL
  USE <catalog_name>.<db_name>
  ```

---

### 删除 Iceberg catalog

您可以使用 [DROP CATALOG](../../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) 删除外部 catalog。

以下示例删除名为 `iceberg_catalog_glue` 的 Iceberg catalog：

```SQL
DROP Catalog iceberg_catalog_glue;
```

---

### 查看 Iceberg 表的模式

您可以使用以下语法之一查看 Iceberg 表的模式：

- 查看模式

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- 从 CREATE 语句中查看模式和位置

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

---

### 查询 Iceberg 表

1. 使用 [SHOW DATABASES](../../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) 查看 Iceberg 集群中的数据库：

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. [切换到 Iceberg catalog 及其中的数据库](#switch-to-an-iceberg-catalog-and-a-database-in-it)。

3. 使用 [SELECT](../../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) 查询指定数据库中的目标表：

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

---

### Iceberg DDL 操作

DDL 操作（CREATE/DROP DATABASE、CREATE/DROP TABLE、CREATE/ALTER VIEW），请参见 [Iceberg DDL 操作](./DDL.md)。

---

### Iceberg DML 操作

DML 操作（INSERT），请参见 [Iceberg DML 操作](./DML.md)。

---

### Iceberg Procedures

有关 Iceberg Procedures，请参见 [Iceberg Procedures](./procedures.md)。

---

### 配置元数据缓存

您的 Iceberg 集群的元数据文件可能存储在远端存储中，例如 AWS S3 或 HDFS。默认情况下，StarRocks 将 Iceberg 元数据缓存到内存中。为了加速查询，StarRocks 采用了两级元数据缓存机制，可以在内存和磁盘上缓存元数据。对于每个初始查询，StarRocks 缓存其计算结果。如果发出与先前查询语义等效的任何后续查询，StarRocks 首先尝试从其缓存中检索请求的元数据，只有在缓存中未命中元数据时才从远端存储中检索元数据。

StarRocks 使用最近最少使用（LRU）算法来缓存和驱逐数据。基本规则如下：

- StarRocks 首先尝试从内存中检索请求的元数据。如果内存中未命中元数据，StarRocks 尝试从磁盘中检索元数据。从磁盘中检索到的元数据将加载到内存中。如果磁盘中也未命中元数据，StarRocks 从远端存储中检索元数据并将其缓存到内存中。
- StarRocks 将从内存中驱逐的元数据写入磁盘，但直接丢弃从磁盘中驱逐的元数据。

从 v3.3.3 开始，StarRocks 支持 [周期性元数据刷新策略](#附录-a-周期性元数据刷新策略)。您可以使用系统变量 [`plan_mode`](../../../sql-reference/System_variable.md#plan_mode) 调整 Iceberg 元数据缓存计划。

#### Iceberg 元数据缓存的 FE 配置

##### enable_iceberg_metadata_disk_cache

- 单位：N/A
- 默认值：`false`
- 描述：指定是否启用磁盘缓存。

##### iceberg_metadata_cache_disk_path

- 单位：N/A
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/caches/iceberg"`
- 描述：磁盘上缓存的元数据文件的保存路径。

##### iceberg_metadata_disk_cache_capacity

- 单位：字节
- 默认值：`2147483648`，相当于 2 GB
- 描述：磁盘上允许缓存的元数据的最大大小。

##### iceberg_metadata_memory_cache_capacity

- 单位：字节
- 默认值：`536870912`，相当于 512 MB
- 描述：内存中允许缓存的元数据的最大大小。

##### iceberg_metadata_memory_cache_expiration_seconds

- 单位：秒  
- 默认值：`86500`
- 描述：从上次访问开始，内存中缓存条目过期的时间。

##### iceberg_metadata_disk_cache_expiration_seconds

- 单位：秒  
- 默认值：`604800`，相当于一周
- 描述：从上次访问开始，磁盘上缓存条目过期的时间。

##### iceberg_metadata_cache_max_entry_size

- 单位：字节
- 默认值：`8388608`，相当于 8 MB
- 描述：可以缓存的文件的最大大小。超过此参数值的文件无法缓存。如果查询请求这些文件，StarRocks 将从远端存储中检索它们。

##### enable_background_refresh_connector_metadata

- 单位：-
- 默认值：true
- 描述：是否启用周期性 Iceberg 元数据缓存刷新。启用后，StarRocks 会轮询 Iceberg 集群的元存储（Hive Metastore 或 AWS Glue），并刷新频繁访问的 Iceberg catalog 的缓存元数据以感知数据更改。`true` 表示启用 Iceberg 元数据缓存刷新，`false` 表示禁用。

##### background_refresh_metadata_interval_millis

- 单位：毫秒
- 默认值：600000
- 描述：两次连续 Iceberg 元数据缓存刷新之间的间隔。

##### background_refresh_metadata_time_secs_since_last_access_sec

- 单位：秒
- 默认值：86400
- 描述：Iceberg 元数据缓存刷新任务的过期时间。对于已访问的 Iceberg catalog，如果超过指定时间未访问，StarRocks 将停止刷新其缓存元数据。对于未访问的 Iceberg catalog，StarRocks 将不刷新其缓存元数据。

## 附录 A：周期性元数据刷新策略

Iceberg 支持 [Snapshot](./iceberg_timetravel.md)。有了最新 Snapshot，就能得到最新结果。因此，只有缓存 Snapshot 能影响数据的新鲜度。您只需注意包含快照的缓存的刷新策略即可。

下面的流程图在时间轴上显示了时间间隔。

![Timeline for updating and discarding cached metadata](../../../_assets/iceberg_catalog_timeline_zh.png)

## 附录 B：元数据文件解析

- **大规模元数据的分布式计划**

  为了有效处理大规模元数据，StarRocks 使用多个 BE 和 CN 节点采用分布式方法。此方法利用现代查询引擎的并行计算能力，可以将读取、解压缩和过滤清单文件等任务分配到多个节点。通过并行处理这些清单文件，显著减少了元数据检索所需的时间，从而加快了作业计划。对于涉及大量清单文件的大型查询，这尤其有益，因为它消除了单点瓶颈并提高了整体查询执行效率。

- **小规模元数据的本地计划**

  对于较小的查询，其中清单文件的重复解压缩和解析可能会引入不必要的延迟，采用了不同的策略。StarRocks 缓存反序列化的内存对象，特别是 Avro 文件，以解决此问题。通过将这些反序列化的文件存储在内存中，系统可以绕过后续查询的解压缩和解析阶段。此缓存机制允许直接访问所需的元数据，显著减少了检索时间。因此，系统变得更加响应，并更适合满足高查询需求和物化视图重写需求。

- **自适应元数据检索策略**（默认）

  StarRocks 旨在根据各种因素自动选择适当的元数据检索方法，包括 FE 和 BE/CN 节点的数量、其 CPU 核心数以及当前查询所需的清单文件数量。此自适应方法确保系统动态优化元数据检索，而无需手动调整与元数据相关的参数。通过这样做，StarRocks 提供了无缝体验，在分布式和本地计划之间取得平衡，以在不同条件下实现最佳查询性能。

您可以使用系统变量 [`plan_mode`](../../../sql-reference/System_variable.md#plan_mode) 调整 Iceberg 元数据缓存计划。
