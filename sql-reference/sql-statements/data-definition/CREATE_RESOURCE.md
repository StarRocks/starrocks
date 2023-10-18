# CREATE RESOURCE

## 功能

创建资源。StarRocks 支持创建以下资源：Apache Spark™、Apache Hive™、Apache Iceberg、 Apache Hudi 和 JDBC。其中 Spark 资源用于 [Spark Load](/loading/SparkLoad.md)，负责管理数据导入的相关信息，比如 YARN 配置，中间数据存储的路径以及 Broker 配置等；Hive、Iceberg、Hudi 和 JDBC 资源用于在查询[外部表](../../../data_source/External_table.md)是管理数据源的访问信息。

> 说明：
>
> - 仅 root 和 admin 用户可以创建资源。
> - 仅 StarRocks 2.3 及以上版本支持创建 JDBC 资源。

## 语法

```SQL
CREATE EXTERNAL RESOURCE "resource_name"
PROPERTIES ("key"="value"[, ...])
```

## 参数说明

### resource_name

资源名称。命名要求如下：

- 必须由数字(0-9)、下划线(_)或字母(a-z或A-Z)组成，且只能以字母开头。
- 总长度不能超过 64 个字符。

### PROPERTIES

资源配置项，不同类型的资源可设置不同配置项。

#### Spark 资源

Spark 集群配置不同，资源需要添加的配置项也不同。当前 Spark Load 仅支持 Spark 的 cluster manager 为 YARN 且数据存储系统为 HDFS，且 YARN 和 HDFS 均支持 HA（高可用）。具体可分为以下几种情况：

- 如果选择使用Broker 进程的方式导入

  - 如果 Spark 的 cluster manager 为 YARN，数据存储系统为 HDFS，则需添加如下配置项：

    | **配置项**                                | **必选** | **说明**                                                     |
    | ----------------------------------------- | -------- | ------------------------------------------------------------ |
    | type                                      | 是       | 资源类型，取值为 `spark`。                                   |
    | spark.master                              | 是       | Spark 的 cluster manager。当前仅支持 YARN，所以取值为 `yarn`。 |
    | spark.submit.deployMode                   | 是       | Spark driver 的部署模式。取值包括`cluster`和`client`。关于取值说明，参考 [Launching Spark on YARN](https://spark.apache.org/docs/3.3.0/running-on-yarn.html#launching-spark-on-yarn)。 |
    | spark.executor.memory                     | 否       | Spark executor 占用的内存量，单位为 KB、MB、GB 或 TB。       |
    | spark.yarn.queue                          | 否       | YARN 队列名称。                                              |
    | spark.hadoop.yarn.resourcemanager.address | 是       | YARN ResourceManager 地址。                                  |
    | spark.hadoop.fs.defaultFS                 | 是       | HDFS 中 NameNode 的地址。格式为：`hdfs://namenode_host:port`。 |
    | working_dir                               | 是       | 一个 HDFS 文件路径，用于存放 ETL 作业生成的文件。例如：`hdfs://host: port/tmp/starrocks`。 |
    | broker                                    | 是       | Broker 名称。您可以使用 [SHOW BROKER](/sql-reference/sql-statements/Administration/SHOW_BROKER.md) 语句查看当前所有 Broker 的名称。如未添加过 Broker，可使用 [ALTER SYSTEM](/sql-reference/sql-statements/Administration/ALTER_SYSTEM.md) 语句添加 Broker。 |
    | broker.username                           | 否       | 通过指定的 HDFS 用户去访问HDFS中的文件。如果 HDFS 文件只能由特定用户访问，则需要传入该参数，如果该文件是所有用户都可以访问的话，则不需要传入该参数。 |
    | broker.password                           | 否       | HDFS 用户密码。                                              |

  - 如果 Spark 的 cluster manager 为 YARN，并且为 YARN ResourceManager HA，数据存储系统为 HDFS，则需添加如下配置项：

    | **配置项**                                       | **必选** | **说明**                                                     |
    | ------------------------------------------------ | -------- | ------------------------------------------------------------ |
    | type                                             | 是       | 资源类型，取值为`spark`。                                    |
    | spark.master                                     | 是       | Spark 的 cluster manager。当前仅支持 YARN，所以取值为`yarn`。 |
    | spark.submit.deployMode                          | 是       | Spark 驱动程序的部署模式。取值包括`cluster`和`client`。关于取值说明，参考 [Launching Spark on YARN](https://spark.apache.org/docs/3.3.0/running-on-yarn.html#launching-spark-on-yarn)。 |
    | spark.hadoop.yarn.resourcemanager.ha.enabled     | 是       | YARN ResourceManager 是否启用 HA。需设置该参数为`true`，即启用 HA。 |
    | spark.hadoop.yarn.resourcemanager.ha.rm-ids      | 是       | YARN ResourceManager 的逻辑 ID 列表。多个逻辑 ID 之间用逗号 (`,`) 隔开。 |
    | spark.hadoop.yarn.resourcemanager.hostname.rm-id | 是       | 对于每个 rm-id，需指定 ResourceManager 对应的主机名。如已添加该配置项，则不需要再添加`spark.hadoop.yarn.resourcemanager.address.rm-id`。 |
    | spark.hadoop.yarn.resourcemanager.address.rm-id  | 是       | 对于每个rm-id，需指定 ResourceManager 对应 `host:port`。如已添加该配置项，则不需要再添加`spark.hadoop.yarn.resourcemanager.hostname.rm-id`。 |
    | spark.hadoop.fs.defaultFS                        | 是       | Spark 使用的 HDFS 中 NameNode 节点地址。格式为：`hdfs://namenode_host:port`。 |
    | working_dir                                      | 是       | ETL 作业目录，用于存放 ETL 作业生成的中间数据。例如：`hdfs://host: port/tmp/starrocks`。 |
    | broker                                           | 是       | Broker 名称。您可以使用 [SHOW BROKER](/sql-reference/sql-statements/Administration/SHOW_BROKER.md) 语句查看当前所有 Broker 的名称。如未添加过 Broker，可使用 [ALTER SYSTEM](/sql-reference/sql-statements/Administration/ALTER_SYSTEM.md) 语句添加 Broker。 |
    | broker.username                           | 否       | 通过指定的 HDFS 用户去访问HDFS中的文件。如果 HDFS 文件只能由特定用户访问，则需要传入该参数，如果该文件是所有用户都可以访问的话，则不需要传入该参数。 |
    | broker.password                           | 否       | HDFS 用户密码。                                              |

  - 如果 Spark 的 cluster manager 为 YARN，数据存储系统为 HDFS HA，则需添加如下配置项：

    | **配置项**                                                   | **必选** | **说明**                                                     |
    | ------------------------------------------------------------ | -------- | ------------------------------------------------------------ |
    | type                                                         | 是       | 资源类型，取值为`spark`。                                    |
    | spark.master                                                 | 是       | Spark 的 cluster manager。当前仅支持 YARN，所以取值为 `yarn`。 |
    | spark.hadoop.yarn.resourcemanager.address                    | 是       | YARN ResourceManager 地址。                                  |
    | spark.hadoop.fs.defaultFS                                    | 是       | Spark 使用的 HDFS 中 NameNode 节点地址。格式为：`hdfs://namenode_host:port`。 |
    | spark.hadoop.dfs.nameservices                                | 是       | HDFS nameservice 的 ID。该配置项供 Spark 使用。              |
    | spark.hadoop.dfs.ha.namenodes.[nameservice ID]               | 是       | HDFS NameNode 的 ID。您可配置多个 NameNode ID。多个 NameNode ID 之间要用逗号 (`,`) 隔开。该配置项供 Spark 使用。 |
    | spark.hadoop.dfs.namenode.rpc-address.[nameservice ID].[name node ID] | 是       | 每个 HDFS NameNode 监听的 RPC 地址。注意需配置完全限定的 RPC 地址。该配置项供 Spark 使用。 |
    | spark.hadoop.dfs.client.failover.proxy.provider              | 是       | HDFS 的 Java 类，其用来联系 Active 状态的 NameNode。该配置项供 Spark 使用。 |
    | working_dir                                                  | 是       | ETL 作业目录，用于存放 ETL 作业生成的中间数据。例如：`hdfs://host: port/tmp/starrocks`。 |
    | broker                                                       | 是       | Broker 名称。您可以使用 [SHOW BROKER](/sql-reference/sql-statements/Administration/SHOW_BROKER.md) 语句查看当前所有 Broker 的名称。如未添加过 Broker，可使用 [ALTER SYSTEM](/sql-reference/sql-statements/Administration/ALTER_SYSTEM.md) 语句添加 Broker。 |
    | broker.username                           | 否       | 通过指定的 HDFS 用户去访问HDFS中的文件。如果 HDFS 文件只能由特定用户访问，则需要传入该参数，如果该文件是所有用户都可以访问，则不需要传入该参数。 |
    | broker.password                           | 否       | HDFS 用户密码。                                              |
    | broker.dfs.nameservices                                      | 是       | HDFS nameservice 的 ID。该配置项供 Broker 使用。             |
    | broker.dfs.ha.namenodes. [nameservice ID]                    | 是       | HDFS NameNode 的 ID。您可配置多个 NameNode ID。多个 NameNode ID 之间要用逗号 (`,`) 隔开。该配置项供 Broker 使用。 |
    | broker.dfs.namenode.rpc-address. [nameservice ID].[name node ID] | 是       | 每个 HDFS NameNode 监听的 RPC 地址。注意需配置完全限定的 RPC 地址。该配置项供 Broker 使用。 |
    | broker.dfs.client.failover.proxy.provider                    | 是       | HDFS 的 Java 类，其用来联系 Active 状态的 NameNode。该配置项供 Broker 使用。 |

- 如果使用无 Broker 进程的方式导入，则在创建资源时参数设置与使用Broker 进程的方式导入稍有差异，具体差异如下：

  - 无需传入 `broker`。
  - 如果您需要配置用户身份认证、NameNode 节点的 HA，则需要在 HDFS 集群中的 **hdfs-site.xml** 文件中配置参数，具体参数和说明，请参见 [broker_properties](/sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md#hdfs)。并且将 **hdfs-site.xml** 文件放到每一个 FE 的 **$FE_HOME/conf** 下以及每个 BE 的 **$BE_HOME/conf** 下。

> **说明**
>
> - 使用无 Broker 进程的方式导入时，如果 HDFS 文件只能由特定用户访问，则您仍然需要传入 HDFS 用户名 `broker.name`和 HDFS 用户密码`broker.password`。
> - 在以上几种情况中，如要添加除表格以外的配置项，可参考 [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html) 和 [Running Spark on YARN](https://spark.apache.org/docs/latest/running-on-yarn.html)。

#### Hive 资源

如创建一个 Hive 资源，需添加如下配置项：

| **配置项**          | **必选** | **说明**                  |
| ------------------- | -------- | ------------------------- |
| type                | 是       | 资源类型，取值为 `hive`。 |
| hive.metastore.uris | 是       | Hive metastore 的 URI。   |

#### Iceberg 资源

如创建一个 Iceberg 资源，需添加如下配置项：

| **配置项**                          | **必选** | **说明**                                                     |
| ----------------------------------- | -------- | ------------------------------------------------------------ |
| type                                | 是       | 资源类型，取值为 `iceberg`。                                 |
| starrocks.catalog-type              | 是       | Iceberg 的 catalog 类型。StarRocks 2.3 以下版本仅支持 Hive catalog；StarRocks 2.3 及以上版本支持 Hive catalog 和 custom catalog。 如要使用 Hive catalog， 设置该参数为 `HIVE`。 如要使用 custom catalog，设置该参数为 `CUSTOM`。详细信息参见[创建 Iceberg 资源](/data_source/External_table#步骤一创建-iceberg-资源)。 |
| iceberg.catalog.hive.metastore.uris | 是       | Hive metastore 的URI。                                       |

#### Hudi 资源

如创建一个 Hudi 资源，需添加如下配置项：

| **配置项**          | **必选** | **说明**                  |
| ------------------- | -------- | ------------------------- |
| type                | 是       | 资源类型，取值为 `hudi`。 |
| hive.metastore.uris | 是       | Hive metastore 的 URI。   |

#### JDBC 资源

如创建一个 JDBC 资源，需添加如下配置项：

| **配置项**   | **必选** | **说明**                                                     |
| ------------ | -------- | ------------------------------------------------------------ |
| type         | 是       | 资源类型，取值为 `jdbc`。                                    |
| user         | 是       | 登录到支持的 JDBC 数据库 （以下简称“目标数据库”）的用户名。  |
| password     | 是       | 目标数据库的登录密码。                                       |
| jdbc_uri     | 是       | 用于连接目标数据库的JDBC URI，需要满足目标数据库 URI 的语法。常见的目标数据库 URI，请参见 [MySQL](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html)、[Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/21/jjdbc/data-sources-and-URLs.html#GUID-6D8EFA50-AB0F-4A2B-88A0-45B4A67C361E)、[PostgreSQL](https://jdbc.postgresql.org/documentation/head/connect.html)、[SQL Server](https://docs.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url?view=sql-server-ver16) 官网文档。 |
| driver_url   | 是       | 用于下载 JDBC 驱动程序 JAR 包的 URL，支持使用 HTTP 协议 或者 file 协议。例如 `https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar`， `file:///home/disk1/postgresql-42.3.3.jar`。 |
| driver_class | 是       | 目标数据库使用的 JDBC 驱动程序的类名称。常见的类名称如下： <ul><li>MySQL：com.mysql.jdbc.Driver（MySQL 5.x 及以下版本）和com.mysql.cj.jdbc.Driver （MySQL 6.x 及以上版本）</li><li>SQL Server：com.microsoft.sqlserver.jdbc.SQLServerDriver </li><li>Oracle： oracle.jdbc.driver.OracleDriver </li><li>PostgreSQL：org.postgresql.Driver</li></ul> |

## 示例

示例一： 当使用 YARN 作为 Spark 的 cluster manager，HDFS 来存储数据时，创建一个名为`spark0`的 Spark 资源，语句如下：

```SQL
CREATE EXTERNAL RESOURCE "spark0"
PROPERTIES (
    "type" = "spark",
    "spark.master" = "yarn",
    "spark.submit.deployMode" = "cluster",
    "spark.executor.memory" = "1g",
    "spark.yarn.queue" = "queue0",
    "spark.hadoop.yarn.resourcemanager.address" = "resourcemanager_host:8032",
    "spark.hadoop.fs.defaultFS" = "hdfs://namenode_host:9000",
    "working_dir" = "hdfs://namenode_host:9000/tmp/starrocks",
    "broker" = "broker0",
    "broker.username" = "user0",
    "broker.password" = "password0"
);
```

示例二：当使用 YARN HA 作为 Spark 的 cluster manager，HDFS 来存储数据时，创建一个名为`spark1`的 Spark 资源，语句如下：

```SQL
CREATE EXTERNAL RESOURCE "spark1"
PROPERTIES (
    "type" = "spark",
    "spark.master" = "yarn",
    "spark.submit.deployMode" = "cluster",
    "spark.hadoop.yarn.resourcemanager.ha.enabled" = "true",
    "spark.hadoop.yarn.resourcemanager.ha.rm-ids" = "rm1,rm2",
    "spark.hadoop.yarn.resourcemanager.hostname.rm1" = "host1",
    "spark.hadoop.yarn.resourcemanager.hostname.rm2" = "host2",
    "spark.hadoop.fs.defaultFS" = "hdfs://namenode_host:9000",
    "working_dir" = "hdfs://namenode_host:9000/tmp/starrocks",
    "broker" = "broker1"
);
```

示例三：当使用 YARN 作为 Spark 的 cluster manager，HDFS HA 来存储数据时，创建一个名为`spark2`的 Spark 资源，语句如下：

```SQL
CREATE EXTERNAL RESOURCE "spark2"
PROPERTIES (
    "type" = "spark", 
    "spark.master" = "yarn",
    "spark.hadoop.yarn.resourcemanager.address" = "resourcemanager_host:8032",
    "spark.hadoop.fs.defaultFS" = "hdfs://myha",
    "spark.hadoop.dfs.nameservices" = "myha",
    "spark.hadoop.dfs.ha.namenodes.myha" = "mynamenode1,mynamenode2",
    "spark.hadoop.dfs.namenode.rpc-address.myha.mynamenode1" = "nn1_host:rpc_port",
    "spark.hadoop.dfs.namenode.rpc-address.myha.mynamenode2" = "nn2_host:rpc_port",
    "spark.hadoop.dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
    "working_dir" = "hdfs://myha/tmp/starrocks",
    "broker" = "broker2",
    "broker.dfs.nameservices" = "myha",
    "broker.dfs.ha.namenodes.myha" = "mynamenode1,mynamenode2",
    "broker.dfs.namenode.rpc-address.myha.mynamenode1" = "nn1_host:rpc_port",
    "broker.dfs.namenode.rpc-address.myha.mynamenode2" = "nn2_host:rpc_port",
    "broker.dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
);
```

示例四：创建一个名为`hive0`的 Hive 资源。

```SQL
CREATE EXTERNAL RESOURCE "hive0"
PROPERTIES (
  "type" = "hive",
  "hive.metastore.uris" = "thrift://10.10.44.98:9083"
);
```

示例五：创建一个名为`iceberg0`的 Iceberg 资源。

```SQL
CREATE EXTERNAL RESOURCE "iceberg0" 
PROPERTIES ( 
   "type" = "iceberg", 
   "starrocks.catalog-type"="HIVE", 
   "iceberg.catalog.hive.metastore.uris"="thrift://192.168.0.81:9083" 
);
```

示例六：创建一个名为`hudi0`的 Hudi 资源。

```SQL
CREATE EXTERNAL RESOURCE "hudi0" 
PROPERTIES ( 
    "type" = "hudi", 
    "hive.metastore.uris" = "thrift://192.168.7.251:9083"
);
```

示例七：创建一个名为`jdbc0`的 JDBC 资源。

```SQL
create external resource jdbc0
properties (
    "type"="jdbc",
    "user"="postgres",
    "password"="changeme",
    "jdbc_uri"="jdbc:postgresql://127.0.0.1:5432/jdbc_test",
    "driver_url"="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar",
    "driver_class"="org.postgresql.Driver"
);
```

## 相关操作

- 如要修改资源属性，参见 [ALTER RESOURCE](/sql-reference/sql-statements/data-definition/ALTER_RESOURCE.md)。
- 如要删除资源，参见 [DROP RESOURCE](/sql-reference/sql-statements/data-definition/DROP_RESOURCE.md)。
- 如要使用 Spark 资源进行 Spark Load，参见 [Spark Load](/loading/SparkLoad.md)。
- 如要引用 Hive、Iceberg、Hudi 和 JDBC 资源创建外部表，参见[外部表](/data_source/External_table.md)。
