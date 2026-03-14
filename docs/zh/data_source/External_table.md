---
displayed_sidebar: docs
---

# 外部表

:::note

外部表功能不再推荐使用，仅适用于某些特殊边缘场景，并可能在未来版本中弃用。在一般场景下，要管理和查询来自外部数据源的数据，[外部 Catalog](./catalog/catalog_overview.md)是推荐的。

- 从 v3.0 版本开始，我们推荐您使用 Catalog 查询 Hive、Iceberg 和 Hudi 中的数据。请参见[Hive Catalog](../data_source/catalog/hive_catalog.md)、[Iceberg Catalog](./catalog/iceberg/iceberg_catalog.md)和[Hudi Catalog](../data_source/catalog/hudi_catalog.md)。

- 从 v3.1 版本开始，我们推荐您使用[JDBC Catalog](../data_source/catalog/jdbc_catalog.md)查询 MySQL 和 PostgreSQL 中的数据，并使用[Elasticsearch Catalog](../data_source/catalog/elasticsearch_catalog.md)查询 Elasticsearch 中的数据。

- 从 v3.2.9 和 v3.3.1 版本开始，我们推荐您使用[JDBC Catalog](../data_source/catalog/jdbc_catalog.md)查询 Oracle 和 SQL Server 中的数据。

- 外部表功能旨在帮助将数据加载到 StarRocks 中，而不是作为常规操作对外部系统执行高效查询。更高效的解决方案是将数据加载到 StarRocks 中。

:::

StarRocks 支持使用外部表访问其他数据源。外部表是基于存储在其他数据源中的数据表创建的。StarRocks 只存储数据表的元数据。您可以使用外部表直接查询其他数据源中的数据。目前，除了 StarRocks 外部表，所有其他外部表都已弃用。**您只能将数据从另一个 StarRocks 集群写入当前 StarRocks 集群。您不能从中读取数据。对于 StarRocks 以外的数据源，您只能从这些数据源读取数据。**

从 2.5 版本开始，StarRocks 提供了数据缓存功能，可加速对外部数据源的热数据查询。更多信息，请参见[数据缓存](data_cache.md)。

## StarRocks 外部表

从 StarRocks 1.19 版本开始，StarRocks 允许您使用 StarRocks 外部表将数据从一个 StarRocks 集群写入另一个集群。这实现了读写分离并提供了更好的资源隔离。您可以首先在目标 StarRocks 集群中创建目标表。然后，在源 StarRocks 集群中，您可以创建一个与目标表具有相同 schema 的 StarRocks 外部表，并在`PROPERTIES`字段中指定目标集群和表的信息。

通过使用 INSERT INTO 语句写入 StarRocks 外部表，可以将数据从源集群写入目标集群。这有助于实现以下目标：

- StarRocks 集群之间的数据同步。
- 读写分离。数据写入源集群，源集群的数据变更同步到目标集群，由目标集群提供查询服务。

以下代码展示了如何创建目标表和外部表。

```SQL
# Create a destination table in the destination StarRocks cluster.
CREATE TABLE t
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=olap
DISTRIBUTED BY HASH(k1);

# Create an external table in the source StarRocks cluster.
CREATE EXTERNAL TABLE external_t
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=olap
DISTRIBUTED BY HASH(k1)
PROPERTIES
(
    "host" = "127.0.0.1",
    "port" = "9020",
    "user" = "user",
    "password" = "passwd",
    "database" = "db_test",
    "table" = "t"
);

# Write data from a source cluster to a destination cluster by writing data into the StarRocks external table. The second statement is recommended for the production environment.
insert into external_t values ('2020-10-11', 1, 1, 'hello', '2020-10-11 10:00:00');
insert into external_t select * from other_table;
```

参数：

- **EXTERNAL：**此关键字表示要创建的表是外部表。

- **host：**此参数指定目标 StarRocks 集群的 Leader FE 节点的 IP 地址。

- **port：**此参数指定目标 StarRocks 集群 FE 节点的 RPC 端口。

  :::note

  为确保 StarRocks 外部表所属的源集群能够访问目标 StarRocks 集群，您必须配置网络和防火墙以允许访问以下端口：

  - FE 节点的 RPC 端口。请参见 `rpc_port` 在 FE 配置文件 **fe/fe.conf**。默认 RPC 端口是 `9020`。
  - BE 节点的 bRPC 端口。请参见 `brpc_port` 在 BE 配置文件 **be/be.conf**。默认 bRPC 端口是 `8060`。

  :::

- **user:**此参数指定用于访问目标 StarRocks 集群的用户名。

- **password:**此参数指定用于访问目标 StarRocks 集群的密码。

- **database:**此参数指定目标表所属的数据库。

- **table:**此参数指定目标表的名称。

使用 StarRocks 外部表时，适用以下限制：

- 您只能对 StarRocks 外部表运行 INSERT INTO 和 SHOW CREATE TABLE 命令。不支持其他数据写入方法。此外，您不能从 StarRocks 外部表查询数据或对其执行 DDL 操作。
- 创建外部表的语法与创建普通表相同，但外部表中的列名和其他信息必须与目标表相同。
- 外部表每 10 秒从目标表同步一次表元数据。如果对目标表执行 DDL 操作，两个表之间的数据同步可能会有延迟。

## （已弃用）JDBC 兼容数据库的外部表

从 v2.3.0 开始，StarRocks 提供外部表来查询 JDBC 兼容数据库。通过这种方式，您可以以极快的速度分析此类数据库的数据，而无需将数据导入 StarRocks。本节介绍如何在 StarRocks 中创建外部表并查询 JDBC 兼容数据库中的数据。

### 前提条件

在使用 JDBC 外部表查询数据之前，请确保 FE 和 BE 可以访问 JDBC 驱动程序的下载 URL。下载 URL 由 `driver_url` 参数指定，该参数用于创建 JDBC 资源的语句中。

### 创建和管理 JDBC 资源

#### 创建 JDBC 资源

在创建外部表以查询数据库中的数据之前，您需要在 StarRocks 中创建一个 JDBC 资源来管理数据库的连接信息。该数据库必须支持 JDBC 驱动程序，并被称为“目标数据库”。创建资源后，您可以使用它来创建外部表。

执行以下语句以创建名为 `jdbc0`：

```SQL
CREATE EXTERNAL RESOURCE jdbc0
PROPERTIES (
    "type"="jdbc",
    "user"="postgres",
    "password"="changeme",
    "jdbc_uri"="jdbc:postgresql://127.0.0.1:5432/jdbc_test",
    "driver_url"="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar",
    "driver_class"="org.postgresql.Driver"
);
```

中所需的参数`PROPERTIES`如下：

- `type`：资源的类型。将值设置为 `jdbc`。

- `user`: 用于连接目标数据库的用户名。

- `password`: 用于连接目标数据库的密码。

- `jdbc_uri`: JDBC 驱动程序用于连接目标数据库的 URI。URI 格式必须满足数据库 URI 语法。有关一些常见数据库的 URI 语法，请访问以下官方网站：[Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/21/jjdbc/data-sources-and-URLs.html#GUID-6D8EFA50-AB0F-4A2B-88A0-45B4A67C361E)、[PostgreSQL](https://jdbc.postgresql.org/documentation/head/connect.html)、[SQL Server](https://docs.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url?view=sql-server-ver16)。

> 注意：URI 必须包含目标数据库的名称。例如，在前面的代码示例中，`jdbc_test` 是您要连接的目标数据库的名称。

- `driver_url`: JDBC 驱动程序 JAR 包的下载 URL。支持 HTTP URL 或文件 URL，例如：`https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar` 或 `file:///home/disk1/postgresql-42.3.3.jar`。

- `driver_class`: JDBC 驱动程序的类名。常见数据库的 JDBC 驱动程序类名如下：
  - MySQL: com.mysql.jdbc.Driver (MySQL 5.x 及更早版本), com.mysql.cj.jdbc.Driver (MySQL 6.x 及更高版本)
  - SQL Server: com.microsoft.sqlserver.jdbc.SQLServerDriver
  - Oracle: oracle.jdbc.driver.OracleDriver
  - PostgreSQL: org.postgresql.Driver

创建资源时，FE 会使用 `driver_url` 参数中指定的 URL 下载 JDBC 驱动程序 JAR 包，生成校验和，并使用该校验和验证 BE 下载的 JDBC 驱动程序。

> 注意：如果 JDBC 驱动程序 JAR 包下载失败，则资源创建也会失败。

当 BE 首次查询 JDBC 外部表时，如果发现其机器上不存在相应的 JDBC 驱动程序 JAR 包，BE 会使用 `driver_url` 参数中指定的 URL 下载 JDBC 驱动程序 JAR 包，所有 JDBC 驱动程序 JAR 包都保存在 `${STARROCKS_HOME}/lib/jdbc_drivers` 目录中。

#### 查看 JDBC 资源

执行以下语句以查看 StarRocks 中的所有 JDBC 资源：

```SQL
SHOW RESOURCES;
```

> 注意：`ResourceType` 列为 `jdbc`。

#### 删除 JDBC 资源

执行以下语句以删除名为 `jdbc0` 的 JDBC 资源：

```SQL
DROP RESOURCE "jdbc0";
```

> 注意：删除 JDBC 资源后，所有使用该 JDBC 资源创建的 JDBC 外部表都将不可用。但是，目标数据库中的数据不会丢失。如果您仍然需要使用 StarRocks 查询目标数据库中的数据，可以重新创建 JDBC 资源和 JDBC 外部表。

### 创建数据库

执行以下语句以在 StarRocks 中创建并访问名为 `jdbc_test` 的数据库：

```SQL
CREATE DATABASE jdbc_test; 
USE jdbc_test; 
```

> 注意：您在上述语句中指定的数据库名称无需与目标数据库的名称相同。

### 创建 JDBC 外部表

执行以下语句以创建名为 `jdbc_tbl` 在数据库中 `jdbc_test`：

```SQL
create external table jdbc_tbl (
    `id` bigint NULL, 
    `data` varchar(200) NULL 
) ENGINE=jdbc 
properties (
    "resource" = "jdbc0",
    "table" = "dest_tbl"
);
```

 中所需的参数如下：`properties` 如下：

- `resource`：用于创建外表的 JDBC 资源名称。

- `table`：数据库中的目标表名称。

有关 StarRocks 和目标数据库之间支持的数据类型和数据类型映射，请参见[数据类型映射](External_table.md#Data type mapping)。

> 注意：
>
> - 不支持索引。
> - 您不能使用 PARTITION BY 或 DISTRIBUTED BY 来指定数据分布规则。

### 查询 JDBC 外表

在查询 JDBC 外表之前，您必须执行以下语句以启用 Pipeline 引擎：

```SQL
set enable_pipeline_engine=true;
```

> 注意：如果 Pipeline 引擎已启用，您可以跳过此步骤。

执行以下语句，通过 JDBC 外表查询目标数据库中的数据。

```SQL
select * from JDBC_tbl;
```

StarRocks 支持谓词下推，即将过滤条件推送到目标表。尽可能在靠近数据源的地方执行过滤条件可以提高查询性能。目前，StarRocks 可以下推运算符，包括二元比较运算符（`>`、`>=`、`=`、`<` 和 `<=`）、`IN`、`IS NULL` 和 `BETWEEN ... AND ...`。但是，StarRocks 无法下推函数。

### 数据类型映射

目前，StarRocks 只能查询目标数据库中的基本类型数据，例如 NUMBER、STRING、TIME 和 DATE。如果目标数据库中的数据值范围不受 StarRocks 支持，则查询会报错。

目标数据库和 StarRocks 之间的映射因目标数据库的类型而异。

#### **MySQL 和 StarRocks**

| MySQL        | StarRocks |
| ------------ | --------- |
| BOOLEAN      | BOOLEAN   |
| TINYINT      | TINYINT   |
| SMALLINT     | SMALLINT  |
| MEDIUMINTINT | INT       |
| BIGINT       | BIGINT    |
| FLOAT        | FLOAT     |
| DOUBLE       | DOUBLE    |
| DECIMAL      | DECIMAL   |
| CHAR         | CHAR      |
| VARCHAR      | VARCHAR   |
| DATE         | DATE      |
| DATETIME     | DATETIME  |

#### **Oracle 和 StarRocks**

| Oracle          | StarRocks |
| --------------- | --------- |
| CHAR            | CHAR      |
| VARCHARVARCHAR2 | VARCHAR   |
| DATE            | DATE      |
| SMALLINT        | SMALLINT  |
| INT             | INT       |
| BINARY_FLOAT    | FLOAT     |
| BINARY_DOUBLE   | DOUBLE    |
| DATE            | DATE      |
| DATETIME        | DATETIME  |
| NUMBER          | DECIMAL   |

#### **PostgreSQL 和 StarRocks**

| PostgreSQL          | StarRocks |
| ------------------- | --------- |
| SMALLINTSMALLSERIAL | SMALLINT  |
| INTEGERSERIAL       | INT       |
| BIGINTBIGSERIAL     | BIGINT    |
| BOOLEAN             | BOOLEAN   |
| REAL                | FLOAT     |
| DOUBLE PRECISION    | DOUBLE    |
| DECIMAL             | DECIMAL   |
| TIMESTAMP           | DATETIME  |
| DATE                | DATE      |
| CHAR                | CHAR      |
| VARCHAR             | VARCHAR   |
| TEXT                | VARCHAR   |

#### **SQL Server 和 StarRocks**

| SQL Server        | StarRocks |
| ----------------- | --------- |
| BOOLEAN           | BOOLEAN   |
| TINYINT           | TINYINT   |
| SMALLINT          | SMALLINT  |
| INT               | INT       |
| BIGINT            | BIGINT    |
| FLOAT             | FLOAT     |
| REAL              | DOUBLE    |
| DECIMALNUMERIC    | DECIMAL   |
| CHAR              | CHAR      |
| VARCHAR           | VARCHAR   |
| DATE              | DATE      |
| DATETIMEDATETIME2 | DATETIME  |

### 限制

- 创建 JDBC 外表时，您不能在表上创建索引，也不能使用 PARTITION BY 和 DISTRIBUTED BY 为表指定数据分布规则。

- 查询 JDBC 外表时，StarRocks 无法将函数下推到表中。

## (已弃用) Elasticsearch 外表

StarRocks 和 Elasticsearch 是两个流行的分析系统。StarRocks 在大规模分布式计算中表现出色。Elasticsearch 非常适合全文搜索。StarRocks 与 Elasticsearch 结合可以提供更完整的 OLAP 解决方案。

### 创建 Elasticsearch 外表示例

#### 语法

```sql
CREATE EXTERNAL TABLE elastic_search_external_table
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=ELASTICSEARCH
PROPERTIES (
    "hosts" = "http://192.168.0.1:9200,http://192.168.0.2:9200",
    "user" = "root",
    "password" = "root",
    "index" = "tindex",
    "type" = "_doc",
    "es.net.ssl" = "true"
);
```

下表描述了参数。

| **参数**        | **必填** | **默认值** | **描述**                                              |
| -------------------- | ------------ | ----------------- | ------------------------------------------------------------ |
| hosts                | Yes          | None              | Elasticsearch 集群的连接地址。您可以指定一个或多个地址。StarRocks 可以从该地址解析 Elasticsearch 版本和索引分片分配。StarRocks 根据 `GET /_nodes/http` API 操作返回的地址与您的 Elasticsearch 集群进行通信。因此，`host` 参数的值必须与 `GET /_nodes/http` API 操作返回的地址相同。否则，BE 可能无法与您的 Elasticsearch 集群通信。|`index` 为 <code class="language-text">hello*</code>，StarRocks 会检索所有名称以 `hello` 开头的索引。|`/*cluster/state/*nodes/http` 和索引的访问权限。|`_doc`            | 索引的类型。默认值：`_doc`。如果您想查询 Elasticsearch 8 及更高版本中的数据，则无需配置此参数，因为 Elasticsearch 8 及更高版本已删除映射类型。|`false`           | 指定 StarRocks 是否仅使用 `hosts` 指定的地址访问 Elasticsearch 集群并获取数据。<ul><li>`true`：StarRocks 仅使用 `hosts` 指定的地址访问 Elasticsearch 集群并获取数据，而不嗅探 Elasticsearch 索引分片所在的数据节点。如果 StarRocks 无法访问 Elasticsearch 集群内部数据节点的地址，则需要将此参数设置为 `true`。</li><li>`false`：StarRocks 使用 `host` 指定的地址嗅探 Elasticsearch 集群索引分片所在的数据节点。StarRocks 生成查询执行计划后，相关的 BE 直接访问 Elasticsearch 集群内部的数据节点，从索引分片中获取数据。如果 StarRocks 可以访问 Elasticsearch 集群内部数据节点的地址，我们建议您保留默认值 `false`。</li></ul> |`false`           | 指定是否可以使用 HTTPS 协议访问您的 Elasticsearch 集群。仅 StarRocks 2.4 及更高版本支持配置此参数。<ul><li>`true`：HTTPS 和 HTTP 协议都可以用于访问您的 Elasticsearch 集群。</li><li>`false`：仅 HTTP 协议可以用于访问您的 Elasticsearch 集群。</li></ul> |`true`            | 指定是否从 Elasticsearch 列式存储中获取目标字段的值。在大多数情况下，从列式存储读取数据优于从行存储读取数据。|`true`            | 指定是否根据 KEYWORD 类型字段嗅探 Elasticsearch 中的 TEXT 类型字段。如果此参数设置为 `false`，StarRocks 会在分词后执行匹配。|

##### 列式扫描以加快查询速度

如果您将 `enable_docvalue_scan` 设置为 `true`，StarRocks 从 Elasticsearch 获取数据时遵循以下规则：

- **尝试查看**：StarRocks 自动检查目标字段是否启用了列式存储。如果启用，StarRocks 将从列式存储中获取目标字段的所有值。
- **自动降级**：如果目标字段中的任何一个在列式存储中不可用，StarRocks 将从行式存储中解析并获取目标字段的所有值（`_source`）。

> **注意**
>
> - Elasticsearch 中 TEXT 类型的字段不支持列式存储。因此，如果您查询包含 TEXT 类型值的字段，StarRocks 将从`_source`。
> - 如果您查询大量（大于或等于 25 个）字段，从`docvalue`读取字段值与从`_source`。

##### 嗅探 KEYWORD 类型字段

如果您将`enable_keyword_sniff`设置为`true`，Elasticsearch 允许直接摄取数据而无需索引，因为它会在摄取后自动创建索引。对于 STRING 类型的字段，Elasticsearch 将创建一个同时具有 TEXT 和 KEYWORD 类型的字段。这就是 Elasticsearch 的多字段功能的工作方式。映射如下：

```SQL
"k4": {
   "type": "text",
   "fields": {
      "keyword": {   
         "type": "keyword",
         "ignore_above": 256
      }
   }
}
```

例如，要在`k4`上执行“=”过滤，StarRocks 在 Elasticsearch 上会将过滤操作转换为 Elasticsearch TermQuery。

原始 SQL 过滤器如下：

```SQL
k4 = "StarRocks On Elasticsearch"
```

转换后的 Elasticsearch 查询 DSL 如下：

```SQL
"term" : {
    "k4": "StarRocks On Elasticsearch"

}
```

的第一个字段是`k4`TEXT 类型，它将由为`k4`配置的分析器（如果未为`k4`配置分析器，则由标准分析器）在数据摄取后进行分词。结果，第一个字段将被分词为三个词项：`StarRocks`、`On`和`Elasticsearch`。详细信息如下：

```SQL
POST /_analyze
{
  "analyzer": "standard",
  "text": "StarRocks On Elasticsearch"
}
```

分词结果如下：

```SQL
{
   "tokens": [
      {
         "token": "starrocks",
         "start_offset": 0,
         "end_offset": 5,
         "type": "<ALPHANUM>",
         "position": 0
      },
      {
         "token": "on",
         "start_offset": 6,
         "end_offset": 8,
         "type": "<ALPHANUM>",
         "position": 1
      },
      {
         "token": "elasticsearch",
         "start_offset": 9,
         "end_offset": 11,
         "type": "<ALPHANUM>",
         "position": 2
      }
   ]
}
```

假设您执行以下查询：

```SQL
"term" : {
    "k4": "StarRocks On Elasticsearch"
}
```

字典中没有与词项`StarRocks On Elasticsearch`匹配的词项，因此不会返回任何结果。

但是，如果您已将`enable_keyword_sniff`设置为`true`，StarRocks 会将`k4 = "StarRocks On Elasticsearch"`转换为`k4.keyword = "StarRocks On Elasticsearch"`以匹配 SQL 语义。转换后的`StarRocks On Elasticsearch`查询 DSL 如下：

```SQL
"term" : {
    "k4.keyword": "StarRocks On Elasticsearch"
}
```

`k4.keyword`是 KEYWORD 类型。因此，数据作为完整词项写入 Elasticsearch，从而实现成功匹配。

#### 列数据类型映射

创建外部表时，您需要根据 Elasticsearch 表中列的数据类型来指定外部表中列的数据类型。下表显示了列数据类型的映射。

| **Elasticsearch** | **StarRocks**               |
| ----------------- | --------------------------- |
| BOOLEAN           | BOOLEAN                     |
| BYTE              | TINYINT/SMALLINT/INT/BIGINT |
| SHORT             | SMALLINT/INT/BIGINT         |
| INTEGER           | INT/BIGINT                  |
| LONG              | BIGINT                      |
| FLOAT             | FLOAT                       |
| DOUBLE            | DOUBLE                      |
| KEYWORD           | CHAR/VARCHAR                |
| TEXT              | CHAR/VARCHAR                |
| DATE              | DATE/DATETIME               |
| NESTED            | CHAR/VARCHAR                |
| OBJECT            | CHAR/VARCHAR                |
| ARRAY             | ARRAY                       |

> **注意**
>
> - StarRocks 使用 JSON 相关函数读取 NESTED 类型的数据。
> - Elasticsearch 会自动将多维数组展平为一维数组。StarRocks 也会这样做。**从 v2.5 版本开始，StarRocks 支持查询 Elasticsearch 中的 ARRAY 类型数据。**

### 谓词下推

StarRocks 支持谓词下推。过滤器可以下推到 Elasticsearch 执行，从而提高查询性能。下表列出了支持谓词下推的运算符。

|   SQL 语法  |   ES 语法  |
| :---: | :---: |
|  `=`   |  term query   |`in`   |  terms query   |`>=,  <=, >, <`   |  range   |`and`   |  bool.filter   |`or`   |  bool.should   |`not`   |  bool.must_not   |`not in`   |  bool.must_not + terms   |`esquery`   |  ES Query DSL  |

### 示例

通过 **esquery 函数** 可以将 **无法用 SQL 表达的查询** (例如 match 和 geoshape) 下推到 Elasticsearch 进行过滤。esquery 函数的第一个参数用于关联索引。第二个参数是基本 Query DSL 的 JSON 表达式，该表达式用方括号 {} 括起来。**JSON 表达式必须只有一个根键**，例如 match、geo_shape 或 bool。

- match 查询

```sql
select * from es_table where esquery(k4, '{
    "match": {
       "k4": "StarRocks on elasticsearch"
    }
}');
```

- 地理相关查询

```sql
select * from es_table where esquery(k4, '{
  "geo_shape": {
     "location": {
        "shape": {
           "type": "envelope",
           "coordinates": [
              [
                 13,
                 53
              ],
              [
                 14,
                 52
              ]
           ]
        },
        "relation": "within"
     }
  }
}');
```

- bool 查询

```sql
select * from es_table where esquery(k4, ' {
     "bool": {
        "must": [
           {
              "terms": {
                 "k1": [
                    11,
                    12
                 ]
              }
           },
           {
              "terms": {
                 "k2": [
                    100
                 ]
              }
           }
        ]
     }
  }');
```

### 使用须知

- Elasticsearch 5.x 之前的版本与 5.x 之后的版本扫描数据的方式不同。目前，**仅支持 5.x 及更高版本**。
- 支持启用 HTTP 基本认证的 Elasticsearch 集群。
- 从 StarRocks 查询数据可能不如直接从 Elasticsearch 查询数据快，例如与 count 相关的查询。原因是 Elasticsearch 直接读取目标文档的元数据，无需过滤实际数据，从而加快了 count 查询。

## (已弃用) Hive 外部表

在使用 Hive 外部表之前，请确保您的服务器上已安装 JDK 1.8。

### 创建 Hive 资源

Hive 资源对应一个 Hive 集群。您必须配置 StarRocks 使用的 Hive 集群，例如 Hive metastore 地址。您必须指定 Hive 外部表使用的 Hive 资源。

- 创建名为 hive0 的 Hive 资源。

```sql
CREATE EXTERNAL RESOURCE "hive0"
PROPERTIES (
  "type" = "hive",
  "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
);
```

- 查看 StarRocks 中创建的资源。

```sql
SHOW RESOURCES;
```

- 删除名为 `hive0`的资源。

```sql
DROP RESOURCE "hive0";
```

您可以修改 `hive.metastore.uris` 的 Hive 资源，在 StarRocks 2.3 及更高版本中。更多信息，请参见 [ALTER RESOURCE](../sql-reference/sql-statements/Resource/ALTER_RESOURCE.md)。

### 创建数据库

```sql
CREATE DATABASE hive_test;
USE hive_test;
```

### 创建 Hive 外部表

语法

```sql
CREATE EXTERNAL TABLE table_name (
  col_name col_type [NULL | NOT NULL] [COMMENT "comment"]
) ENGINE=HIVE
PROPERTIES (
  "key" = "value"
);
```

示例：创建外部表 `profile_parquet_p7`，在 `rawdata` 数据库中，对应 `hive0` 资源的 Hive 集群。

```sql
CREATE EXTERNAL TABLE `profile_wos_p7` (
  `id` bigint NULL,
  `first_id` varchar(200) NULL,
  `second_id` varchar(200) NULL,
  `p__device_id_list` varchar(200) NULL,
  `p__is_deleted` bigint NULL,
  `p_channel` varchar(200) NULL,
  `p_platform` varchar(200) NULL,
  `p_source` varchar(200) NULL,
  `p__city` varchar(200) NULL,
  `p__province` varchar(200) NULL,
  `p__update_time` bigint NULL,
  `p__first_visit_time` bigint NULL,
  `p__last_seen_time` bigint NULL
) ENGINE=HIVE
PROPERTIES (
  "resource" = "hive0",
  "database" = "rawdata",
  "table" = "profile_parquet_p7"
);
```

说明：

- 外部表中的列
  - 列名必须与 Hive 表中的列名相同。
  - 列的顺序 **不需要** 与 Hive 表中的列顺序相同。
  - 您只能选择 **Hive 表中的部分列**，但必须选择所有 **分区键列**。
  - 外部表的分区键列不需要使用 `partition by` 来指定。它们必须与其他列在相同的描述列表中定义。您无需指定分区信息。StarRocks 将自动从 Hive 表中同步此信息。
  - 将 `ENGINE` 设置为 HIVE。
- PROPERTIES：
  - **hive.resource**：使用的 Hive 资源。
  - **database**：Hive 数据库。
  - **table**：Hive 中的表。**视图** 不支持。
- 下表描述了 Hive 和 StarRocks 之间列数据类型的映射关系。

  | Hive 列类型 | StarRocks 列类型 | 说明 |
| --- | --- | ---|
| INT/INTEGER | INT | |
| BIGINT | BIGINT | |
| TIMESTAMP | DATETIME | 将 TIMESTAMP 数据转换为 DATETIME 数据时，精度和时区信息会丢失。您需要根据 sessionVariable 中的时区，将 TIMESTAMP 数据转换为不带时区偏移的 DATETIME 数据。 |
| STRING | VARCHAR | |
| VARCHAR | VARCHAR | |
| CHAR | CHAR | |
| DOUBLE | DOUBLE | |
| FLOAT | FLOAT| |
| DECIMAL | DECIMAL| |
| ARRAY | ARRAY | |

> 注意：
>
> - 目前，支持的 Hive 存储格式包括 Parquet、ORC 和 CSV。
如果存储格式为 CSV，则不能使用引号作为转义字符。
> - 支持 SNAPPY 和 LZ4 压缩格式。
> - 可查询的 Hive 字符串列的最大长度为 1 MB。如果字符串列超过 1 MB，则会将其作为 NULL 列处理。

### 使用 Hive 外部表

查询 的总行数。`profile_wos_p7`。

```sql
select count(*) from profile_wos_p7;
```

### 更新缓存的 Hive 表元数据

- Hive 分区信息和相关文件信息缓存在 StarRocks 中。缓存会按照 指定的间隔刷新。`hive_meta_cache_refresh_interval_s`。默认值为 7200。
  - 缓存数据也可以手动刷新。
    1. 如果 Hive 表中添加或删除了分区，您必须运行 `REFRESH EXTERNAL TABLE hive_t` 命令来刷新 StarRocks 中缓存的表元数据。`hive_t` 是 StarRocks 中 Hive 外部表的名称。
    2. 如果某些 Hive 分区中的数据已更新，您必须通过运行 `REFRESH EXTERNAL TABLE hive_t PARTITION ('k1=01/k2=02', 'k1=03/k2=04')` 命令来刷新 StarRocks 中的缓存数据。`hive_t` 是 StarRocks 中 Hive 外部表的名称。`'k1=01/k2=02'` 和 `'k1=03/k2=04'` 是数据已更新的 Hive 分区的名称。
    3. 当您运行 `REFRESH EXTERNAL TABLE hive_t` 时，StarRocks 首先检查 Hive 外部表的列信息是否与 Hive Metastore 返回的 Hive 表的列信息相同。如果 Hive 表的 schema 发生变化，例如添加列或删除列，StarRocks 会将这些变化同步到 Hive 外部表。同步后，Hive 外部表的列顺序与 Hive 表的列顺序保持一致，分区列作为最后一列。
- 当 Hive 数据以 Parquet、ORC 和 CSV 格式存储时，您可以在 StarRocks 2.3 及更高版本中将 Hive 表的 schema 更改（例如 ADD COLUMN 和 REPLACE COLUMN）同步到 Hive 外部表。

### 访问对象存储

- FE 配置文件路径为 `fe/conf`，如果需要自定义 Hadoop 集群，可以将配置文件添加到该路径。例如：如果 HDFS 集群使用高可用 nameservice，您需要将 `hdfs-site.xml` 放在 `fe/conf` 下。如果 HDFS 配置了 ViewFs，您需要将 `core-site.xml` 放在 `fe/conf`。

- BE 配置文件路径为 `be/conf`，如果需要自定义 Hadoop 集群，可以将配置文件添加到该路径。例如：如果 HDFS 集群使用高可用 nameservice，您需要将 `hdfs-site.xml` 放在 `be/conf` 下。如果 HDFS 配置了 ViewFs，您需要将 `core-site.xml` 放在 `be/conf`。

- 在 BE 所在机器上，在 BE **启动脚本** `bin/start_be.sh`，例如，`export JAVA_HOME = <JDK path>`。您必须将此配置添加到脚本的开头，并重启 BE 以使配置生效。

- 配置 Kerberos 支持：
  1. 要使用 `kinit -kt keytab_path principal` 登录所有 FE/BE 机器，您需要访问 Hive 和 HDFS。kinit 命令登录只在一段时间内有效，需要放入 crontab 定期执行。
  2. 将 `hive-site.xml/core-site.xml/hdfs-site.xml` 放在 `fe/conf` 下，并将 `core-site.xml/hdfs-site.xml` 放在 `be/conf` 下。
  3. 将 `-Djava.security.krb5.conf=/etc/krb5.conf` 添加到 `JAVA_OPTS` 文件中 **$FE_HOME/conf/fe.conf** 选项的值中。**/etc/krb5.conf** 是 **krb5.conf** 文件的保存路径。您可以根据您的操作系统更改路径。
  4. 直接将 `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` 添加到 **$BE_HOME/conf/be.conf** 文件中。**/etc/krb5.conf** 是 **krb5.conf** 文件的保存路径。您可以根据您的操作系统更改路径。
  5. 添加 Hive 资源时，您必须向 `hive.metastore.uris` 传入域名。此外，您必须在 **/etc/hosts** 文件中添加 Hive/HDFS 域名和 IP 地址的映射。

- 配置 AWS S3 支持：将以下配置添加到 `fe/conf/core-site.xml` 和 `be/conf/core-site.xml` 中。

  ```XML
  <configuration>
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
        <value>s3.us-west-2.amazonaws.com</value>
     </property>
     <property>
     <name>fs.s3a.connection.maximum</name>
     <value>500</value>
     </property>
  </configuration>
  ```

  1. `fs.s3a.access.key`：AWS 访问密钥 ID。
  2. `fs.s3a.secret.key`：AWS 秘密密钥。
  3. `fs.s3a.endpoint`：要连接的 AWS S3 端点。
  4. `fs.s3a.connection.maximum`：StarRocks 到 S3 的最大并发连接数。如果在查询期间发生错误 `Timeout waiting for connection from poll`，您可以将此参数设置为更大的值。

## （已弃用）Iceberg 外部表

从 v2.1.0 版本开始，StarRocks 支持通过外部表查询 Apache Iceberg 中的数据。要查询 Iceberg 中的数据，您需要在 StarRocks 中创建 Iceberg 外部表。创建表时，您需要建立外部表与要查询的 Iceberg 表之间的映射关系。

### 开始之前

确保 StarRocks 具有访问 Apache Iceberg 所使用的元数据服务（例如 Hive metastore）、文件系统（例如 HDFS）和对象存储系统（例如 Amazon S3 和阿里云对象存储服务）的权限。

### 注意事项

- Iceberg 外部表仅支持查询以下类型的数据：
  - Iceberg v1 表（分析数据表）。从 v3.0 版本开始支持 ORC 格式的 Iceberg v2 表（行级删除），从 v3.1 版本开始支持 Parquet 格式的 Iceberg v2 表。有关 Iceberg v1 表和 Iceberg v2 表之间的差异，请参阅[Iceberg 表规范](https://iceberg.apache.org/spec/)。
  - 以 gzip（默认格式）、Zstd、LZ4 或 Snappy 格式压缩的表。
  - 以 Parquet 或 ORC 格式存储的文件。

- StarRocks 2.3 及更高版本中的 Iceberg 外部表支持同步 Iceberg 表的 Schema 变更，而 StarRocks 2.3 之前的版本不支持。如果 Iceberg 表的 Schema 发生变更，您必须删除相应的外部表并重新创建。

### 操作步骤

#### 步骤 1：创建 Iceberg 资源

在创建 Iceberg 外部表之前，您必须在 StarRocks 中创建 Iceberg 资源。该资源用于管理 Iceberg 访问信息。此外，您还需要在创建外部表的语句中指定此资源。您可以根据业务需求创建资源：

- 如果 Iceberg 表的元数据是从 Hive metastore 获取的，您可以创建一个资源并将 Catalog 类型设置为`HIVE`。

- 如果 Iceberg 表的元数据是从其他服务获取的，您需要创建一个自定义 Catalog。然后创建一个资源并将 Catalog 类型设置为`CUSTOM`。

##### 创建 Catalog 类型为`HIVE`

的资源。例如，创建一个名为`iceberg0`的资源，并将 Catalog 类型设置为`HIVE`。

```SQL
CREATE EXTERNAL RESOURCE "iceberg0" 
PROPERTIES (
   "type" = "iceberg",
   "iceberg.catalog.type" = "HIVE",
   "iceberg.catalog.hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083" 
);
```

下表描述了相关参数。

|**参数**|**描述**| type | 资源类型。将值设置为 `iceberg`。|
| iceberg.catalog.type | 资源的 Catalog 类型。支持 Hive Catalog 和自定义 Catalog。如果指定 Hive Catalog，将值设置为 `hive`。如果指定自定义 Catalog，将值设置为 `custom`。|
| iceberg.catalog.hive.metastore.uris | Hive metastore 的 URI。参数值格式为 `thrift://host:port`。端口号默认为 9083。Apache Iceberg 使用 Hive Catalog 访问 Hive metastore，然后查询 Iceberg 表的元数据。|`iceberg`hive`HIVE`custom`CUSTOM`thrift://host:port`thrift://< IP address of Iceberg metadata >:< port number >`。

##### 创建 Catalog 类型为`CUSTOM`

的资源。自定义 Catalog 需要继承抽象类 BaseMetastoreCatalog，并且您需要实现 IcebergCatalog 接口。此外，自定义 Catalog 的类名不能与 StarRocks 中已存在的类名重复。创建 Catalog 后，将 Catalog 及其相关文件打包，并将其放置在每个 Frontend (FE) 的**fe/lib**路径下。然后重启每个 FE。完成上述操作后，您可以创建 Catalog 为自定义 Catalog 的资源。

例如，创建一个名为`iceberg1`的资源，并将 Catalog 类型设置为`CUSTOM`。

```SQL
CREATE EXTERNAL RESOURCE "iceberg1" 
PROPERTIES (
   "type" = "iceberg",
   "iceberg.catalog.type" = "CUSTOM",
   "iceberg.catalog-impl" = "com.starrocks.IcebergCustomCatalog" 
);
```

下表描述了相关参数。

| **参数**          | **描述**                                              |
| ---------------------- | ------------------------------------------------------------ |
| type                   | 资源类型。将值设置为`iceberg`。               |
| iceberg.catalog.type | 资源的 Catalog 类型。支持 Hive Catalog 和自定义 Catalog。如果您指定 Hive Catalog，将值设置为`HIVE`。如果您指定自定义 Catalog，将值设置为`CUSTOM`。 |
| iceberg.catalog-impl   | 自定义 Catalog 的完全限定类名。FE 会根据此名称搜索 Catalog。如果 Catalog 包含自定义配置项，您必须在创建 Iceberg 外部表时，以键值对的形式将其添加到`PROPERTIES` 参数中。 |

您可以在 StarRocks 2.3 及更高版本中修改 Iceberg 资源的`hive.metastore.uris`和`iceberg.catalog-impl`。更多信息，请参见[ALTER RESOURCE](../sql-reference/sql-statements/Resource/ALTER_RESOURCE.md)。

##### 查看 Iceberg 资源

```SQL
SHOW RESOURCES;
```

##### 删除 Iceberg 资源

例如，删除名为`iceberg0`的资源。

```SQL
DROP RESOURCE "iceberg0";
```

删除 Iceberg 资源会使所有引用该资源的外部表不可用。但是，Apache Iceberg 中对应的数据不会被删除。如果您仍然需要查询 Apache Iceberg 中的数据，请创建一个新资源和新外部表。

#### 步骤 2：（可选）创建数据库

例如，在 StarRocks 中创建名为`iceberg_test`的数据库。

```SQL
CREATE DATABASE iceberg_test; 
USE iceberg_test; 
```

> 注意：StarRocks 中数据库的名称可以与 Apache Iceberg 中数据库的名称不同。

#### 步骤 3：创建 Iceberg 外部表

例如，在数据库`iceberg_tbl`中创建名为`iceberg_test`的 Iceberg 外部表。

```SQL
CREATE EXTERNAL TABLE `iceberg_tbl` ( 
    `id` bigint NULL, 
    `data` varchar(200) NULL 
) ENGINE=ICEBERG 
PROPERTIES ( 
    "resource" = "iceberg0", 
    "database" = "iceberg", 
    "table" = "iceberg_table" 
); 
```

下表描述了相关参数。

| **参数** | **描述**                                              |
| ------------- | ------------------------------------------------------------ |
| ENGINE        | 引擎名称。将值设置为`ICEBERG`。                 |
| resource      | 外部表引用的 Iceberg 资源的名称。 |
| database      | Iceberg 表所属的数据库名称。 |
| table         | Iceberg 表的名称。                               |

> 注意：
>
> - 外部表的名称可以与 Iceberg 表的名称不同。
>
> - 外部表的列名必须与 Iceberg 表中的列名相同。两个表的列顺序可以不同。

如果您在自定义 Catalog 中定义了配置项，并希望在查询数据时这些配置项生效，您可以在创建外部表时，以键值对的形式将配置项添加到`PROPERTIES` 参数中。例如，如果您在自定义 Catalog 中定义了配置项`custom-catalog.properties`，您可以运行以下命令创建外部表。

```SQL
CREATE EXTERNAL TABLE `iceberg_tbl` ( 
    `id` bigint NULL, 
    `data` varchar(200) NULL 
) ENGINE=ICEBERG 
PROPERTIES ( 
    "resource" = "iceberg0", 
    "database" = "iceberg", 
    "table" = "iceberg_table",
    "custom-catalog.properties" = "my_property"
); 
```

创建外部表时，您需要根据Iceberg表中的列数据类型来指定外部表中列的数据类型。下表显示了列数据类型的映射关系。

| **Iceberg 表** | **Iceberg 外部表** |
| ----------------- | -------------------------- |
| BOOLEAN           | BOOLEAN                    |
| INT               | TINYINT / SMALLINT / INT   |
| LONG              | BIGINT                     |
| FLOAT             | FLOAT                      |
| DOUBLE            | DOUBLE                     |
| DECIMAL(P, S)     | DECIMAL                    |
| DATE              | DATE / DATETIME            |
| TIME              | BIGINT                     |
| TIMESTAMP         | DATETIME                   |
| STRING            | STRING / VARCHAR           |
| UUID              | STRING / VARCHAR           |
| FIXED(L)          | CHAR                       |
| BINARY            | VARCHAR                    |
| LIST              | ARRAY                      |

StarRocks 不支持查询数据类型为 TIMESTAMPTZ、STRUCT 和 MAP 的 Iceberg 数据。

#### 步骤 4：查询 Apache Iceberg 中的数据

创建外部表后，您可以使用该外部表查询 Apache Iceberg 中的数据。

```SQL
select count(*) from iceberg_tbl;
```

## (已弃用) Hudi 外部表

从 v2.2.0 版本开始，StarRocks 支持通过 Hudi 外部表查询 Hudi 数据湖中的数据，从而实现极速数据湖分析。本文介绍如何在 StarRocks 集群中创建 Hudi 外部表，并使用 Hudi 外部表查询 Hudi 数据湖中的数据。

### 开始之前

请确保您的 StarRocks 集群已获得访问 Hive Metastore、HDFS 集群或存储桶的权限，以便注册 Hudi 表。

### 注意事项

- Hudi 外部表是只读的，仅用于查询。
- StarRocks 支持查询 Copy on Write 表和 Merge On Read 表（MOR 表从 v2.5 版本开始支持）。有关这两种表类型的区别，请参见 [表和查询类型](https://hudi.apache.org/docs/table_types/)。
- StarRocks 支持 Hudi 的以下两种查询类型：快照查询 (Snapshot Queries) 和读优化查询 (Read Optimized Queries)（Hudi 仅支持对 Merge On Read 表执行读优化查询）。不支持增量查询 (Incremental Queries)。有关 Hudi 查询类型的更多信息，请参见 [表和查询类型](https://hudi.apache.org/docs/next/table_types/#query-types)。
- StarRocks 支持 Hudi 文件的以下压缩格式：gzip、zstd、LZ4 和 Snappy。Hudi 文件的默认压缩格式为 gzip。
- StarRocks 无法同步 Hudi 托管表的 schema 变更。更多信息，请参见 [Schema 演进](https://hudi.apache.org/docs/schema_evolution/)。如果 Hudi 托管表的 schema 发生变更，您必须从 StarRocks 集群中删除关联的 Hudi 外部表，然后重新创建该外部表。

### 操作步骤

#### 步骤 1：创建和管理 Hudi 资源

您必须在 StarRocks 集群中创建 Hudi 资源。Hudi 资源用于管理您在 StarRocks 集群中创建的 Hudi 数据库和外部表。

##### 创建 Hudi 资源

执行以下语句创建名为 `hudi0` 的 Hudi 资源：

```SQL
CREATE EXTERNAL RESOURCE "hudi0" 
PROPERTIES ( 
    "type" = "hudi", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
);
```

下表描述了参数。

| 参数                | 描述                                                         |
| ------------------- | ------------------------------------------------------------ |
| type                | Hudi 资源的类型。设置为 hudi。                               |
| hive.metastore.uris | Hudi 资源连接的 Hive Metastore 的 Thrift URI。将 Hudi 资源连接到 Hive Metastore 后，您可以使用 Hive 创建和管理 Hudi 表。Thrift URI 的格式为 `<IP address of the Hive metastore>:<Port number of the Hive metastore>` 格式。默认端口号为 9083。|

从 v2.3 版本开始，StarRocks 支持修改 `hive.metastore.uris` Hudi 资源的值。更多信息，请参见 [ALTER RESOURCE](../sql-reference/sql-statements/Resource/ALTER_RESOURCE.md)。

##### 查看 Hudi 资源

执行以下语句查看 StarRocks 集群中创建的所有 Hudi 资源：

```SQL
SHOW RESOURCES;
```

##### 删除 Hudi 资源

执行以下语句删除名为 `hudi0` 的 Hudi 资源：

```SQL
DROP RESOURCE "hudi0";
```

> 注意：
>
> 删除 Hudi 资源会导致所有使用该 Hudi 资源创建的 Hudi 外部表不可用。但是，删除操作不会影响您存储在 Hudi 中的数据。如果您仍想通过 StarRocks 查询 Hudi 中的数据，则必须在 StarRocks 集群中重新创建 Hudi 资源、Hudi 数据库和 Hudi 外部表。

#### 步骤 2：创建 Hudi 数据库

执行以下语句在 StarRocks 集群中创建并打开名为 `hudi_test` 的 Hudi 数据库：

```SQL
CREATE DATABASE hudi_test; 
USE hudi_test; 
```

> 注意：
>
> 您在 StarRocks 集群中为 Hudi 数据库指定的名称无需与 Hudi 中关联的数据库名称相同。

#### 步骤 3：创建 Hudi 外部表

执行以下语句创建名为 `hudi_tbl` 的 Hudi 外部表，在 `hudi_test` Hudi 数据库中：

```SQL
CREATE EXTERNAL TABLE `hudi_tbl` ( 
    `id` bigint NULL, 
    `data` varchar(200) NULL 
) ENGINE=HUDI 
PROPERTIES ( 
    "resource" = "hudi0", 
    "database" = "hudi", 
    "table" = "hudi_table" 
); 
```

下表描述了参数。

| 参数 | 描述 |
| --------- | ------------------------------------------------------------ |
| ENGINE | Hudi 外部表的查询引擎。将值设置为 `HUDI`。|
| resource | StarRocks 集群中 Hudi 资源的名称。|
| database | Hudi 外部表所属的 Hudi 数据库在 StarRocks 集群中的名称。|
| table | Hudi 外部表关联的 Hudi 托管表。|

> 注意：
>
> - 您为 Hudi 外部表指定的名称无需与关联的 Hudi 托管表名称相同。
>
> - Hudi 外部表中的列必须与关联的 Hudi 托管表中的对应列具有相同的名称，但顺序可以不同。
>
> - 您可以从关联的 Hudi 托管表中选择部分或全部列，并在 Hudi 外部表中仅创建选定的列。下表列出了 Hudi 支持的数据类型与 StarRocks 支持的数据类型之间的映射关系。

| Hudi 支持的数据类型 | StarRocks 支持的数据类型 |
| ---------------------------- | --------------------------------- |
| BOOLEAN | BOOLEAN |
| INT | TINYINT/SMALLINT/INT |
| DATE | DATE |
| TimeMillis/TimeMicros | TIME |
| TimestampMillis/TimestampMicros| DATETIME |
| LONG | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| STRING | CHAR/VARCHAR |
| ARRAY | ARRAY |
| DECIMAL | DECIMAL |

> **注意**
>
> StarRocks 不支持查询 STRUCT 或 MAP 类型的数据，也不支持查询 Merge On Read 表中的 ARRAY 类型数据。

#### 步骤 4：从 Hudi 外部表查询数据

创建与特定 Hudi 托管表关联的 Hudi 外部表后，您无需将数据加载到 Hudi 外部表中。要从 Hudi 查询数据，请执行以下语句：

```SQL
SELECT COUNT(*) FROM hudi_tbl;
```

## （已弃用）MySQL 外部表

在星型模型中，数据通常分为维度表和事实表。维度表数据量较小，但涉及 UPDATE 操作。目前 StarRocks 不支持直接的 UPDATE 操作（可以通过 Unique Key 表实现更新）。在某些场景下，您可以将维度表存储在 MySQL 中以直接读取数据。

要查询 MySQL 数据，您必须在 StarRocks 中创建外部表并将其映射到 MySQL 数据库中的表。创建表时需要指定 MySQL 连接信息。

```sql
CREATE EXTERNAL TABLE mysql_external_table
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=mysql
PROPERTIES
(
    "host" = "127.0.0.1",
    "port" = "3306",
    "user" = "mysql_user",
    "password" = "mysql_passwd",
    "database" = "mysql_db_test",
    "table" = "mysql_table_test"
);
```

参数：

- **host**：MySQL 数据库的连接地址
- **port**：MySQL 数据库的端口号
- **user**：登录 MySQL 的用户名
- **password**：登录 MySQL 的密码
- **database**：MySQL 数据库的名称
- **表**：MySQL 数据库中表的名称
