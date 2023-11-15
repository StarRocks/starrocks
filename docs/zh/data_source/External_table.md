# 外部表

StarRocks 支持以外部表的形式，接入其他数据源。外部表指的是保存在其他数据源中的数据表，而 StartRocks 只保存表对应的元数据，并直接向外部表所在数据源发起查询。目前 StarRocks 已支持的第三方数据源包括 MySQL、Elasticsearch、Hive、StarRocks、Apache Iceberg 和 Apache Hudi。**对于 StarRocks 数据源，现阶段只支持 Insert 写入，不支持读取，对于其他数据源，现阶段只支持读取，还不支持写入**。

<br/>

## MySQL外部表

星型模型中，数据一般划分为维度表和事实表。维度表数据量少，但会涉及 UPDATE 操作。目前 StarRocks 中还不直接支持 UPDATE 操作（可以通过 Unique 数据模型实现），在一些场景下，可以把维度表存储在 MySQL 中，查询时直接读取维度表。

<br/>

在使用MySQL的数据之前，需在StarRocks创建外部表，与之相映射。StarRocks中创建MySQL外部表时需要指定MySQL的相关连接信息，如下图。

~~~sql
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
~~~

参数说明：

* **host**：MySQL的连接地址
* **port**：MySQL的连接端口号
* **user**：MySQL登陆的用户名
* **password**：MySQL登陆的密码
* **database**：MySQL相关数据库名
* **table**：MySQL相关数据表名

<br/>

## Elasticsearch 外部表

StarRocks 与 Elasticsearch 都是目前流行的分析系统，StarRocks 强于大规模分布式计算，Elasticsearch 擅长全文检索。StarRocks 支持 Elasticsearch 访问的目的，就在于将这两种能力结合，提供更完善的一个 OLAP 解决方案。

### 建表示例

~~~sql
CREATE EXTERNAL TABLE elastic_search_external_table
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=ELASTICSEARCH
PARTITION BY RANGE(k1)
()
PROPERTIES 
(
    "hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
    "user" = "root",
    "password" = "root",
    "index" = "tindex",
    "type" = "doc"
);
~~~

参数说明：

* `hosts`：Elasticsearch 集群连接地址，用于获取 Elasticsearch 版本号以及索引的分片分布信息，可指定一个或多个。StarRocks 是根据 `GET /_nodes/http` API 返回的地址和 Elasticsearch 集群进行通讯，所以 `hosts` 参数值必须和 `GET /_nodes/http` 返回的地址一致，否则可能导致 BE 无法和 Elasticsearch 集群进行正常的通讯。
* `user`：开启 basic 认证的 Elasticsearch 集群的用户名，需要确保该用户有访问 **/*cluster/state/* nodes/http** 等路径权限和对索引的读取权限。
* `password`：对应用户的密码信息。
* `index`：StarRocks 中的表对应的 Elasticsearch 的索引名字，可以是索引的别名。
* `type`：指定索引的类型，默认是 `_doc`。如果您要查询的是数据是在 Elasticsearch 8 及以上版本，那么在 StarRocks 中创建外部表时就不需要配置该参数，因为 Elasticsearch 8 以及上版本已经移除了 mapping types。
* `transport`：内部保留，默认为 `http`。

创建外部表时，需根据 Elasticsearch 的字段类型指定 StarRocks 中外部表的列类型，具体映射关系如下：

| **Elasticsearch**          | **StarRocks**                   |
| -------------------------- | --------------------------------|
| BOOLEAN                    | BOOLEAN                         |
| BYTE                       | TINYINT/SMALLINT/INT/BIGINT     |
| SHORT                      | SMALLINT/INT/BIGINT             |
| INTEGER                    | INT/BIGINT                      |
| LONG                       | BIGINT                          |
| FLOAT                      | FLOAT                           |
| DOUBLE                     | DOUBLE                          |
| KEYWORD                    | CHAR/VARCHAR                    |
| TEXT                       | CHAR/VARCHAR                    |
| DATE                       | DATE/DATETIME                   |
| NESTED                     | CHAR/VARCHAR                    |
| OBJECT                     | CHAR/VARCHAR                    |

> **说明**
>
> StarRocks 会通过 JSON 相关函数读取嵌套字段。

### 谓词下推

StarRocks 支持对 Elasticsearch 表进行谓词下推，把过滤条件推给 Elasticsearch 进行执行，让执行尽量靠近存储，提高查询性能。目前支持下推的算子如下表：

| **SQL syntax**   | **Elasticsearch syntax**   |
| ---------------- | ---------------------------|
| `=`                | term query                 |
| `in`               | terms query                |
| `\>=,  <=, >, <`   | range                      |
| `and`              | bool.filter                |
| `or`               | bool.should                |
| `not`              | bool.must_not              |
| `not in`           | bool.must_not + terms      |
| `esquery`          | ES Query DSL               |

### 查询示例

通过 esquery 函数将一些无法用 SQL 表述的 Elasticsearch query，如 match 和 geoshape 等下推给 Elasticsearch 进行过滤处理。esquery 的第一个列名参数用于关联 index，第二个参数是 Elasticsearch 的基本 Query DSL 的 json 表述，使用花括号（`{}`）包含，**json 的 root key 有且只能有一个**，如 match、geo_shape 和 bool 等。

* match 查询

   ~~~sql
   select * from es_table where esquery(k4, '{
      "match": {
         "k4": "StarRocks on elasticsearch"
      }
   }');
   ~~~

* geo 相关查询

   ~~~sql
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
   ~~~

* bool 查询

   ~~~sql
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
   ~~~

### 注意事项

* Elasticsearch 5.x 之前和之后的数据扫描方式不同，目前 StarRocks 只支持查询 5.x 之后的版本。
* 支持查询使用 HTTP Basic 认证的 Elasticsearch 集群。
* 一些通过 StarRocks 的查询会比直接请求 Elasticsearch 会慢很多，比如 count 相关查询。这是因为 Elasticsearch 内部会直接读取满足条件的文档个数相关的元数据，不需要对真实的数据进行过滤操作，使得 count 的速度非常快。

## Hive外表

### 创建Hive资源

StarRocks使用Hive资源来管理使用到的Hive集群相关配置，如Hive Metastore地址等，一个Hive资源对应一个Hive集群。创建Hive外表的时候需要指定使用哪个Hive资源。

~~~sql
-- 创建一个名为hive0的Hive资源
CREATE EXTERNAL RESOURCE "hive0"
PROPERTIES (
  "type" = "hive",
  "hive.metastore.uris" = "thrift://10.10.44.98:9083"
);

-- 查看StarRocks中创建的资源
SHOW RESOURCES;

-- 删除名为hive0的资源
DROP RESOURCE "hive0";
~~~

<br/>

### 创建数据库

~~~sql
CREATE DATABASE hive_test;
USE hive_test;
~~~

<br/>

### 创建Hive外表

~~~sql
-- 语法
CREATE EXTERNAL TABLE table_name (
  col_name col_type [NULL | NOT NULL] [COMMENT "comment"]
) ENGINE=HIVE
PROPERTIES (
  "key" = "value"
);

-- 例子：创建hive0资源对应的Hive集群中rawdata数据库下的profile_parquet_p7表的外表
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

~~~

说明：

* 外表列：
  * 列名需要与Hive表一一对应。
  * 列顺序与Hive表的关系。如果Hive表的存储格式为Parquet或ORC，则列的顺序**不需要**与Hive表一致。如果Hive表的存储格式为CSV，则列的顺序**需要**与Hive表一致。
  * 可以只选择Hive表中的**部分列**，但**分区列**必须要全部包含。
  * 外表的分区列无需通过partition by语句指定，需要与普通列一样定义到描述列表中。不需要指定分区信息，StarRocks会自动从Hive同步。
  * ENGINE指定为HIVE。
* PROPERTIES属性：
  * **hive.resource**：指定使用的Hive资源。
  * **database**：指定Hive中的数据库。
  * **table**：指定Hive中的表，**不支持view**。
* 创建外部表时，需根据 Hive 表列类型指定 StarRocks 中外部表列类型，具体映射关系如下：

| **Hive**      | **StarRocks**                                                |
| ------------- | ------------------------------------------------------------ |
| INT/INTEGER   | INT                                                          |
| BIGINT        | BIGINT                                                       |
| TIMESTAMP     | DATETIME <br />注意 TIMESTAMP 转成 DATETIME会损失精度和时区信息，并根据 sessionVariable 中的时区转成无时区 DATETIME。 |
| STRING        | VARCHAR                                                      |
| VARCHAR       | VARCHAR                                                      |
| CHAR          | CHAR                                                         |
| DOUBLE        | DOUBLE                                                       |
| FLOAT         | FLOAT                                                        |
| DECIMAL       | DECIMAL                                                      |
| BOOLEAN       | BOOLEAN                                                      |

说明：

* Hive 表 Schema 变更 **不会自动同步**，需要在 StarRocks 中重建 Hive 外表。
* 支持 Hive 的存储格式为 Parquet 和 ORC。
* 压缩格式支持 Snappy 和 LZ4。

<br/>

### 查询Hive外表

~~~sql
-- 查询profile_wos_p7的总行数
select count(*) from profile_wos_p7;
~~~

<br/>

### 配置

* fe配置文件路径为fe/conf，如果需要自定义hadoop集群的配置可以在该目录下添加配置文件，例如：hdfs集群采用了高可用的nameservice，需要将hadoop集群中的hdfs-site.xml放到该目录下，如果hdfs配置了viewfs，需要将core-site.xml放到该目录下。
* be配置文件路径为be/conf，如果需要自定义hadoop集群的配置可以在该目录下添加配置文件，例如：hdfs集群采用了高可用的nameservice，需要将hadoop集群中的hdfs-site.xml放到该目录下，如果hdfs配置了viewfs，需要将core-site.xml放到该目录下。
* be所在的机器也需要配置JAVA_HOME，一定要配置成jdk环境，不能配置成jre环境
* kerberos 支持：
  1. 在所有的fe/be机器上用`kinit -kt keytab_path principal`登陆，该用户需要有访问hive和hdfs的权限。kinit命令登陆是有实效性的，需要将其放入crontab中定期执行。
  2. 把hadoop集群中的hive-site.xml/core-site.xml/hdfs-site.xml放到fe/conf下，把core-site.xml/hdfs-site.xml放到be/conf下。
  3. 在fe/conf/fe.conf文件中的JAVA_OPTS/JAVA_OPTS_FOR_JDK_9选项加上 -Djava.security.krb5.conf:/etc/krb5.conf，/etc/krb5.conf是krb5.conf文件的路径，可以根据自己的系统调整。
  4. resource中的uri地址一定要使用域名，并且相应的hive和hdfs的域名与ip的映射都需要配置到/etc/hosts中。
* S3 支持:
  2.0.1及之后的版本默认不开启此功能，可以按照以下步骤配置后使用。
  1. 下载[依赖库](https://cdn-thirdparty.starrocks.com/hive_s3_jar.tar.gz)并添加到fe/lib/和be/lib/hadoop/hdfs/路径下。
  2. 在fe/conf/core-site.xml和be/conf/core-site.xml中加入如下配置，并重启fe和be。

~~~xml
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
~~~

  1. `fs.s3a.access.key` 指定aws的access key id
  2. `fs.s3a.secret.key` 指定aws的secret access key
  3. `fs.s3a.endpoint` 指定aws的区域
  4. `fs.s3a.connection.maximum` 配置最大链接数，如果查询过程中有报错`Timeout waiting for connection from poll`，可以适当调高该参数

### 缓存更新

Hive的partition信息以及partition对应的文件信息都会缓存在starrocks中。您可以在 fe.conf 文件中通过设置hive_meta_cache_refresh_interval_s参数修改缓存自动刷新的间隔时间（默认值为7200, 单位：秒），也可以通过设置hive_meta_cache_ttl_s参数修改缓存的失效时间（默认值为86400，单位：秒）。修改后需重启 FE 生效。

**手动更新元数据缓存**

  1. hive中新增或者删除分区时，需要刷新**表**的元数据信息：`REFRESH EXTERNAL TABLE hive_t`，其中hive_t是starrocks中的外表名称。
  2. hive中向某些partition中新增数据时，需要**指定partition**进行刷新：`REFRESH EXTERNAL TABLE hive_t PARTITION ('k1=01/k2=02', 'k1=03/k2=04')`，其中hive_t是starrocks中的外表名称，'k1=01/k2=02'、 'k1=03/k2=04'是hive中的partition名称。

**自动增量更新元数据缓存**
自动增量更新元数据缓存主要是通过定期消费 Hive Metastore 的 event 来实现，新增分区以及分区新增数据无需通过手动执行 refresh 来更新。用户需要在 Hive Metastore 端开启元数据 Event 机制。相比 Loading Cache 的自动刷新机制，自动增量更新性能更好，建议用户开启该功能。开启该功能后，Loading Cache 的自动刷新机制将不再生效。

* Hive Metastore 开启 event 机制

   用户需要在$HiveMetastore/conf/hive-site.xml 中添加如下配置，并重启 Hive Metastore. 以下配置为 Hive Metastore 3.1.2 版本的配置，用户可以将以下配置先拷贝到 hive-site.xml 中进行验证，因为在 Hive Metastore 中配置不存在的参数只会提示 WARN 信息，不会抛出任何异常。

~~~xml
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
~~~

* StarRocks 开启自动增量元数据同步

    用户需要在$FE_HOME/conf/fe.conf 中添加如下配置并重启 FE.
    `enable_hms_events_incremental_sync=true`
    自动增量元数据同步相关配置如下，如无特殊需求，无需修改。

   | 参数值                             | 说明                                      | 默认值 |
   | --- | --- | ---|
   | enable_hms_events_incremental_sync | 是否开启元数据自动增量同步功能            | false |
   | hms_events_polling_interval_ms     | StarRocks 拉取 Hive Metastore Event 事件间隔 | 5 秒 |
   | hms_events_batch_size_per_rpc      | StarRocks 每次拉取 Event 事件的最大数量      | 500 |
   | enable_hms_parallel_process_evens  | 对接收的 Events 是否并行处理                | true |
   | hms_process_events_parallel_num    | 处理 Events 事件的并发数                    | 4 |

* 注意事项
  * 不同版本 Hive Metastore 的 Events 事件可能不同，且上述开启 HiveMetastore Event 机制的配置在不同版本也存在不同。使用时相关配置可根据实际版进行适当调整。当前已经验证可以开启 Hive Metastore Event 机制的版本有 2.X 和 3.X。用户可以在 FE 日志中搜索 "event id" 来验证 event 是否开启成功，如果没有开启成功，event id 始终保持为 0。如果无法判断是否成功开启 Event 机制，请在 StarRocks 用户交流群中联系值班同学进行排查。
  * 当前 Hive 元数据缓存模式为懒加载，即：如果 Hive 新增了分区，StarRocks 只会将新增分区的 partition key 进行缓存，不会立即缓存该分区的文件信息。只有当查询该分区时或者用户手动执行 refresh 分区操作时，该分区的文件信息才会被加载。StarRocks 首次缓存该分区统计信息后，该分区后续的元数据变更就会自动同步到 StarRocks 中。
  * 手动执行缓存方式执行效率较低，相比之下自动增量更新性能开销较小，建议用户开启该功能进行更新缓存。
  * 当前自动更新不支持 add/drop column 等 schema change 操作，Hive 表结构如有更改，需要重新创建 Hive 外表。Hive 外表支持 Schema change 将会在近期推出，敬请期待。

## StarRocks 外部表

1.19 版本开始，StarRocks 支持将数据通过外表方式写入另一个 StarRocks 集群的表中。这可以解决用户的读写分离需求，提供更好的资源隔离。用户需要首先在目标集群上创建一张目标表，然后在源 StarRocks 集群上创建一个 Schema 信息一致的外表，并在属性中指定目标集群和表的信息。

通过 insert into 写入数据至 StarRocks 外表, 可以实现如下目标:

* 集群间的数据同步
* 在外表集群计算结果写入目标表集群，并在目标表集群提供查询服务，实现读写分离

以下是创建目标表和外表的实例：

~~~sql
# 在目标集群上执行
CREATE TABLE t
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=olap
DISTRIBUTED BY HASH(k1) BUCKETS 10;

# 在外表集群上执行
CREATE EXTERNAL TABLE external_t
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=olap
DISTRIBUTED BY HASH(k1) BUCKETS 10
PROPERTIES
(
    "host" = "127.0.0.1",
    "port" = "9020",
    "user" = "user",
    "password" = "passwd",
    "database" = "db_test",
    "table" = "t"
);

# 向外表插入数据,线上推荐使用第二种方式
insert into external_t values ('2020-10-11', 1, 1, 'hello', '2020-10-11 10:00:00');

insert into external_t select * from other_table;
~~~

其中：

* **EXTERNAL**：该关键字指定创建的是StarRocks外表
* **host**：该属性描述目标表所属StarRocks集群Leader FE的IP地址
* **port**：该属性描述目标表所属StarRocks集群Leader FE的RPC访问端口，该值可参考配置fe/fe.conf中的rpc_port配置取值
* **user**：该属性描述目标表所属StarRocks集群的访问用户名
* **password**：该属性描述目标表所属StarRocks集群的访问密码
* **database**：该属性描述目标表所属数据库名称
* **table**：该属性描述目标表名称

目前StarRocks外表使用上有以下限制：

* 仅可以在外表上执行insert into 和show create table操作，不支持其他数据写入方式，也不支持查询和DDL
* 创建外表语法和创建普通表一致，但其中的列名等信息请保持同其对应的目标表一致
* 外表会周期性从目标表同步元信息（同步周期为10秒），在目标表执行的DDL操作可能会延迟一定时间反应在外表上

## Apache Iceberg外表

StarRocks支持通过外表的方式查询Apache Iceberg数据湖中的数据，帮助您实现对数据湖的极速分析。本文介绍如何在StarRock创建外表，查询Apache Iceberg中的数据。

### 前提条件

请确认StarRocks有权限访问Apache Iceberg对应的Hive Metastore、HDFS集群或者对象存储的Bucket。

### 注意事项

* Iceberg外表是只读的，只能用于查询操作。
* 支持Iceberg的表格式为V1（Copy on write表），暂不支持为 V2（Merge on read表）。V1和V2之间的更多区别，请参见[Apache Iceberg官网](https://iceberg.apache.org/#spec/#format-versioning)。
* 支持Iceberg文件的压缩格式为GZIP（默认值），ZSTD，LZ4和SNAPPY。
* 仅支持Iceberg的Catalog类型为Hive Catalog，数据存储格式为Parquet和ORC。
* StarRocks暂不⽀持同步Iceberg中的[schema evolution](https://iceberg.apache.org/#evolution#schema-evolution)，如果Iceberg表schema evolution发生变更，您需要在StarRocks中删除对应Iceberg外表并重新建立。

### 操作步骤

#### 步骤一：创建和管理Iceberg资源

您需要提前在StarRocks中创建Iceberg资源，用于管理在StarRocks中创建的Iceberg数据库和外表。

执行如下命令，创建一个名为`iceberg0`的Iceberg资源。

~~~sql
CREATE EXTERNAL RESOURCE "iceberg0" 
PROPERTIES ( 
"type" = "iceberg", 
"starrocks.catalog-type"="HIVE", 
"iceberg.catalog.hive.metastore.uris"="thrift://192.168.0.81:9083" 
);
~~~

|  参数   | 说明  |
|  ----  | ----  |
| type  | 资源类型，固定取值为**iceberg**。 |
| starrocks.catalog-type  | Iceberg的Catalog类型。目前仅支持为Hive Catalog，取值为HIVE。 |
| iceberg.catalog.hive.metastore.uris | Hive Metastore的thrift URI。<br />Iceberg通过创建Hive Catalog，连接Hive Metastore，以创建并管理表。您需要传入该Hive Metastore的thrift URI。格式为**`thrift://<Hive Metadata的IP地址>:<端口号>`**，端口号默认为9083。 |

执行如下命令，查看StarRocks中的所有Iceberg资源。

~~~sql
SHOW RESOURCES;
~~~~

执行如下命令，删除名为`iceberg0`的Iceberg资源。

~~~sql
DROP RESOURCE "iceberg0";
~~~~

> 删除Iceberg资源会导致其包含的所有Iceberg外表不可用，但Apache Iceberg中的数据并不会丢失。如果您仍需要通过StarRocks查询Iceberg的数据，请重新创建Iceberg资源，Iceberg数据库和外表。

#### 步骤二：创建Iceberg数据库

执行如下命令，在StarRocks中创建并进入名为`iceberg_test`的Iceberg数据库。

~~~sql
CREATE DATABASE iceberg_test; 

USE iceberg_test; 
~~~

> 库名无需与Iceberg的实际库名保持一致。

#### 步骤三：创建Iceberg外表

执行如下命令，在Iceberg数据库`iceberg_test`中，创建一张名为`iceberg_tbl`的Iceberg外表。

~~~sql
CREATE EXTERNAL TABLE `iceberg_tbl` ( 
`id` bigint NULL, 
`data` varchar(200) NULL 
) ENGINE=ICEBERG 
PROPERTIES ( 
"resource" = "iceberg0", 
"database" = "iceberg", 
"table" = "iceberg_table" 
); 
~~~

* 相关参数说明，请参见下表：

| **参数**     | **说明**                       |
| ------------ | ------------------------------ |
| **ENGINE**   | 固定为**ICEBERG**，无需更改。  |
| **resource** | StarRocks中Iceberg资源的名称。 |
| **database** | Iceberg中的数据库名称。        |
| **table**    | Iceberg中的数据表名称。        |

* 表名无需与Iceberg的实际表名保持一致。
* 列名需要与Iceberg的实际列名保持一致，列的顺序无需保持一致。
* 您可以按照业务需求选择Iceberg表中的全部或部分列。创建外部表时，需根据 Iceberg 表的列类型指定 StarRocks 中外部表的列类型，具体映射关系如下：

| **Iceberg**    | **StarRocks**            |
| -------------- | ------------------------ |
| BOOLEAN        | BOOLEAN                  |
| INT            | TINYINT/SMALLINT/INT     |
| LONG           | BIGINT                   |
| FLOAT          | FLOAT                    |
| DOUBLE         | DOUBLE                   |
| DECIMAL(P, S)  | DECIMAL                  |
| DATE           | DATE/DATETIME            |
| TIME           | BIGINT                   |
| TIMESTAMP      | DATETIME                 |
| STRING         | STRING/VARCHAR           |
| UUID           | STRING/VARCHAR           |
| FIXED(L)       | CHAR                     |
| BINARY         | VARCHAR                  |

StarRocks 不支持查询以下类型的数据： TIMESTAMPTZ、STRUCT、LIST 和 MAP。

#### 步骤四：查询Iceberg外表

创建Iceberg外表后，无需导入数据，执行如下命令，即可查询Iceberg的数据。

~~~sql
select count(*) from iceberg_tbl;
~~~
