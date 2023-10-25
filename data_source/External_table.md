# 外部表

StarRocks 支持以外部表 (external table) 的形式，接入其他数据源。外部表指的是保存在其他数据源中的数据表，而 StartRocks 只保存表对应的元数据，并直接向外部表所在数据源发起查询。目前 StarRocks 已支持的第三方数据源包括 MySQL、Elasticsearch、Hive、StarRocks、Apache Iceberg 和 Apache Hudi。**对于 StarRocks 数据源，现阶段只支持 Insert 写入，不支持读取，对于其他数据源，现阶段只支持读取，还不支持写入**。

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

* **host**：MySQL 的连接地址
* **port**：MySQL 的连接端口号
* **user**：MySQL 登录的用户名
* **password**：MySQL 登录的密码
* **database**：MySQL 相关数据库名
* **table**：MySQL 相关数据表名

<br/>

## Elasticsearch 外部表

如要查询 Elasticsearch 中的数据，需要在 StarRocks 中创建 Elasticsearch 外部表，并将外部表与中需要查询的 Elasticsearch 表建立映射。StarRocks 与 Elasticsearch 都是目前流行的分析系统。StarRocks 擅长大规模分布式计算，且支持通过外部表查询 Elasticsearch。Elasticsearch 擅长全文检索。两者结合提供了一个更完善的 OLAP 解决方案。

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
| =                | term query                 |
| in               | terms query                |
| \>=,  <=, >, <   | range                      |
| and              | bool.filter                |
| or               | bool.should                |
| not              | bool.must_not              |
| not in           | bool.must_not + terms      |
| esquery          | ES Query DSL               |

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
| INT/INTEGER   | INT                                                          |                                                         |
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

* fe配置文件路径为$FE_HOME/conf，如果需要自定义hadoop集群的配置可以在该目录下添加配置文件，例如：hdfs集群采用了高可用的nameservice，需要将hadoop集群中的hdfs-site.xml放到该目录下，如果hdfs配置了viewfs，需要将core-site.xml放到该目录下。
* be配置文件路径为$BE_HOME/conf，如果需要自定义hadoop集群的配置可以在该目录下添加配置文件，例如：hdfs集群采用了高可用的nameservice，需要将hadoop集群中的hdfs-site.xml放到该目录下，如果hdfs配置了viewfs，需要将core-site.xml放到该目录下。
* be所在的机器也需要配置JAVA_HOME，一定要配置成jdk环境，不能配置成jre环境
* kerberos 支持
  1. 在所有的fe/be机器上用`kinit -kt keytab_path principal`登陆，该用户需要有访问hive和hdfs的权限。kinit命令登陆是有实效性的，需要将其放入crontab中定期执行。
  2. 把hadoop集群中的hive-site.xml/core-site.xml/hdfs-site.xml放到$FE_HOME/conf下，把core-site.xml/hdfs-site.xml放到$BE_HOME/conf下。
  3. 在$FE_HOME/conf/fe.conf文件中的JAVA_OPTS/JAVA_OPTS_FOR_JDK_9选项加上 -Djava.security.krb5.conf:/etc/krb5.conf，/etc/krb5.conf是krb5.conf文件的路径，可以根据自己的系统调整。
  4. resource中的uri地址一定要使用域名，并且相应的hive和hdfs的域名与ip的映射都需要配置到/etc/hosts中。

#### AWS S3/Tencent Cloud COS支持

一. 下载[依赖库](https://cdn-thirdparty.starrocks.com/hive_s3_jar.tar.gz)并添加到$FE_HOME/lib/路径下。

二. 在 $FE_HOME/conf/core-site.xml 中加入如下配置。

~~~xml
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
      <value>s3.us-west-2.amazonaws.com</value>
   </property>
   <property>
     <name>fs.s3a.connection.maximum</name>
     <value>500</value>
   </property>
</configuration>
~~~

* `fs.s3a.access.key` 指定aws的access key id
* `fs.s3a.secret.key` 指定aws的secret access key
* `fs.s3a.endpoint` 指定aws的区域
* `fs.s3a.connection.maximum` 配置最大链接数，如果查询过程中有报错`Timeout waiting for connection from poll`，可以适当调高该参数

二. 在$BE_HOME/conf/be.conf中加入如下配置。

* `object_storage_access_key_id` 与FE端core-site.xml配置`fs.s3a.access.key`相同
* `object_storage_secret_access_key` 与FE端core-site.xml配置`fs.s3a.secret.key`相同
* `object_storage_endpoint` 与FE端core-site.xml配置`fs.s3a.endpoint`相同
* `object_storage_region` 只有腾讯COS需要额外添加该配置项。如：ap-beijing****

三. 重启FE，BE。

#### Aliyun OSS 支持

一. 在$FE_HOME/conf/core-site.xml中加入如下配置。

~~~xml
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
        <value>xxx</value>
    </property>
    <property>
        <name>fs.oss.accessKeySecret</name>
        <value>xxx</value>
    </property>
    <property>
        <name>fs.oss.endpoint</name>
        <!-- 以下以北京地域为例，其他地域请根据实际情况替换。 -->
        <value>oss-cn-beijing.aliyuncs.com</value>
    </property>
</configuration>
~~~

* `fs.oss.accessKeyId` 指定阿里云账号或RAM用户的AccessKey ID，获取方式，请参见[获取AccessKey](https://help.aliyun.com/document_detail/53045.htm?spm=a2c4g.11186623.0.0.128b4b7896DD4W#task968)。
* `fs.oss.accessKeySecret` 指定阿里云账号或RAM用户的AccessKey Secret，获取方式，请参见[获取AccessKey](https://help.aliyun.com/document_detail/53045.htm?spm=a2c4g.11186623.0.0.128b4b7896DD4W#task968)。
* `fs.oss.endpoint` 指定相关OSS Bucket所在地域对应的Endpoint。
    您可以通过以下方式查询Endpoint：

  * 根据Endpoint与地域的对应关系进行查找，请参见[访问域名和数据中心](https://help.aliyun.com/document_detail/31837.htm#concept-zt4-cvy-5db)。
  * 您可以登录[阿里云OSS管理控制台](https://oss.console.aliyun.com/index?spm=a2c4g.11186623.0.0.11d24772leoEEg#/)，进入Bucket 概览页，Bucket域名examplebucket.oss-cn-hangzhou.aliyuncs.com的后缀部分oss-cn-hangzhou.aliyuncs.com，即为该Bucket的外网Endpoint。

二. 在$BE_HOME/conf/be.conf中加入如下配置。

* `object_storage_access_key_id` 与FE端core-site.xml配置`fs.oss.accessKeyId`相同
* `object_storage_secret_access_key` 与FE端core-site.xml配置`fs.oss.accessKeySecret`相同
* `object_storage_endpoint` 与FE端core-site.xml配置`fs.oss.endpoint`相同

三. 重启FE，BE。

### 缓存更新

Hive Table的Partition统计信息以及Partition下面的文件信息可以缓存到StarRocks FE中，缓存的内存结构为Guava LoadingCache。您可以在 fe.conf 文件中通过设置`hive_meta_cache_refresh_interval_s`参数修改缓存自动刷新的间隔时间（默认值为`7200`, 单位：秒），也可以通过设置`hive_meta_cache_ttl_s`参数修改缓存的失效时间（默认值为`86400`，单位：秒）。修改后需重启 FE 生效。

#### 手动更新元数据缓存

* 手动刷新元数据信息：
  1. hive中新增或者删除分区时，需要刷新**表**的元数据信息：`REFRESH EXTERNAL TABLE hive_t`，其中hive_t是starrocks中的外表名称。
  2. hive中向某些partition中新增数据时，需要**指定partition**进行刷新：`REFRESH EXTERNAL TABLE hive_t PARTITION ('k1=01/k2=02', 'k1=03/k2=04')`，其中hive_t是starrocks中的外表名称，'k1=01/k2=02'、 'k1=03/k2=04'是hive中的partition名称。

#### 自动增量更新元数据缓存

自动增量更新元数据缓存主要是通过定期消费Hive Metastore的event来实现，新增分区以及分区新增数据无需通过手动执行refresh来更新。用户需要在Hive Metastore端开启元数据Event机制。相比Loading Cache的自动刷新机制，自动增量更新性能更好，建议用户开启该功能。开启该功能后，Loading Cache的自动刷新机制将不再生效。

* Hive Metastore开启event机制

   用户需要在$HiveMetastore/conf/hive-site.xml中添加如下配置，并重启Hive Metastore. 以下配置为Hive Metastore 3.1.2版本的配置，用户可以将以下配置先拷贝到hive-site.xml中进行验证，因为在Hive Metastore中配置不存在的参数只会提示WARN信息，不会抛出任何异常。

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

* StarRocks开启自动增量元数据同步

    用户需要在$FE_HOME/conf/fe.conf中添加如下配置并重启FE.
    `enable_hms_events_incremental_sync=true`
    自动增量元数据同步相关配置如下，如无特殊需求，无需修改。

   | 参数值                             | 说明                                      | 默认值 |
   | --- | --- | ---|
   | enable_hms_events_incremental_sync | 是否开启元数据自动增量同步功能            | false |
   | hms_events_polling_interval_ms     | StarRocks拉取Hive Metastore Event事件间隔 | 5秒 |
   | hms_events_batch_size_per_rpc      | StarRocks每次拉取Event事件的最大数量      | 500 |
   | enable_hms_parallel_process_evens  | 对接收的Events是否并行处理                | true |
   | hms_process_events_parallel_num    | 处理Events事件的并发数                    | 4 |

* 注意事项
  * 不同版本 Hive Metastore 的 Events 事件可能不同，且上述开启 HiveMetastore Event 机制的配置在不同版本也存在不同。使用时相关配置可根据实际版进行适当调整。当前已经验证可以开启 Hive Metastore Event 机制的版本有 2.X 和 3.X。用户可以在 FE 日志中搜索 "event id" 来验证 event 是否开启成功，如果没有开启成功，event id 始终保持为 0。如果无法判断是否成功开启 Event 机制，请在 StarRocks 用户交流群中联系值班同学进行排查。
  * 当前 Hive 元数据缓存模式为懒加载，即：如果 Hive 新增了分区，StarRocks 只会将新增分区的 partition key 进行缓存，不会立即缓存该分区的文件信息。只有当查询该分区时或者用户手动执行 refresh 分区操作时，该分区的文件信息才会被加载。StarRocks 首次缓存该分区统计信息后，该分区后续的元数据变更就会自动同步到 StarRocks 中。
  * 手动执行缓存方式执行效率较低，相比之下自动增量更新性能开销较小，建议用户开启该功能进行更新缓存。
  * 当前自动更新不支持add/drop column等schema change操作，Hive表结构如有更改，需要重新创建Hive外表。Hive外表支持Schema change将会在近期推出，敬请期待。

## StarRocks外部表

1.19版本开始，StarRocks支持将数据通过外表方式写入另一个StarRocks集群的表中。这可以解决用户的读写分离需求，提供更好的资源隔离。用户需要首先在目标集群上创建一张目标表，然后在源StarRocks集群上创建一个Schema信息一致的外表，并在属性中指定目标集群和表的信息。

通过insert into 写入数据至StarRocks外表,可以实现如下目标:

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

StarRocks支持通过外表的方式查询Apache Iceberg数据湖中的数据，帮助您实现对数据湖的极速分析。本文介绍如何在StarRocks创建外表，查询Apache Iceberg中的数据。

### 前提条件

在创建 JDBC 资源以及首次查询 JDBC 外部表时，FE、BE 节点需要下载 JDBC 驱动程序，因此在这两个阶段，FE、BE 节点所在机器必须能够访问用于下载 JDBC 驱动程序 JAR 包的 URL，该 URL 由创建 JDBC 资源中的配置项 `driver_url` 指定。

### **创建和**管理**JDBC 资源**

#### 创建 JDBC 资源

您需要提前在 StarRocks 中创建 JDBC 资源，用于管理数据库的相关连接信息。这里的数据库是指支持 JDBC 的数据库，以下简称为“目标数据库”。创建资源后，即可使用该资源创建外部表。

执行如下语句，创建一个名为 `jdbc0` 的 JDBC 资源：

~~~SQL
create external resource jdbc0
properties (
    "type"="jdbc",
    "user"="postgres",
    "password"="changeme",
    "jdbc_uri"="jdbc:postgresql://127.0.0.1:5432/jdbc_test",
    "driver_url"="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar",
    "driver_class"="org.postgresql.Driver"
);
~~~

`properties` 的必填配置项：

* `type`：资源类型，固定取值为 `jdbc`。

* `user`：目标数据库的用户名。

* `password`：目标数据库用户的登录密码。

* `jdbc_uri`：JDBC 驱动程序连接目标数据库的 URI，需要满足目标数据库 URI 的语法。常见的目标数据库 URI，请参见 [MySQL](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html)、[Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/21/jjdbc/data-sources-and-URLs.html#GUID-6D8EFA50-AB0F-4A2B-88A0-45B4A67C361E)、[PostgreSQL](https://jdbc.postgresql.org/documentation/head/connect.html)、[SQL Server](https://docs.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url?view=sql-server-ver16) 官网文档。

> 说明：目标数据库 URI 中必须指定具体数据库的名称，如上示例中的`jdbc_test`。

* `driver_url`：用于下载 JDBC 驱动程序 JAR 包的 URL。支持为 HTTP URL 和 file URL。例如`https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar`,`file:///home/disk1/postgresql-42.3.3.jar`。

* `driver_class`：JDBC 驱动程序的类名称。以下列举常见 JDBC 驱动程序的类名称：

  * MySQL：com.mysql.jdbc.Driver（MySQL 5.x 及以下版本）、com.mysql.cj.jdbc.Driver （MySQL 8.x 及以上版本）
  * SQL Server：com.microsoft.sqlserver.jdbc.SQLServerDriver
  * Oracle： oracle.jdbc.driver.OracleDriver
  * PostgreSQL：org.postgresql.Driver

成功创建资源后，在 FE 上通过 `driver_url` 下载 JDBC 驱动程序 JAR 包，生成 checksum 并保存起来，用于校验 BE 节点上下载的 JDBC 驱动程序 JAR 包的正确性。

> 说明：如果下载 JDBC 驱动程序失败，则创建资源也会失败。

BE 节点首次查询 JDBC 外部表时，如果发现所在机器上不存在相应的 JDBC 驱动程序 JAR 包，则会通过 `driver_url` 进行下载，所有的 JDBC 驱动程序 JAR 包都会保存在 **${STARROCKS_HOME}/lib/jdbc_drivers** 目录下。

#### 查看 JDBC 资源

执行如下语句，查看 StarRocks 中的所有 JDBC 资源：

> 说明：`ResourceType` 列为 `jdbc`。

~~~SQL
SHOW RESOURCES;
~~~

#### 删除 JDBC 资源

执行如下语句，删除名为 `jdbc0` 的 JDBC 资源：

~~~SQL
DROP RESOURCE "jdbc0";
~~~

> 说明：删除 JDBC 资源会导致使用该 JDBC 资源创建的 JDBC 外部表不可用，但目标数据库的数据并不会丢失。如果您仍需要通过 StarRocks 查询目标数据库的数据，可以重新创建 JDBC 资源和 JDBC 外部表。

### **创建数据库**

执行如下语句，在 StarRocks 中创建并进入名为 `jdbc_test` 的数据库：

~~~SQL
CREATE DATABASE jdbc_test; 
USE jdbc_test; 
~~~

> 说明**：**库名无需与目标数据库的名称保持一致。

### **创建 JDBC 外部表**

执行如下语句，在数据库 `jdbc_test` 中，创建一张名为 `jdbc_tbl` 的 JDBC 外部表：

~~~SQL
create external table jdbc_tbl (
    `id` bigint NULL, 
    `data` varchar(200) NULL 
) ENGINE=jdbc 
properties (
    "resource"="jdbc0",
    "table"="dest_tbl"
);
~~~

`properties` 配置项：

* `resource`：所使用 JDBC 资源的名称，必填项。

* `table`：目标数据库的表名，必填项。

支持的数据类型以及与 StarRocks 的数据类型映射关系，请参见[数据类型映射](#数据类型映射)。
> 说明：

* 不支持索引。
* 不支持通过 PARTITION BY、DISTRIBUTED BY 来指定数据分布规则。

### **查询 JDBC 外部表**

查询 JDBC 外部表前，必须启用 Pipeline 引擎。

> 说明：如果已经启用 Pipeline 引擎，则可跳过本步骤。

~~~SQL
set enable_pipeline_engine=true;
~~~

执行如下语句，通过 JDBC 外部表查询目标数据库的数据：

~~~SQL
select * from jdbc_tbl;
~~~

StarRocks 支持对目标表进行谓词下推，把过滤条件推给目标表执行，让执行尽量靠近数据源，进而提高查询性能。目前支持下推运算符，包括二元比较运算符（`>`、`>=`、`=`、`<`、`<=`）、`IN`、`IS NULL` 和 `BETWEEN ... AND ...`，但是不支持下推函数。

### 数据类型映射

目前仅支持查询目标数据库中数字、字符串、时间、日期等基础类型的数据。如果目标数据库中的数据超出 StarRocks 中数据类型的表示范围，则查询会报错。

如下以目标数据库 MySQL、Oracle、PostgreSQL、SQL Server 为例，说明支持查询的数据类型，以及与 StarRocks 数据类型的映射关系。

#### **目标数据库为 MySQL**

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

#### **目标数据库为 Oracle**

| Oracle          | StarRocks |
| --------------- | --------- |
| CHAR            | CHAR      |
| VARCHAR/VARCHAR2 | VARCHAR   |
| DATE            | DATE      |
| SMALLINT        | SMALLINT  |
| INT             | INT       |
| DATE            | DATETIME      |
| NUMBER          | DECIMAL   |

#### **目标数据库为 PostgreSQL**

| PostgreSQL          | StarRocks |
| ------------------- | --------- |
| SMALLINT/SMALLSERIAL | SMALLINT  |
| INTEGER/SERIAL       | INT       |
| BIGINT/BIGSERIAL     | BIGINT    |
| BOOLEAN             | BOOLEAN   |
| REAL                | FLOAT     |
| DOUBLE PRECISION    | DOUBLE    |
| DECIMAL             | DECIMAL   |
| TIMESTAMP           | DATETIME  |
| DATE                | DATE      |
| CHAR                | CHAR      |
| VARCHAR             | VARCHAR   |
| TEXT                | VARCHAR   |

#### **目标数据库为 SQL Server**

| SQL Server        | StarRocks |
| ----------------- | --------- |
| BIT           | BOOLEAN   |
| TINYINT           | TINYINT   |
| SMALLINT          | SMALLINT  |
| INT               | INT       |
| BIGINT            | BIGINT    |
| FLOAT             | FLOAT/DOUBLE     |
| REAL              | FLOAT    |
| DECIMAL/NUMERIC    | DECIMAL   |
| CHAR              | CHAR      |
| VARCHAR           | VARCHAR   |
| DATE              | DATE      |
| DATETIME/DATETIME2 | DATETIME  |

### 使用限制

* 创建 JDBC 外部表时，不支持索引，也不支持通过 PARTITION BY、DISTRIBUTED BY 来指定数据分布规则。
* 查询 JDBC 外部表时，不支持下推函数。

## Hive 外表

### 创建 Hive 资源

StarRocks 使用 Hive 资源来管理使用到的 Hive 集群相关配置，如 Hive Metastore 地址等，一个 Hive 资源对应一个 Hive 集群。创建 Hive 外表的时候需要指定使用哪个 Hive 资源。

~~~sql
-- 创建一个名为 hive0 的 Hive 资源。
CREATE EXTERNAL RESOURCE "hive0"
PROPERTIES (
  "type" = "hive",
  "hive.metastore.uris" = "thrift://10.10.44.98:9083"
);

-- 查看 StarRocks 中创建的资源。
SHOW RESOURCES;

-- 删除名为 hive0 的资源。
DROP RESOURCE "hive0";
~~~

### 创建数据库

~~~sql
CREATE DATABASE hive_test;
USE hive_test;
~~~

<br/>

### 创建 Hive 外表

~~~sql
-- 语法
CREATE EXTERNAL TABLE table_name (
  col_name col_type [NULL | NOT NULL] [COMMENT "comment"]
) ENGINE=HIVE
PROPERTIES (
  "key" = "value"
);

-- 例子：创建 hive0 资源对应的 Hive 集群中 rawdata 数据库下的 profile_parquet_p7 表的外表。
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
  * 列名需要与 Hive 表一一对应。
  * 列顺序与 Hive 表的关系。如果 Hive 表的存储格式为 Parquet 或 ORC，则列的顺序 **不需要** 与 Hive 表一致。如果 Hive 表的存储格式为 CSV，则列的顺序 **需要** 与 Hive 表一致。
  * 可以只选择 Hive 表中的 **部分列**，但 **分区列** 必须要全部包含。
  * 外表的分区列无需通过 partition by 语句指定，需要与普通列一样定义到描述列表中。不需要指定分区信息，StarRocks 会自动从 Hive 同步。
  * ENGINE 指定为 HIVE。
* PROPERTIES 属性：
  * **hive.resource**：指定使用的 Hive 资源。
  * **database**：指定 Hive 中的数据库。
  * **table**：指定 Hive 中的表，**不支持 view**。
* 创建外部表时，需根据 Hive 表列类型指定 StarRocks 中外部表列类型，具体映射关系如下：

| **Hive**      | **StarRocks**                                                |
| ------------- | ------------------------------------------------------------ |
| INT/INTEGER | INT                                                          |
| BIGINT        | BIGINT                                                       |
| TIMESTAMP     | DATETIME <br />注意 TIMESTAMP 转成 DATETIME会损失精度和时区信息，并根据 sessionVariable 中的时区转成无时区 DATETIME。 |
| STRING        | VARCHAR                                                      |
| VARCHAR       | VARCHAR                                                      |
| CHAR          | CHAR                                                         |
| DOUBLE        | DOUBLE                                                       |
| FLOAT         | FLOAT                                                        |
| DECIMAL       | DECIMAL                                                      |

说明：

* Hive 表 Schema 变更 **不会自动同步**，需要在 StarRocks 中重建 Hive 外表。
* 支持 Hive 的存储格式为 Parquet 和 ORC。
* 压缩格式支持 Snappy 和 LZ4。

<br/>

### 查询 Hive 外表

~~~sql
-- 查询 profile_wos_p7 的总行数。
select count(*) from profile_wos_p7;
~~~

<br/>

### 配置

* fe 配置文件路径为$FE_HOME/conf，如果需要自定义 hadoop 集群的配置可以在该目录下添加配置文件，例如：hdfs 集群采用了高可用的 nameservice，需要将 hadoop 集群中的 hdfs-site.xml 放到该目录下，如果 hdfs 配置了 viewfs，需要将 core-site.xml 放到该目录下。
* be 配置文件路径为$BE_HOME/conf，如果需要自定义 hadoop 集群的配置可以在该目录下添加配置文件，例如：hdfs 集群采用了高可用的 nameservice，需要将 hadoop 集群中的 hdfs-site.xml 放到该目录下，如果 hdfs 配置了 viewfs，需要将 core-site.xml 放到该目录下。
* be 所在的机器也需要配置 JAVA_HOME，一定要配置成 jdk 环境，不能配置成 jre 环境
* kerberos 支持
  1. 在所有的 fe/be 机器上用 `kinit -kt keytab_path principal` 登录，该用户需要有访问 hive 和 hdfs 的权限。kinit 命令登录是有实效性的，需要将其放入 crontab 中定期执行。
  2. 把 hadoop 集群中的 hive-site.xml/core-site.xml/hdfs-site.xml 放到$FE_HOME/conf 下，把 core-site.xml/hdfs-site.xml 放到$BE_HOME/conf 下。
  3. 在$FE_HOME/conf/fe.conf 文件中的 JAVA_OPTS/JAVA_OPTS_FOR_JDK_9 选项加上 -Djava.security.krb5.conf=/etc/krb5.conf，/etc/krb5.conf 是 krb5.conf 文件的路径，可以根据自己的系统调整。
  4. 在$BE_HOME/conf/be.conf 文件增加选项 JAVA_OPTS/JAVA_OPTS_FOR_JDK_9="-Djava.security.krb5.conf=/etc/krb5.conf"，其中 /etc/krb5.conf 是 krb5.conf 文件的路径，可以根据自己的系统调整。
  5. resource 中的 uri 地址一定要使用域名，并且相应的 hive 和 hdfs 的域名与 ip 的映射都需要配置到/etc/hosts 中。

#### AWS S3/Tencent Cloud COS支持

一. 下载[依赖库](https://cdn-thirdparty.starrocks.com/hive_s3_jar.tar.gz)并添加到$FE_HOME/lib/路径下。

二. 在 $FE_HOME/conf/core-site.xml 中加入如下配置。

~~~xml
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
      <value>s3.us-west-2.amazonaws.com</value>
   </property>
   <property>
     <name>fs.s3a.connection.maximum</name>
     <value>500</value>
   </property>
</configuration>
~~~

* `fs.s3a.access.key` 指定 aws 的 access key id
* `fs.s3a.secret.key` 指定 aws 的 secret access key
* `fs.s3a.endpoint` 指定 aws 的区域
* `fs.s3a.connection.maximum` 配置最大链接数，如果查询过程中有报错 `Timeout waiting for connection from poll`，可以适当调高该参数

二. 在$BE_HOME/conf/be.conf 中加入如下配置。

* `object_storage_access_key_id` 与 FE 端 core-site.xml 配置 `fs.s3a.access.key` 相同
* `object_storage_secret_access_key` 与 FE 端 core-site.xml 配置 `fs.s3a.secret.key` 相同
* `object_storage_endpoint` 与 FE 端 core-site.xml 配置 `fs.s3a.endpoint` 相同
* `object_storage_region` 只有腾讯COS需要额外添加该配置项。如：ap-beijing****

三. 重启 FE，BE。

#### Aliyun OSS 支持

一. 在$FE_HOME/conf/core-site.xml 中加入如下配置。

~~~xml
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
        <value>xxx</value>
    </property>
    <property>
        <name>fs.oss.accessKeySecret</name>
        <value>xxx</value>
    </property>
    <property>
        <name>fs.oss.endpoint</name>
        <!-- 以下以北京地域为例，其他地域请根据实际情况替换。 -->
        <value>oss-cn-beijing.aliyuncs.com</value>
    </property>
</configuration>
~~~

* `fs.oss.accessKeyId` 指定阿里云账号或 RAM 用户的 AccessKey ID，获取方式，请参见 [获取 AccessKey](https://help.aliyun.com/document_detail/53045.htm?spm=a2c4g.11186623.0.0.128b4b7896DD4W#task968)。
* `fs.oss.accessKeySecret` 指定阿里云账号或 RAM 用户的 AccessKey Secret，获取方式，请参见 [获取 AccessKey](https://help.aliyun.com/document_detail/53045.htm?spm=a2c4g.11186623.0.0.128b4b7896DD4W#task968)。
* `fs.oss.endpoint` 指定相关 OSS Bucket 所在地域对应的 Endpoint。
    您可以通过以下方式查询 Endpoint：

  * 根据 Endpoint 与地域的对应关系进行查找，请参见 [访问域名和数据中心](https://help.aliyun.com/document_detail/31837.htm#concept-zt4-cvy-5db)。
  * 您可以登录 [阿里云 OSS 管理控制台](https://oss.console.aliyun.com/index?spm=a2c4g.11186623.0.0.11d24772leoEEg#/)，进入 Bucket 概览页，Bucket 域名 examplebucket.oss-cn-hangzhou.aliyuncs.com 的后缀部分 oss-cn-hangzhou.aliyuncs.com，即为该 Bucket 的外网 Endpoint。

二. 在$BE_HOME/conf/be.conf 中加入如下配置。

* `object_storage_access_key_id` 与 FE 端 core-site.xml 配置 `fs.oss.accessKeyId` 相同
* `object_storage_secret_access_key` 与 FE 端 core-site.xml 配置 `fs.oss.accessKeySecret` 相同
* `object_storage_endpoint` 与 FE 端 core-site.xml 配置 `fs.oss.endpoint` 相同

三. 重启 FE，BE。

### 缓存更新

Hive Table 的 Partition 统计信息以及 Partition 下面的文件信息可以缓存到 StarRocks FE 中，缓存的内存结构为 Guava LoadingCache。您可以在 fe.conf 文件中通过设置`hive_meta_cache_refresh_interval_s`参数修改缓存自动刷新的间隔时间（默认值为`7200`，单位：秒），也可以通过设置`hive_meta_cache_ttl_s`参数修改缓存的失效时间（默认值为`86400`，单位：秒）。修改后需重启 FE 生效。

#### 手动更新元数据缓存

* 手动刷新元数据信息：
  1. hive 中新增或者删除分区时，需要刷新 **表** 的元数据信息：`REFRESH EXTERNAL TABLE hive_t`，其中 hive_t 是 starrocks 中的外表名称。
  2. hive 中向某些 partition 中新增数据时，需要 **指定 partition** 进行刷新：`REFRESH EXTERNAL TABLE hive_t PARTITION ('k1=01/k2=02', 'k1=03/k2=04')`，其中 hive_t 是 starrocks 中的外表名称，'k1 = 01/k2 = 02'、 'k1 = 03/k2 = 04'是 hive 中的 partition 名称。
  3. 在执行 `REFRESH EXTERNAL TABLE hive_t` 命令时，StarRocks 会先检查 Hive 外部表中的列信息和 Hive Metastore 返回的 Hive 表中的列信息是否一致。若发现 Hive 表的 schema 有修改，如增加列或减少列，那么 StarRocks 会将修改的信息同步到 Hive 外部表。同步后，Hive 外部表的列顺序和 Hive 表的列顺序保持一致，且分区列为最后一列。
  
#### 自动增量更新元数据缓存

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
  * 当 Hive 数据存储为 Parquet、ORC、CSV 格式时，StarRocks 2.3及以上版本支持 Hive 外部表同步 ADD COLUMN、REPLACE COLUMN 等表结构变更（Schema Change）。

## Apache Iceberg 外部表

如要查询 Iceberg 数据，需要在 StarRocks 中创建 Iceberg 外部表，并将外部表与需要查询的 Iceberg 表建立映射。

### 前提条件

确保 StarRocks 有权限访问 Iceberg 依赖的元数据服务（如 Hive metastore）、文件系统（如 HDFS ）和对象存储系统（如 Amazon S3 和阿里云对象存储 OSS）。

### 注意事项

* Iceberg外表是只读的，只能用于查询操作。
* 支持Iceberg的表格式为V1（Copy on write表），暂不支持为 V2（Merge on read表）。V1和V2之间的更多区别，请参见[Apache Iceberg官网](https://iceberg.apache.org/#spec/#format-versioning)。
* 支持Iceberg文件的压缩格式为GZIP（默认值），ZSTD，LZ4和SNAPPY。
* 仅支持Iceberg的Catalog类型为Hive Catalog，数据存储格式为Parquet和ORC。
* StarRocks暂不⽀持同步Iceberg中的[schema evolution](https://iceberg.apache.org/#evolution#schema-evolution)，如果Iceberg表schema evolution发生变更，您需要在StarRocks中删除对应Iceberg外表并重新建立。

### 操作步骤

#### 步骤一：创建和管理Iceberg资源

您需要提前在 StarRocks 中创建 Iceberg 资源，该资源管理 Iceberg 数据源的相关连接信息。创建资源后，即可使用该资源创建外部表。

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
| iceberg.catalog.hive.metastore.uris | Hive Metastore的thrift URI。<br />Iceberg通过创建Hive Catalog，连接Hive Metastore，以创建并管理表。您需要传入该Hive Metastore的thrift URI。格式为**thrift://<Hive Metadata的IP地址>:<端口号>**，端口号默认为9083。 |

执行如下命令，查看StarRocks中的所有Iceberg资源。

~~~sql
SHOW RESOURCES;
~~~~

执行如下命令，删除名为`iceberg0`的Iceberg资源。

~~~sql
DROP RESOURCE "iceberg0";
~~~~

> 删除Iceberg资源会导致其包含的所有Iceberg外表不可用，但Apache Iceberg中的数据并不会丢失。如果您仍需要通过StarRocks查询Iceberg的数据，请重新创建Iceberg资源，Iceberg数据库和外表。

#### 步骤二：创建数据库

执行如下命令，在StarRocks中创建并进入名为`iceberg_test`的数据库。

~~~sql
CREATE DATABASE iceberg_test; 
USE iceberg_test; 
~~~

> 库名无需与Iceberg的实际库名保持一致。

#### 步骤三：创建Iceberg外表

执行如下命令，在数据库`iceberg_test`中，创建一张名为`iceberg_tbl`的Iceberg外表。

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
* 创建外部表时，需根据 Iceberg 表的列类型指定 StarRocks 中外部表的列类型，具体映射关系如下：

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

#### 步骤四：查询Iceberg数据

创建Iceberg外表后，无需导入数据，执行如下命令，即可查询Iceberg的数据。

~~~sql
select count(*) from iceberg_tbl;
~~~

## Apache Hudi外表

StarRocks支持通过外表的方式查询Apache Hudi数据湖中的数据，帮助您实现对数据湖的极速分析。本文介绍如何在StarRock创建外表，查询Apache Hudi中的数据。

### 前提条件

请确认StarRocks有权限访问Apache Hudi对应的Hive Metastore、HDFS集群或者对象存储的Bucket。

### 注意事项

* Hudi 外表是只读的，只能用于查询操作。
* 当前支持 Hudi 的表类型为 Copy on write(下文简称 COW)，暂不支持 Merge on read(下文简称 MOR)表类型。COW 和 MOR 之间的更多区别，请参见 [Apache Hudi 官网](https://hudi.apache.org/docs/table_types)。
* 支持 Hudi 文件的压缩格式为 GZIP（默认值），ZSTD，LZ4 和 SNAPPY。
* StarRocks 暂不⽀持同步 Hudi 表结构。如果 Hudi 表结构发生变化，您需要在 StarRocks 中删除相应的外部表并重新创建。

### 操作步骤

#### 步骤一：创建和管理Hudi资源

您需要提前在StarRocks中创建Hudi资源，用于管理在StarRocks中创建的Hudi数据库和外表。

执行如下命令，创建一个名为`hudi0`的Hudi资源。

~~~sql
CREATE EXTERNAL RESOURCE "hudi0" 
PROPERTIES ( 
    "type" = "hudi", 
    "hive.metastore.uris" = "thrift://192.168.7.251:9083"
);
~~~

|  参数   | 说明  |
|  ----  | ----  |
| type  | 资源类型，固定取值为 **hudi**。 |
| hive.metastore.uris | Hive Metastore 的 thrift URI。<br /> Hudi 通过连接 Hive Metastore，以创建并管理表。您需要传入该 Hive Metastore 的 thrift URI。格式为 **thrift://<Hudi元数据的IP地址>:<端口号>**，端口号默认为 9083。 |

执行如下命令，查看StarRocks中的所有Hudi资源。

~~~sql
SHOW RESOURCES;
~~~~

执行如下命令，删除名为`hudi0`的Hudi资源。

~~~sql
DROP RESOURCE "hudi0";
~~~~

> 删除Hudi资源会导致其包含的所有Hudi外表不可用，但Apache Hudi中的数据并不会丢失。如果您仍需要通过StarRocks查询Hudi的数据，请重新创建Hudi资源，Hudi数据库和外表。

#### 步骤二：创建Hudi数据库

执行如下命令，在StarRocks中创建并进入名为`hudi_test`的Hudi数据库。

~~~sql
CREATE DATABASE hudi_test; 
USE hudi_test; 
~~~

> 库名无需与Hudi的实际库名保持一致。

#### 步骤三：创建Hudi外表

执行如下命令，在Hudi数据库`hudi_test`中，创建一张名为`hudi_tbl`的Hudi外表。

~~~sql
CREATE EXTERNAL TABLE `hudi_tbl` ( 
    `id` bigint NULL, 
    `data` varchar(200) NULL 
) ENGINE=HUDI 
PROPERTIES ( 
    "resource" = "hudi0", 
    "database" = "hudi", 
    "table" = "hudi_table" 
); 
~~~

* 相关参数说明，请参见下表：

| **参数**     | **说明**                       |
| ------------ | ------------------------------ |
| **ENGINE**   | 固定为**HUDI**，无需更改。  |
| **resource** | StarRocks的Hudi资源的名称。 |
| **database** | Hudi表所在的数据库名称。        |
| **table**    | Hudi表所在的数据表名称。        |

* 表名无需与Hudi实际表名保持一致。
* 列名需要与Hudi实际列名保持一致，列的顺序无需保持一致。
* 您可以按照业务需求选择Hudi表中的全部或部分列。
* 创建外部表时，需根据 Hudi 表列类型指定 StarRocks 中外部表列类型，具体映射关系如下：

| **Hudi**                     | **StarRocks**           |
| ---------------------------- | ----------------------- |
| BOOLEAN                      | BOOLEAN                 |
| INT                          | TINYINT/SMALLINT/INT    |
| DATE                         | DATE                    |
| TimeMillis/TimeMicros        | TIME                    |
| LONG                         | BIGINT                  |
| FLOAT                        | FLOAT                   |
| DOUBLE                       | DOUBLE                  |
| STRING                       | CHAR/VARCHAR            |
| ARRAY                        | ARRAY                   |
| DECIMAL                      | DECIMAL                 |

> 如果Apache Hudi部分列的数据类型为FIXED, ENUM, UNION, MAP, BYTES，则StarRocks暂不支持通过Hudi关联外表的方式访问此数据类型。

#### 步骤四：查询Hudi外表

创建Hudi外表后，无需导入数据，执行如下命令，即可查询Hudi的数据。

~~~sql
SELECT COUNT(*) FROM hudi_tbl;
~~~
