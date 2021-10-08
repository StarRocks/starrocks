# 外部表

StarRocks 支持以外部表的形式，接入其他数据源。外部表指的是保存在其他数据源中的数据表，而 StartRocks 只保存表对应的元数据，并直接向外部表所在数据源发起查询。目前 StarRocks 已支持的第三方数据源包括 MySQL、HDFS、ElasticSearch、Hive。对这几种种数据源，**现阶段只支持读取，还不支持写入**。

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

## HDFS外部表

与访问MySQL类似，StarRocks访问HDFS文件之前，也需提前建立好与之相对应的外部表，如下图。

~~~sql
CREATE EXTERNAL TABLE hdfs_external_table (
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=broker
PROPERTIES (
    "broker_name" = "broker_name",
    "path" = "hdfs://hdfs_host:hdfs_port/data1",
    "column_separator" = "|",
    "line_delimiter" = "\n"
)
BROKER PROPERTIES (
    "username" = "hdfs_username",
    "password" = "hdfs_password"
)
~~~

参数说明：

* **broker_name**：Broker名字
* **path**：HDFS文件路径
* **column_separator**：列分隔符
* **line_delimiter**：行分隔符

StarRocks不能直接访问HDFS文件，需要通过Broker进行访问。所以，建表时除了需要指定HDFS文件的相关信息之外，还需要指定Broker的相关信息。关于Broker的相关介绍，可以参见[Broker导入](../loading/BrokerLoad.md)。

<br/>

## ElasticSearch外部表

StarRocks与ElasticSearch都是目前流行的分析系统，StarRocks强于大规模分布式计算，ElasticSearch擅长全文检索。StarRocks支持ElasticSearch访问的目的，就在于将这两种能力结合，提供更完善的一个OLAP解决方案。

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
PROPERTIES (
    "hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
    "user" = "root",
    "password" = "root",
    "index" = "tindex",
    "type" = "doc"
);
~~~

参数说明：

* **host**：ES集群连接地址，可指定一个或多个，StarRocks通过这个地址获取到ES版本号、index的shard分布信息
* **user**：开启**basic认证**的ES集群的用户名，需要确保该用户有访问 /*cluster/state/*nodes/http 等路径权限和对index的读权限
* **password**：对应用户的密码信息
* **index**：StarRocks中的表对应的ES的index名字，可以是alias
* **type**：指定index的type，默认是**doc**
* **transport**：内部保留，默认为**http**

<br/>

### 谓词下推

StarRocks支持对ElasticSearch表进行谓词下推，把过滤条件推给ElasticSearch进行执行，让执行尽量靠近存储，提高查询性能。目前支持哪些下推的算子如下表：

|   SQL syntax  |   ES syntax  |
| :---: | :---: |
|  =   |  term query   |
|  in   |  terms query   |
|  \>=,  <=, >, <   |  range   |
|  and   |  bool.filter   |
|  or   |  bool.should   |
|  not   |  bool.must_not   |
|  not in   |  bool.must_not + terms   |
|  esquery   |  ES Query DSL   |

表1 ：支持的谓词下推列表

<br/>

### 查询示例

通过**esquery函数**将一些**无法用sql表述的ES query**如match、geoshape等下推给ES进行过滤处理。esquery的第一个列名参数用于关联index，第二个参数是ES的基本Query DSL的json表述，使用花括号{}包含，**json的root key有且只能有一个**，如match、geo_shape、bool等。

* match查询：

~~~sql
select * from es_table where esquery(k4, '{
    "match": {
       "k4": "StarRocks on elasticsearch"
    }
}');
~~~

* geo相关查询：

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

* bool查询：

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

<br/>

### 注意事项

* ES在5.x之前和之后的数据扫描方式不同，目前**只支持5.x之后的版本**；
* 支持使用HTTP Basic认证方式的ES集群；
* 一些通过StarRocks的查询会比直接请求ES会慢很多，比如count相关的query等。这是因为ES内部会直接读取满足条件的文档个数相关的元数据，不需要对真实的数据进行过滤操作，使得count的速度非常快。

<br/>

## Hive外表

### 创建Hive资源

一个Hive资源对应一个Hive集群，管理StarRocks使用的Hive集群相关配置，如Hive meta store地址等。创建Hive外表的时候需要指定使用哪个Hive资源。

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
  * 列的顺序**不需要**与Hive表一致。
  * 可以只选择Hive表中的**部分列**，但**分区列**必须要全部包含。
  * 外表的分区列无需通过partition by语句指定，需要与普通列一样定义到描述列表中。不需要指定分区信息，StarRocks会自动从Hive同步。
  * ENGINE指定为HIVE。
* PROPERTIES属性：
  * **hive.resource**：指定使用的Hive资源。
  * **database**：指定Hive中的数据库。
  * **table**：指定Hive中的表，**不支持view**。
* 支持的列类型对应关系如下表：

    |  Hive列类型   | StarRocks列类型    | 描述 |
    | --- | --- | ---|
    |   INT/INTEGER  | INT    |
    |   BIGINT  | BIGINT    |
    |   TIMESTAMP  | DATETIME    |Timestamp转成Datetime，会损失精度和时区信息，<br/>根据sessionVariable中的时区转成无时区Datatime|
    |  STRING  | VARCHAR   |
    |  VARCHAR  | VARCHAR   |
    |  CHAR  | CHAR   |
    |  DOUBLE | DOUBLE |
    | FLOATE | FLOAT|

    说明：

  * Hive表Schema变更**不会自动同步**，需要在StarRocks中重建Hive外表。
  * 当前Hive的存储格式仅支持Parquet和ORC类型
  * 压缩格式支持snappy，lz4

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
