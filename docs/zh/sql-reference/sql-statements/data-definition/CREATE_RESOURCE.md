# CREATE RESOURCE

## 功能

该语句用于创建资源。仅 root 或 admin 用户可以创建资源。目前仅支持 Spark 和 Hive 资源，可用于 [SPARK LOAD](/sql-reference/sql-statements/data-manipulation/SPARK_LOAD.md) 及 [Hive 外表](/data_source/External_table.md#hive外表) 功能。将来其他外部资源可能会加入到 StarRocks 中使用，如 Spark/GPU 用于查询，HDFS/S3 用于外部存储，MapReduce 用于 ETL 等。

删除 RESOURCE 操作请参考 [DROP RESOURCE](../data-definition/DROP_RESOURCE.md) 章节。

## 语法

```sql
CREATE [EXTERNAL] RESOURCE "resource_name"
PROPERTIES ("key"="value", ...);
```

注：方括号 [] 中内容可省略不写。

说明：

1. `PROPERTIES` 中需要指定资源的类型，目前仅支持 spark 和 hive。
2. 根据资源类型的不同 `PROPERTIES` 有所不同，具体见示例。

## 示例

1. 创建一个 yarn cluster 模式，名为 spark0 的 Spark 资源。

    ```sql
    CREATE EXTERNAL RESOURCE "spark0"
    PROPERTIES
    (
        "type" = "spark",
        "spark.master" = "yarn",
        "spark.submit.deployMode" = "cluster",
        "spark.jars" = "xxx.jar,yyy.jar",
        "spark.files" = "/tmp/aaa,/tmp/bbb",
        "spark.executor.memory" = "1g",
        "spark.yarn.queue" = "queue0",
        "spark.hadoop.yarn.resourcemanager.address" = "127.0.0.1:9999",
        "spark.hadoop.fs.defaultFS" = "hdfs://127.0.0.1:10000",
        "working_dir" = "hdfs://127.0.0.1:10000/tmp/starrocks",
        "broker" = "broker0",
        "broker.username" = "user0",
        "broker.password" = "password0"
    );
    ```

    Spark 相关参数如下：

    ```plain text
    1. spark.master: 必填，目前支持yarn，spark://host:port。
    2. spark.submit.deployMode: Spark 程序的部署模式，必填，支持 cluster，client 两种。
    3. spark.hadoop.yarn.resourcemanager.address: master为yarn时必填。
    4. spark.hadoop.fs.defaultFS: master为yarn时必填。
    5. 其他参数为可选，参考http://spark.apache.org/docs/latest/configuration.html
    ```

    Spark 用于 ETL 时需要指定 working_dir 和 broker。说明如下：

    ```plain text
    working_dir: ETL 使用的目录。spark作为ETL资源使用时必填。例如：hdfs://host:port/tmp/starrocks。
    broker: broker 名字。spark作为ETL资源使用时必填。需要使用`ALTER SYSTEM ADD BROKER` 命令提前完成配置。
    broker.property_key: broker读取ETL生成的中间文件时需要指定的认证信息等。
    ```

2. 创建一个名为 hive0 的 Hive 资源。

    ```sql
    CREATE EXTERNAL RESOURCE "hive0"
    PROPERTIES
    (
        "type" = "hive",
        "hive.metastore.uris" = "thrift://10.10.44.98:9083"
    );
    ```

## 关键字(keywords)

CREATE RESOURCE
