# 使用 Spark connector 导入数据（推荐）

StarRocks 提供 Apache Spark™ 连接器 (StarRocks Connector for Apache Spark™)，可以通过 Spark 导入数据至 StarRocks（推荐）。
基本原理是对数据攒批后，通过 [Stream Load](./StreamLoad.md) 批量导入StarRocks。Connector 导入数据基于Spark DataSource V2 实现，
可以通过 Spark DataFrame 或 Spark SQL 创建 DataSource，支持 Batch 和 Structured Streaming。

## 版本要求

| Connector | Spark           | StarRocks | Java  | Scala |
|----------|-----------------|-----------|-------| ---- |
| 1.1.0    | 3.2, 3.3, 3.4   | 2.5 及以上   | 8     | 2.12 |

## 获取 Connector

您可以通过以下方式获取 connector jar 包

* 直接下载已经编译好的jar
* 通过 Maven 添加 connector 依赖
* 通过源码手动编译

connector jar包的命名格式如下

`starrocks-spark-connector-${spark_version}_${scala_version}-${connector_version}.jar`

比如，想在 Spark 3.2 和 scala 2.12 上使用 1.1.0 版本的 connector，可以选择 `starrocks-spark-connector-3.2_2.12-1.1.0.jar`。

> **注意**
>
> 一般情况下最新版本的 connector 只维护最近3个版本的 Spark。

### 直接下载

可以在 [Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks) 获取不同版本的 connector jar。

### Maven 依赖

依赖配置的格式如下，需要将 `spark_version`、`scala_version` 和 `connector_version` 替换成对应的版本。

```xml
<dependency>
  <groupId>com.starrocks</groupId>
  <artifactId>starrocks-spark-connector-${spark_version}_${scala_version}</artifactId>
  <version>${connector_version}</version>
</dependency>
```

比如，想在 Spark 3.2 和 scala 2.12 上使用 1.1.0 版本的 connector，可以添加如下依赖

```xml
<dependency>
  <groupId>com.starrocks</groupId>
  <artifactId>starrocks-spark-connector-3.2_2.12</artifactId>
  <version>1.1.0</version>
</dependency>
```

### 手动编译

1. 下载 [Spark 连接器代码](https://github.com/StarRocks/starrocks-connector-for-apache-spark)。

2. 通过如下命令进行编译，需要将 `spark_version` 替换成相应的 Spark 版本

      ```shell
      sh build.sh <spark_version>
      ```

   比如，在 Spark 3.2 上使用，命令如下

      ```shell
      sh build.sh 3.2
      ```

3. 编译完成后，`target/` 目录下会生成 connector jar 包，比如 `starrocks-spark-connector-3.2_2.12-1.1-SNAPSHOT.jar`。

> **注意**
>
> 非正式发布的connector版本会带有`SNAPSHOT`后缀。

## 参数说明

| 参数                                             | 是否必填   | 默认值 | 描述                                                                                                                                                                                                                    |
|------------------------------------------------|-------- | ---- |-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| starrocks.fe.http.url                          | 是      | 无 | FE 的 HTTP 地址，支持输入多个FE地址，使用逗号 , 分隔。格式为 <fe_host1>:<fe_http_port1>,<fe_host2>:<fe_http_port2>。                                                                                                                          |
| starrocks.fe.jdbc.url                          | 是      | 无 | FE 的 MySQL Server 连接地址。格式为 jdbc:mysql://<fe_host>:<fe_query_port>。                                                                                                                                                    |
| starrocks.table.identifier                     | 是      | 无 | StarRocks 目标表的名称，格式为 <database_name>.<table_name>。                                                                                                                                                                    |
| starrocks.user                                 | 是      | 无 | StarRocks 集群账号的用户名。                                                                                                                                                                                                   |
| starrocks.password                             | 是      | 无 | StarRocks 集群账号的用户密码。                                                                                                                                                                                                  |
| starrocks.write.label.prefix                   | 否      | spark- | 指定Stream Load使用的label的前缀。                                                                                                                                                                                             |
| starrocks.write.enable.transaction-stream-load | 否      | true | 是否使用Stream Load的事务接口导入数据，详见[Stream Load事务接口](https://docs.starrocks.io/zh-cn/latest/loading/Stream_Load_transaction_interface)，该功能需要StarRocks 2.4及以上版本。                                                               |
| starrocks.write.buffer.size                    | 否      | 104857600 | 数据攒批的内存大小，达到该阈值后数据批量发送给 StarRocks，支持带单位`k`, `m`, `g`。增大该值能提高导入性能，但会带来写入延迟。                                                                                                                                                              |
| starrocks.write.flush.interval.ms              | 否      | 300000 | 数据攒批发送的间隔，用于控制数据写入StarRocks的延迟。                                                                                                                                                                                       |
| starrocks.columns                              | 否      | 无 | 支持向 StarRocks 表中写入部分列，通过该参数指定列名，多个列名之间使用逗号 (,) 分隔，例如"c0,c1,c2"。                                                                                                                                                       |
| starrocks.write.properties.*                   | 否      | 无 | 指定Stream Load 的参数，控制导入行为，例如使用starrocks.write.operties.format选择Stream load使用csv或json格式。支持的参数和说明，请参见[Stream Load](https://docs.starrocks.io/zh-cn/latest/sql-reference/sql-statements/data-manipulation/STREAM%20LOAD)。 |
| starrocks.write.properties.format              | 否      | CSV | 指定stream load导入数据使用的格式，取值为CSV 和 JSON。connector会将每批数据转换成相应的格式发送给StarRocks。                                                                                                                                             |
| starrocks.write.properties.row_delimiter       | 否      | \n | 使用CSV格式导入时，用于指定行分隔符。                                                                                                                                                                                                  |
| starrocks.write.properties.column_separator    | 否      | \t | 使用CSV格式导入时，用于指定列分隔符。                                                                                                                                                                                                  |
| starrocks.write.num.partitions                 | 否      | 无 | Spark用于并行写入的分区数，数据量小时可以通过减少分区数降低导入并发和频率，默认分区数由Spark决定。使用该功能可能会引入 Spark Shuffle cost。                                                                                                                                  |
| starrocks.write.partition.columns              | 否      | 无 | 用于Spark分区的列，只有指定 starrocks.write.num.partitions 后才有效，如果不指定则使用所有写入的列进行分区                                                                                                                                               |

## 数据类型映射

| StarRocks 数据类型 | Spark 数据类型    |
|----------------|---------------|
| BOOLEAN        | BooleanType   |
| TINYINT        | ByteType      |
| SMALLINT       | ShortType     |
| INT            | IntegerType   |
| BIGINT         | LongType      |
| LARGEINT       | StringType    |
| FLOAT          | FloatType     |
| DOUBLE         | DoubleType    |
| DECIMAL        | DecimalType   |
| CHAR           | StringType    |
| VARCHAR        | StringType    |
| STRING         | StringType    |
| DATE           | DateType      |
| DATETIME       | TimestampType |

## 使用示例

通过一个例子说明如何使用 connector 写入 StarRocks 表，包括使用 Spark DataFrame 和 Spark SQL，其中 DataFrame 包括 Batch 和 Structured Streaming 两种模式。

更多示例请参考 [Spark Connector Examples](https://github.com/StarRocks/starrocks-connector-for-apache-spark/tree/main/src/test/java/com/starrocks/connector/spark/examples)，后续会补充更多例子。

### 准备工作

#### 创建StarRocks表

创建数据库 `test`，并在其中创建名为 `score_board` 的主键表。

```SQL
CREATE DATABASE `test`;

CREATE TABLE `test`.`score_board`
(
    `id` int(11) NOT NULL COMMENT "",
    `name` varchar(65533) NULL DEFAULT "" COMMENT "",
    `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
)
ENGINE=OLAP
PRIMARY KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "1"
);
```

#### Spark 环境

示例基于 Spark 3.2.4，使用 `spark-shell` 和 `spark-sql` 进行演示，运行前请将 connector jar放置在 `$SPARK_HOME/jars` 目录下。

### 使用 Spark DataFrame 写入数据

下面分别介绍在 Batch 和 Structured Streaming 下如何写入数据。

#### Batch

该例子演示了在内存中构造数据并写入 StarRocks 表。

1. 在 `spark-shell` 中运行示例

   ```scala
   // 1. create a DataFrame from a sequence
   val data = Seq((1, "starrocks", 100), (2, "spark", 100))
   val df = data.toDF("id", "name", "score")

   // 2. write to starrocks with the format "starrocks",
   // and replace the options with your own
   df.write.format("starrocks")
      .option("starrocks.fe.http.url", "127.0.0.1:8038")
      .option("starrocks.fe.jdbc.url", "jdbc:mysql://127.0.0.1:9038")
      .option("starrocks.table.identifier", "test.score_board")
      .option("starrocks.user", "root")
      .option("starrocks.password", "")
      .mode("append")
      .save()
   ```

2. 在 StarRocks中查询结果

```SQL
MySQL [test]> SELECT * FROM `score_board`;
+------+-----------+-------+
| id   | name      | score |
+------+-----------+-------+
|    1 | starrocks |   100 |
|    2 | spark     |   100 |
+------+-----------+-------+
2 rows in set (0.00 sec)
```

#### Structured Streaming

该例子演示了在从csv文件流式读取数据并写入 StarRocks 表。

1. 在目录 `csv-data` 下创建 csv 文件 `test.csv`，数据如下

   ```csv
   3,starrocks,100
   4,spark,100
   ```

2. 在 `spark-shell` 中运行示例

   ```scala
   import org.apache.spark.sql.types.StructType

   // 1. create a DataFrame from csv
   val schema = (new StructType()
         .add("id", "integer")
         .add("name", "string")
         .add("score", "integer")
      )
   val df = (spark.readStream
         .option("sep", ",")
         .schema(schema)
         .format("csv") 
         // replace it with your path to the directory "csv-data"
         .load("/path/to/csv-data")
      )

   // 2. write to starrocks with the format "starrocks", and replace the options with your own
   val query = (df.writeStream.format("starrocks")
         .option("starrocks.fe.http.url", "127.0.0.1:8038")
         .option("starrocks.fe.jdbc.url", "jdbc:mysql://127.0.0.1:9038")
         .option("starrocks.table.identifier", "test.score_board")
         .option("starrocks.user", "root")
         .option("starrocks.password", "")
         // replace it with your checkpoint directory
         .option("checkpointLocation", "/path/to/checkpoint")
         .outputMode("append")
         .start()
      )
   ```

3. 在 StarRocks中查询结果

```SQL
MySQL [test]> select * from score_board;
+------+-----------+-------+
| id   | name      | score |
+------+-----------+-------+
|    4 | spark     |   100 |
|    3 | starrocks |   100 |
+------+-----------+-------+
2 rows in set (0.67 sec)
```

### 使用 Spark SQL 写入数据

该例子演示使用 `INSERT INTO` 写入数据，可以通过 [Spark SQL CLI](https://spark.apache.org/docs/latest/sql-distributed-sql-engine-spark-sql-cli.html) 运行该示例。

1. 在 `spark-sql` 中运行示例

   ```SQL
   -- 1. create a table using datasource "starrocks", and replace the options with your own
   CREATE TABLE `score_board`
   USING starrocks
   OPTIONS(
      "starrocks.fe.http.url"="127.0.0.1:8038",
      "starrocks.fe.jdbc.url"="jdbc:mysql://127.0.0.1:9038",
      "starrocks.table.identifier"="test.score_board",
      "starrocks.user"="root",
      "starrocks.password"=""
   );

   -- 2. insert two rows into the table
   INSERT INTO `score_board` VALUES (5, "starrocks", 100), (6, "spark", 100);
   ```

2. 在 StarRocks 中查询结果

```SQL
MySQL [test]> select * from score_board;
+------+-----------+-------+
| id   | name      | score |
+------+-----------+-------+
|    6 | spark     |   100 |
|    5 | starrocks |   100 |
+------+-----------+-------+
2 rows in set (0.00 sec)
```
