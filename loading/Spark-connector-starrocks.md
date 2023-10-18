# 使用 Spark connector 导入数据（推荐）

StarRocks 提供 Apache Spark™ 连接器 (StarRocks Connector for Apache Spark™)，可以通过 Spark 导入数据至 StarRocks（推荐）。
基本原理是对数据攒批后，通过 [Stream Load](./StreamLoad.md) 批量导入StarRocks。Connector 导入数据基于Spark DataSource V2 实现，
可以通过 Spark DataFrame 或 Spark SQL 创建 DataSource，支持 Batch 和 Structured Streaming。

> **注意**
>
> 使用 Spark connector 导入数据至 StarRocks 需要目标表的 SELECT 和 INSERT 权限。如果您的用户账号没有这些权限，请参考 [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) 给用户赋权。

## 版本要求

| Connector | Spark           | StarRocks | Java  | Scala |
|----------|-----------------|-----------|-------| ---- |
| 1.1.1 | 3.2, 3.3, 3.4 | 2.5 及以上 | 8 | 2.12 |
| 1.1.0    | 3.2, 3.3, 3.4   | 2.5 及以上   | 8     | 2.12 |

> **注意**
>
> - 了解不同版本的 Spark connector 之间的行为变化，请查看[升级 Spark connector](#升级-spark-connector)。
> - 自 1.1.1 版本起，Spark connector 不再提供 MySQL JDBC 驱动程序，您需要将驱动程序手动放到 Spark 的类路径中。您可以在 [MySQL 官网](https://dev.mysql.com/downloads/connector/j/)或 [Maven 中央仓库](https://repo1.maven.org/maven2/mysql/mysql-connector-java/)上找到该驱动程序。

## 获取 Connector

您可以通过以下方式获取 connector jar 包

- 直接下载已经编译好的jar
- 通过 Maven 添加 connector 依赖
- 通过源码手动编译

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
| starrocks.fe.http.url                          | 是      | 无 | FE 的 HTTP 地址，支持输入多个FE地址，使用逗号 , 分隔。格式为 `<fe_host1>:<fe_http_port1>,<fe_host2>:<fe_http_port2>`。自版本 1.1.1 开始，您还可以在 URL 中添加 `http://` 前缀，例如 `http://<fe_host1>:<fe_http_port1>,http://<fe_host2>:<fe_http_port2>`。|
| starrocks.fe.jdbc.url                          | 是      | 无 | FE 的 MySQL Server 连接地址。格式为 `jdbc:mysql://<fe_host>:<fe_query_port>`。                                                                                                                                                    |
| starrocks.table.identifier                     | 是      | 无 | StarRocks 目标表的名称，格式为 `<database_name>.<table_name>`。                                                                                                                                                                    |
| starrocks.user                                 | 是      | 无 | StarRocks 集群账号的用户名。使用 Spark connector 导入数据至 StarRocks 需要目标表的 SELECT 和 INSERT  权限。如果您的用户账号没有这些权限，请参考 [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) 给用户赋权。                                                                                                                                                                                                   |
| starrocks.password                             | 是      | 无 | StarRocks 集群账号的用户密码。                                                                                                                                                                                                  |
| starrocks.write.label.prefix                   | 否      | spark- | 指定Stream Load使用的label的前缀。                                                                                                                                                                                             |
| starrocks.write.enable.transaction-stream-load | 否      | true | 是否使用 [Stream Load 事务接口](../loading/Stream_Load_transaction_interface.md)导入数据。要求 StarRocks 版本为 v2.5 或更高。此功能可以在一次导入事务中导入更多数据，同时减少内存使用量，提高性能。<br/> **注意：**<br/> 自 1.1.1 版本以来，只有当  `starrocks.write.max.retries` 的值为非正数时，此参数才会生效，因为 Stream Load 事务接口不支持重试。                                                               |
| starrocks.write.buffer.size                    | 否      | 104857600 | 积攒在内存中的数据量，达到该阈值后数据一次性发送给 StarRocks，支持带单位`k`, `m`, `g`。增大该值能提高导入性能，但会带来写入延迟。                                                                                                                                                              |
| starrocks.write.buffer.rows                    | 否      | Integer.MAX_VALUE | 自 1.1.1 版本起支持。积攒在内存中的数据行数，达到该阈值后数据一次性发送给 StarRocks。                                                                                                                                                              |
| starrocks.write.flush.interval.ms              | 否      | 300000 | 数据攒批发送的间隔，用于控制数据写入StarRocks的延迟。                                                                                                                                                                                       |
| starrocks.write.max.retries                    | 否       | 3             | 自 1.1.1 版本起支持。如果一批数据导入失败，Spark connector 导入该批数据的重试次数上线。<br/> **注意：**由于 Stream Load 事务接口不支持重试。如果此参数为正数，则 Spark connector 始终使用 Stream Load 接口，并忽略 `starrocks.write.enable.transaction-stream-load` 的值。|
| starrocks.write.retry.interval.ms              | 否       | 10000         | 自 1.1.1 版本起支持。如果一批数据导入失败，Spark connector 尝试再次导入该批数据的时间间隔。|
| starrocks.columns                              | 否      | 无 | 支持向 StarRocks 表中写入部分列，通过该参数指定列名，多个列名之间使用逗号 (,) 分隔，例如"c0,c1,c2"。                                                                                                                                                       |
| starrocks.write.properties.*                   | 否      | 无 | 指定 Stream Load 的参数，用于控制导入行为，例如使用 `starrocks.write.properties.format` 指定导入数据的格式为 CSV 或者 JSON。更多参数和说明，请参见 [Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)。 |
| starrocks.write.properties.format              | 否      | CSV | 指定导入数据的格式，取值为 CSV 和 JSON。connector 会将每批数据转换成相应的格式发送给 StarRocks。                                                                                                                                             |
| starrocks.write.properties.row_delimiter       | 否      | \n | 使用CSV格式导入时，用于指定行分隔符。                                                                                                                                                                                                  |
| starrocks.write.properties.column_separator    | 否      | \t | 使用CSV格式导入时，用于指定列分隔符。                                                                                                                                                                                                  |
| starrocks.write.num.partitions                 | 否      | 无 | Spark用于并行写入的分区数，数据量小时可以通过减少分区数降低导入并发和频率，默认分区数由Spark决定。使用该功能可能会引入 Spark Shuffle cost。                                                                                                                                  |
| starrocks.write.partition.columns              | 否      | 无 | 用于Spark分区的列，只有指定 starrocks.write.num.partitions 后才有效，如果不指定则使用所有写入的列进行分区                                                                                                                                               |
| starrocks.timezone | 否 | JVM 默认时区|自 1.1.1 版本起支持。StarRocks 的时区。用于将 Spark 的 `TimestampType` 类型的值转换为 StarRocks 的 `DATETIME` 类型的值。默认为 `ZoneId#systemDefault()` 返回的 JVM 时区。格式可以是时区名称，例如 Asia/Shanghai，或时区偏移，例如 +08:00。|

## 数据类型映射

- 数据类型映射默认如下：

  |  Spark 数据类型  | StarRocks 数据类型                                             |
  | --------------- | ------------------------------------------------------------ |
  | BooleanType     | BOOLEAN                                                      |
  | ByteType        | TINYINT                                                      |
  | ShortType       | SMALLINT                                                     |
  | IntegerType     | INT                                                          |
  | LongType        | BIGINT                                                       |
  | StringType      | LARGEINT                                                     |
  | FloatType       | FLOAT                                                        |
  | DoubleType      | DOUBLE                                                       |
  | DecimalType     | DECIMAL                                                      |
  | StringType      | CHAR                                                         |
  | StringType      | VARCHAR                                                      |
  | StringType      | STRING                                                       |
  | DateType        | DATE                                                         |
  | TimestampType   | DATETIME                                                     |
  | ArrayType       | ARRAY <br /> **说明:** <br /> **自版本 1.1.1 开始支持。** 详细步骤, 请参见 [导入至 ARRAY 类型的列](#导入至-array-列). |

- 您还可以自定义数据类型映射。

例如，一个 StarRocks 表包含了 BITMAP 和 HLL 类型的列，但 Spark 不支持这两种数据类型。则您需要在 Spark 中设置其支持的数据类型，并且自定义数据类型映射关系。详细步骤，参见导入至 [BITMAP](#导入至-bitmap-列) 和 [HLL](#导入至-hll-列) 类型的列。自版本 1.1.1 起支持导入至 BITMAP 和 HLL 类型的列。

## 升级 Spark connector

### 1.1.0 升级至 1.1.1

- 自 1.1.1 版本开始，Spark connector 不再提供 MySQL 官方 JDBC 驱动程序 `mysql-connector-java`，因为该驱动程序使用 GPL 许可证，存在一些限制。然而，Spark连接器仍然需要 MySQL JDBC 驱动程序才能连接到 StarRocks 以获取表的元数据，因此您需要手动将驱动程序添加到 Spark 类路径中。您可以在 [MySQL 官网](https://dev.mysql.com/downloads/connector/j/) 或 [Maven 中央仓库](https://repo1.maven.org/maven2/mysql/mysql-connector-java/)上找到这个驱动程序。
- 自 1.1.1 版本开始，Spark connector 默认使用 Stream Load 接口，而不是 1.1.0 版本中的 Stream Load 事务接口。如果您仍然希望使用 Stream Load 事务接口，您可以将选项 `starrocks.write.max.retries` 设置为 `0`。详细信息，参见 `starrocks.write.enable.transaction-stream-load` 和 `starrocks.write.max.retries` 的说明。

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
;
```

#### Spark 环境

示例基于 Spark 3.2.4，使用 `spark-shell`，`pyspark` 和 `spark-sql` 进行演示，运行前请将 connector jar放置在 `$SPARK_HOME/jars` 目录下。

### 使用 Spark DataFrame 写入数据

下面分别介绍在 Batch 和 Structured Streaming 下如何写入数据。

#### Batch

该例子演示了在内存中构造数据并写入 StarRocks 表。

1. 您可以使用 Scala 或者 Python 语言编写 Spark 应用程序。

   如果是使用 Scala 语言，则在 `spark-shell` 中可以运行如下代码：

      ```scala
      // 1. Create a DataFrame from CSV.
      val data = Seq((1, "starrocks", 100), (2, "spark", 100))
      val df = data.toDF("id", "name", "score")

      // 2. Write to StarRocks by configuring the format as "starrocks" and the following options. 
      // You need to modify the options according your own environment.
      df.write.format("starrocks")
         .option("starrocks.fe.http.url", "127.0.0.1:8030")
         .option("starrocks.fe.jdbc.url", "jdbc:mysql://127.0.0.1:9030")
         .option("starrocks.table.identifier", "test.score_board")
         .option("starrocks.user", "root")
         .option("starrocks.password", "")
         .mode("append")
         .save()
      ```

   如果是使用 python 语言，则在 `pyspark` 中可以运行如下代码：

   ```python
   from pyspark.sql import SparkSession

   spark = SparkSession \
        .builder \
        .appName("StarRocks Example") \
        .getOrCreate()

    # 1. Create a DataFrame from a sequence.
    data = [(1, "starrocks", 100), (2, "spark", 100)]
    df = spark.sparkContext.parallelize(data) \
            .toDF(["id", "name", "score"])

    # 2. Write to StarRocks by configuring the format as "starrocks" and the following options. 
    # You need to modify the options according your own environment.
    df.write.format("starrocks") \
        .option("starrocks.fe.http.url", "127.0.0.1:8030") \
        .option("starrocks.fe.jdbc.url", "jdbc:mysql://127.0.0.1:9030") \
        .option("starrocks.table.identifier", "test.score_board") \
        .option("starrocks.user", "root") \
        .option("starrocks.password", "") \
        .mode("append") \
        .save()
    ```

2. 在 StarRocks 中查询结果。

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

2. 您可以使用 Scala 或者 Python 语言编写 Spark 应用程序。

   如果是使用 Scala 语言，则在 `spark-shell` 中可以运行如下代码：

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
         .option("starrocks.fe.http.url", "127.0.0.1:8030")
         .option("starrocks.fe.jdbc.url", "jdbc:mysql://127.0.0.1:9030")
         .option("starrocks.table.identifier", "test.score_board")
         .option("starrocks.user", "root")
         .option("starrocks.password", "")
         // replace it with your checkpoint directory
         .option("checkpointLocation", "/path/to/checkpoint")
         .outputMode("append")
         .start()
      )
   ```

   如果是使用 Python 语言，则在 `pyspark` 中可以运行如下代码：

   ```python
   from pyspark.sql import SparkSession
   from pyspark.sql.types import IntegerType, StringType, StructType, StructField

   spark = SparkSession \
        .builder \
        .appName("StarRocks SS Example") \
        .getOrCreate()

    # 1. Create a DataFrame from CSV.
    schema = StructType([ \
            StructField("id", IntegerType()), \
            StructField("name", StringType()), \
            StructField("score", IntegerType()) \
        ])
    df = spark.readStream \
            .option("sep", ",") \
            .schema(schema) \
            .format("csv") \
            # Replace it with your path to the directory "csv-data".
            .load("/path/to/csv-data")

    # 2. Write to StarRocks by configuring the format as "starrocks" and the following options. 
    # You need to modify the options according your own environment.
    query = df.writeStream.format("starrocks") \
            .option("starrocks.fe.http.url", "127.0.0.1:8030") \
            .option("starrocks.fe.jdbc.url", "jdbc:mysql://127.0.0.1:9030") \
            .option("starrocks.table.identifier", "test.score_board") \
            .option("starrocks.user", "root") \
            .option("starrocks.password", "") \
            # replace it with your checkpoint directory
            .option("checkpointLocation", "/path/to/checkpoint") \
            .outputMode("append") \
            .start()
        )
    ```

3. 在 StarRocks 中查询结果。

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
   -- 1. Create a table by configuring the data source as  `starrocks` and the following options. 
   -- You need to modify the options according your own environment
   CREATE TABLE `score_board`
   USING starrocks
   OPTIONS(
      "starrocks.fe.http.url"="127.0.0.1:8030",
      "starrocks.fe.jdbc.url"="jdbc:mysql://127.0.0.1:9030",
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

## 最佳实践

### 导入至主键模型表

本节将展示如何将数据导入到 StarRocks 主键模型表中，以实现部分更新和条件更新。部分更新和条件更新的更多介绍，请参见[通过导入实现数据变更](./Load_to_Primary_Key_tables.md)。

以下示例使用 Spark SQL。

#### 准备工作

在 StarRocks 中创建一个名为 `test` 的数据库，并在其中创建一个名为 `score_board` 的主键模型表。

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
DISTRIBUTED BY HASH(`id`);
```

#### 部分更新

本示例展示如何通过导入数据仅更新 StarRocks 表中列 `name` 的值。

1. 在 MySQL 客户端向 StarRocks 表 `score_board` 插入两行数据。

   ```SQL
   mysql> INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'spark', 100);

   mysql> select * from score_board;
   +------+-----------+-------+
   | id   | name      | score |
   +------+-----------+-------+
   |    1 | starrocks |   100 |
   |    2 | spark     |   100 |
   +------+-----------+-------+
   2 rows in set (0.02 sec)
   ```

2. 在 Spark SQL 客户端创建表 `score_board`。
   - 将选项 `starrocks.write.properties.partial_update` 设置为 `true`，以要求 Spark connector 执行部分更新。
   - 将选项 `starrocks.columns` 设置为 `id,name`，以告诉 Spark connector 需要更新的列。

   ```SQL
   CREATE TABLE `score_board`
   USING starrocks
   OPTIONS(
         "starrocks.fe.http.url"="127.0.0.1:8030",
         "starrocks.fe.jdbc.url"="jdbc:mysql://127.0.0.1:9030",
         "starrocks.table.identifier"="test.score_board",
         "starrocks.user"="root",
         "starrocks.password"="",
         "starrocks.write.properties.partial_update"="true",
         "starrocks.columns"="id,name"
      );
   ```

3. 在 Spark SQL 客户端将两行数据插入两行数据到表中。数据行的主键与 StarRocks 表的数据行主键相同，但是 `name` 列的值被修改。

   ```SQL
   INSERT INTO `score_board` VALUES (1, 'starrocks-update'), (2, 'spark-update');
   ```

4. 在 MySQL 客户端查询 StarRocks 表。

  ```SQL
  mysql> select * from score_board;
  +------+------------------+-------+
  | id   | name             | score |
  +------+------------------+-------+
  |    1 | starrocks-update |   100 |
  |    2 | spark-update     |   100 |
  +------+------------------+-------+
  2 rows in set (0.02 sec)
  ```

   您会看到只有 `name` 列的值发生了变化，而 `score` 列的值没有变化。

#### 条件更新

本示例展示如何根据 `score` 列的值进行条件更新。只有导入的数据行中 `score` 列值大于等于 StarRocks 表当前值时，该数据行才会更新。

1. 在 MySQL 客户端中向 StarRocks 表中插入两行数据。

   ```SQL
   mysql> INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'spark', 100);

   mysql> select * from score_board;
   +------+-----------+-------+
   | id   | name      | score |
   +------+-----------+-------+
   |    1 | starrocks |   100 |
   |    2 | spark     |   100 |
   +------+-----------+-------+
   2 rows in set (0.02 sec)
   ```

2. 在 Spark SQL 客户端按照以下方式创建表 `score_board`：

   - 将选项 `starrocks.write.properties.merge_condition` 设置为 `score`，要求 Spark connector 使用 `score`  列作为更新条件。
   - 确保 Spark connector 使用 Stream Load 接口导入数据，而不是 Stream Load 事务接口。因为 Stream Load 事务接口不支持条件更新。

   ```SQL
   CREATE TABLE `score_board`
   USING starrocks
   OPTIONS(
         "starrocks.fe.http.url"="127.0.0.1:8030",
         "starrocks.fe.jdbc.url"="jdbc:mysql://127.0.0.1:9030",
         "starrocks.table.identifier"="test.score_board",
         "starrocks.user"="root",
         "starrocks.password"="",
         "starrocks.write.properties.merge_condition"="score"
      );
   ```

3. 在 Spark SQL 客户端插入两行数据到表中。数据行的主键与 StarRocks 表中的行相同。第一行数据 `score` 列中具有较小的值，而第二行数据 `score` 列中具有较大的值。

   ```SQL
   INSERT INTO `score_board` VALUES (1, 'starrocks-update', 99), (2, 'spark-update', 101);
   ```

4. 在 MySQL 客户端查询 StarRocks 表。

   ```SQL
   mysql> select * from score_board;
   +------+--------------+-------+
   | id   | name         | score |
   +------+--------------+-------+
   |    1 | starrocks    |   100 |
   |    2 | spark-update |   101 |
   +------+--------------+-------+
   2 rows in set (0.03 sec)
   ```

   您会注意到仅第二行数据发生了变化，而第一行数据未发生变化。

### 导入至 BITMAP 列

`BITMAP` 常用于加速精确去重计数，例如计算独立访客数（UV），更多信息，请参见[使用 Bitmap 实现精确去重](../using_starrocks/Using_bitmap.md)。

本示例以计算独立访客数（UV）为例，展示如何导入数据至 StarRocks 表 `BITMAP` 列中。**自版本 1.1.1 起支持导入至 `BITMAP` 列**。

1. 在 MySQL 客户端中创建一个 StarRocks 聚合表。

   在数据库`test`中，创建聚合表 `page_uv`，其中列 `visit_users` 被定义为 `BITMAP` 类型，并配置聚合函数 `BITMAP_UNION`。

    ```SQL
    CREATE TABLE `test`.`page_uv` (
      `page_id` INT NOT NULL COMMENT 'page ID',
      `visit_date` datetime NOT NULL COMMENT 'access time',
      `visit_users` BITMAP BITMAP_UNION NOT NULL COMMENT 'user ID'
    ) ENGINE=OLAP
    AGGREGATE KEY(`page_id`, `visit_date`)
    DISTRIBUTED BY HASH(`page_id`);
    ```

2. 在 Spark SQL 客户端中创建一个表。

   Spark 表 schema 是从 StarRocks 表中推断出来的，而 Spark 不支持 BITMAP 类型。因此，您需要在 Spark 中自定义相应列的数据类型，例如配置选项 `"starrocks.column.types"="visit_users BIGINT"`，将其配置为 BIGINT 类型。在使用 Stream Load 来导入数据时，Spark connector 使用 `to_bitmap` 函数将 BIGINT 类型的数据转换为 BITMAP 类型。

   在 `spark-sql` 中运行如下 DDL 语句：

    ```SQL
    CREATE TABLE `page_uv`
    USING starrocks
    OPTIONS(
       "starrocks.fe.http.url"="127.0.0.1:8030",
       "starrocks.fe.jdbc.url"="jdbc:mysql://127.0.0.1:9030",
       "starrocks.table.identifier"="test.page_uv",
       "starrocks.user"="root",
       "starrocks.password"="",
       "starrocks.column.types"="visit_users BIGINT"
    );
    ```

3. 在 Spark SQL 客户端中插入数据至表中。

   在 `spark-sql` 中运行如下 DML 语句：

    ```SQL
    INSERT INTO `page_uv` VALUES
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 13),
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 23),
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 33),
       (1, CAST('2020-06-23 02:30:30' AS TIMESTAMP), 13),
       (2, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 23);
    ```

4. 在 MySQL 客户端查询 StarRocks 表来计算页面 UV 数。

    ```SQL
    MySQL [test]> SELECT `page_id`, COUNT(DISTINCT `visit_users`) FROM `page_uv` GROUP BY `page_id`;
    +---------+-----------------------------+
    | page_id | count(DISTINCT visit_users) |
    +---------+-----------------------------+
    |       2 |                           1 |
    |       1 |                           3 |
    +---------+-----------------------------+
    2 rows in set (0.01 sec)
    ```

> **注意**
>
> 如果 Spark 中该列的数据类型为 TINYINT、SMALLINT、INTEGER 或者 BIGINT 类型，则 Spark connector 使用 [`to_bitmap`](../sql-reference/sql-functions/bitmap-functions/to_bitmap.md) 函数将该列的数据转换为 StarRocks 中的 BITMAP 类型。如果 Spark 中该列为其它数据类型，则 Spark connector 使用 [`bitmap_hash`](../sql-reference/sql-functions/bitmap-functions/bitmap_hash.md) 函数进行转换。

### 导入至 HLL 列

`HLL` 可用于近似去重计数，更多信息，请参见[使用 HLL 实现近似去重](https://chat.openai.com/using_starrocks/Using_HLL)。

本示例以计算独立访客数（UV）为例，展示如何导入数据至 StarRocks 表 `HLL` 列中。**自版本 1.1.1 起支持导入至 `HLL` 列**。

1. 在 MySQL 客户端中创建一个 StarRocks 聚合表。

   在数据库 `test` 中，创建一个名为`hll_uv`的聚合表，其中列`visit_users`被定义为`HLL`类型，并配置聚合函数`HLL_UNION`。

   ```SQL
   CREATE TABLE `hll_uv` (
   `page_id` INT NOT NULL COMMENT 'page ID',
   `visit_date` datetime NOT NULL COMMENT 'access time',
   `visit_users` HLL HLL_UNION NOT NULL COMMENT 'user ID'
   ) ENGINE=OLAP
   AGGREGATE KEY(`page_id`, `visit_date`)
   DISTRIBUTED BY HASH(`page_id`);
   ```

2. 在 Spark SQL 客户端中创建一个表。

   Spark 表 schema 是从 StarRocks 表中推断出来的，而 Spark 不支持 HLL 类型。因此，您需要在 Spark 中自定义相应列的数据类型，例如配置选项 `"starrocks.column.types"="visit_users BIGINT"`，将其配置为 BIGINT 类型。在使用 Stream Load 来导入数据时，Spark connector 使用 [hll_hash](../sql-reference/sql-functions/aggregate-functions/hll_hash.md) 函数将 BIGINT 类型的数据转换为 HLL 类型。

   在 `spark-sql` 中运行如下 DDL 语句：

    ```SQL
    CREATE TABLE `hll_uv`
    USING starrocks
    OPTIONS(
       "starrocks.fe.http.url"="127.0.0.1:8030",
       "starrocks.fe.jdbc.url"="jdbc:mysql://127.0.0.1:9030",
       "starrocks.table.identifier"="test.hll_uv",
       "starrocks.user"="root",
       "starrocks.password"="",
       "starrocks.column.types"="visit_users BIGINT"
    );
    ```

3. 在 Spark SQL 客户端中插入数据至表中。

   在 `spark-sql` 中运行如下 DML 语句：

    ```SQL
    INSERT INTO `hll_uv` VALUES
       (3, CAST('2023-07-24 12:00:00' AS TIMESTAMP), 78),
       (4, CAST('2023-07-24 13:20:10' AS TIMESTAMP), 2),
       (3, CAST('2023-07-24 12:30:00' AS TIMESTAMP), 674);
    ```

4. 在 MySQL 客户端查询 StarRocks 表来计算页面 UV 数。

    ```SQL
    MySQL [test]> SELECT `page_id`, COUNT(DISTINCT `visit_users`) FROM `hll_uv` GROUP BY `page_id`;
    +---------+-----------------------------+
    | page_id | count(DISTINCT visit_users) |
    +---------+-----------------------------+
    |       4 |                           1 |
    |       3 |                           2 |
    +---------+-----------------------------+
    2 rows in set (0.01 sec)
    ```

### 导入至 ARRAY 列

以下示例说明了如何将数据导入到 ARRAY 类型的列中。

1. 创建 StarRocks 表

   在数据库 `test` 中创建一个主键表 `array_tbl`，该表包括一个 `INT` 列和两个 `ARRAY` 列。

   ```sql
   CREATE TABLE `array_tbl` (
     `id` INT NOT NULL,
     `a0` ARRAY<STRING>,
     `a1` ARRAY<ARRAY<INT>>
   ) ENGINE=OLAP
   PRIMARY KEY(`id`)
   DISTRIBUTED BY HASH(`id`);
   ```

2. 写入数据至 StarRocks 表。

   由于某些版本的 StarRocks 不提供 ARRAY 列的元数据，因此 Spark connector 无法推断出该列的对应的 Spark 数据类型。但是，您可以在选项 starrocks.column.type s中显式指定列的相应 Spark 数据类型。在这个示例中，您可以将选项配置为 `a0 ARRAY<STRING>,a1 ARRAY<ARRAY<INT>>`。

   如果是使用 Scala 语言，则在 `spark-shell` 中可以运行如下代码：

   ```scala
   val data = Seq(
      |  (1, Seq("hello", "starrocks"), Seq(Seq(1, 2), Seq(3, 4))),
      |  (2, Seq("hello", "spark"), Seq(Seq(5, 6, 7), Seq(8, 9, 10)))
      | )
   val df = data.toDF("id", "a0", "a1")
   df.write
        .format("starrocks")
        .option("starrocks.fe.http.url", "127.0.0.1:8030")
        .option("starrocks.fe.jdbc.url", "jdbc:mysql://127.0.0.1:9030")
        .option("starrocks.table.identifier", "test.array_tbl")
        .option("starrocks.user", "root")
        .option("starrocks.password", "")
        .option("starrocks.column.types", "a0 ARRAY<STRING>,a1 ARRAY<ARRAY<INT>>")
        .mode("append")
        .save()
   ```

3. 查询 StarRocks 表。

   ```SQL
   MySQL [test]> SELECT * FROM `array_tbl`;
   +------+-----------------------+--------------------+
   | id   | a0                    | a1                 |
   +------+-----------------------+--------------------+
   |    1 | ["hello","starrocks"] | [[1,2],[3,4]]      |
   |    2 | ["hello","spark"]     | [[5,6,7],[8,9,10]] |
   +------+-----------------------+--------------------+
   2 rows in set (0.01 sec)
   ```
