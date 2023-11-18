---
displayed_sidebar: "Chinese"
---

# 使用 Spark Connector 读取数据

StarRocks 提供 Apache Spark™ Connector (StarRocks Connector for Apache Spark™)，支持通过 Spark 读取 StarRocks 中存储的数据。您可以使用 Spark 对读取到的数据进行复杂处理、机器学习等。

Spark Connector 支持三种数据读取方式：Spark SQL、Spark DataFrame 和 Spark RDD。

您可以使用 Spark SQL 在 StarRocks 表上创建临时视图，然后通过临时视图直接读取 StarRocks 表的数据。

您也可以将 StarRocks 表映射为 Spark DataFrame 或者 Spark RDD，然后从 Spark DataFrame 或者 Spark RDD 中读取数据。推荐使用 Spark DataFrame 来读取 StarRocks 中存储的数据。

## 使用说明

- 支持在 StarRocks 端完成数据过滤，从而减少数据传输量。

- 如果读取数据的开销比较大，可以通过合理的表设计和使用过滤条件，控制 Spark不要一次读取过多的数据，从而避免给磁盘和网络造成过大的 I/O 压力或影响正常的查询业务。

## 版本要求

| Spark Connector | Spark         | StarRocks   | Java | Scala |
|---------------- | ------------- | ----------- | ---- | ----- |
| 1.1.1           | 3.2, 3.3, 3.4 | 2.5 及以上   | 8    | 2.12  |
| 1.1.0           | 3.2, 3.3, 3.4 | 2.5 及以上   | 8    | 2.12  |
| 1.0.0           | 3.x           | 1.18 及以上  | 8    | 2.12  |
| 1.0.0           | 2.x           | 1.18 及以上  | 8    | 2.11  |

> **注意**
>
> - 了解不同版本的 Spark connector 之间的行为变化，请查看[升级 Spark connector](#100-升级至-110)。
> - 自 1.1.1 版本起，Spark connector 不再提供 MySQL JDBC 驱动程序，您需要将驱动程序手动放到 Spark 的类路径中。您可以在 [MySQL 官网](https://dev.mysql.com/downloads/connector/j/)或 [Maven 中央仓库](https://repo1.maven.org/maven2/mysql/mysql-connector-java/)上找到该驱动程序。
> - 1.0.0 版本只支持读取 StarRocks，从 1.1.0 版本开始同时支持读写 StarRocks。
> - 1.0.0 版本和 1.1.0 版本在参数和类型映射上存在差别，请查看[升级 Spark connector](#100-升级至-110)。
> - 1.0.0 版本一般情况下不再增加新功能，条件允许请尽快升级 Spark Connector。

## 获取 Spark Connector

您可以通过以下方式获取 Spark Connector Jar 包：

- 直接下载已经编译好的 Jar 包。
- 通过 Maven 添加 Spark Connector 的依赖 (仅支持 1.1.0 及以上版本)。
- 通过源码手动编译。

### Spark Connector 1.1.0 及以上版本

Spark Connector Jar 包的命名格式如下：

`starrocks-spark-connector-${spark_version}_${scala_version}-${connector_version}.jar`

比如，您想在 Spark 3.2 和 Scala 2.12 上使用 1.1.0 版本的 Spark Connector，可以选择 `starrocks-spark-connector-3.2_2.12-1.1.0.jar`。

> **注意**
>
> 一般情况下最新版本的 Spark Connector 只维护最近三个版本的 Spark。

#### 直接下载

您可以在 [Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks) 获取不同版本的 Spark Connector Jar 包。

#### 添加 Maven 依赖

依赖配置的格式如下：

> **注意**
>
> 需要将 `spark_version`、`scala_version` 和 `connector_version` 替换成对应的版本。

```xml
<dependency>
  <groupId>com.starrocks</groupId>
  <artifactId>starrocks-spark-connector-${spark_version}_${scala_version}</artifactId>
  <version>${connector_version}</version>
</dependency>
```

比如，您想在 Spark 3.2 和 Scala 2.12 上使用 1.1.0 版本的 Spark Connector，可以添加如下依赖：

```xml
<dependency>
  <groupId>com.starrocks</groupId>
  <artifactId>starrocks-spark-connector-3.2_2.12</artifactId>
  <version>1.1.0</version>
</dependency>
```

#### 手动编译

1. 下载 [Spark Connector 代码](https://github.com/StarRocks/starrocks-connector-for-apache-spark)。

2. 通过如下命令进行编译:

   > **注意**
   >
   > 需要将 `spark_version` 替换成相应的 Spark 版本。

   ```shell
   sh build.sh <spark_version>
   ```

   比如，您想在 Spark 3.2 上使用 Spark Connector，可以通过如下命令进行编译：

   ```shell
   sh build.sh 3.2
   ```

3. 编译完成后，进入 `target/` 路径查看。路径下会生成 Spark Connector Jar 包，比如 `starrocks-spark-connector-3.2_2.12-1.1.0-SNAPSHOT.jar`。

   > **注意**
   >
   > 如果使用的是非正式发布的 Spark Connector 版本，生成的 Spark Connector Jar 包名称中会带有 `SNAPSHOT` 后缀。

### Spark Connector 1.0.0

#### 直接下载

- [Spark 2.x](https://cdn-thirdparty.starrocks.com/spark/starrocks-spark2_2.11-1.0.0.jar)
- [Spark 3.x](https://cdn-thirdparty.starrocks.com/spark/starrocks-spark3_2.12-1.0.0.jar)

#### 手动编译

1. 下载 [Spark Connector 代码](https://github.com/StarRocks/starrocks-connector-for-apache-spark/tree/spark-1.0)。

   > **注意**
   >
   > 需要切换到 `spark-1.0` 分支。

2. 通过如下命令对 Spark Connector 进行编译：

   - 如果 Spark 版本是 2.x，则执行如下命令，默认编译的是配套 Spark 2.3.4 的 Spark Connector：

     ```Plain
     sh build.sh 2
     ```

   - 如果 Spark 版本是 3.x，则执行如下命令，默认编译的是配套 Spark 3.1.2 的 Spark Connector：

     ```Plain
     sh build.sh 3
     ```

3. 编译完成后，进入 `output/` 路径查看。路径下会生成 `starrocks-spark2_2.11-1.0.0.jar` 文件。将该文件拷贝至 Spark 的类文件路径 (Classpath) 下：

   - 如果您的 Spark 以 `Local` 模式运行，需要把该文件放在 `jars/` 路径下。
   - 如果您的 Spark 以 `Yarn` 模式运行，需要把该文件放在预安装程序包 (Pre-deployment Package) 里。

把文件放置到指定位置后，才可以开始使用 Spark Connector 读取数据。

## 参数说明

本小节描述您在使用 Spark Connector 读取数据的过程中需要配置的参数。

### 通用参数

以下参数适用于 Spark SQL、Spark DataFrame、Spark RDD 三种读取方式。

| 参数名称                             | 默认值            | 说明                                                         |
| ------------------------------------ | ----------------- | ------------------------------------------------------------ |
| starrocks.fenodes                    | 无                | StarRocks 集群中 FE 的 HTTP 地址，格式为 `<fe_host>:<fe_http_port>`。支持输入多个地址，使用逗号 (,) 分隔。 |
| starrocks.table.identifier           | 无                | StarRocks 表的名称，格式为 `<database_name>.<table_name>`。   |
| starrocks.request.retries            | 3                 | Spark Connector 向 StarRocks 发送一个读请求的重试次数。                            |
| starrocks.request.connect.timeout.ms | 30000             | 一个读请求的连接建立超时时间。                       |
| starrocks.request.read.timeout.ms    | 30000             | 一个读请求读取 StarRocks 数据超时时间。|
| starrocks.request.query.timeout.s    | 3600              | 从 StarRocks 查询数据的超时时间。默认超时时间为 1 小时。|
| starrocks.request.tablet.size        | Integer.MAX_VALUE | 一个 Spark RDD 分区对应的 StarRocks Tablet 的个数。参数设置越小，生成的分区越多，Spark 侧的并行度也就越大，但与此同时会给 StarRocks 侧造成更大的压力。 |
| starrocks.batch.size                 | 4096              | 单次从 BE 读取的最大行数。调大参数取值可减少 Spark 与 StarRocks 之间建立连接的次数，从而减轻网络延迟所带来的的额外时间开销。对于StarRocks 2.2及以后版本最小支持的batch size为4096，如果配置小于该值，则按4096处理 |
| starrocks.exec.mem.limit             | 2147483648        | 单个查询的内存限制。单位：字节。默认内存限制为 2 GB。        |
| starrocks.deserialize.arrow.async    | false             | 是否支持把 Arrow 格式异步转换为 Spark Connector 迭代所需的 RowBatch。 |
| starrocks.deserialize.queue.size     | 64                | 异步转换 Arrow 格式时内部处理队列的大小，当 `starrocks.deserialize.arrow.async` 为 `true` 时生效。 |
| starrocks.filter.query               | 无                | 指定过滤条件。多个过滤条件用 `and` 连接。StarRocks 根据指定的过滤条件完成对待读取数据的过滤。 |
| starrocks.timezone | JVM 默认时区|自 1.1.1 版本起支持。StarRocks 的时区。用于将 StarRocks 的 `DATETIME` 类型的值转换为 Spark 的 `TimestampType` 类型的值。默认为 `ZoneId#systemDefault()` 返回的 JVM 时区。格式可以是时区名称，例如 Asia/Shanghai，或时区偏移，例如 +08:00。|

### Spark SQL 和 Spark DataFrame 专有参数

以下参数仅适用于 Spark SQL 和 Spark DataFrame 读取方式。

| 参数名称                             | 默认值  | 说明                                                         |
| ----------------------------------- | ------ | ------------------------------------------------------------ |
| starrocks.fe.http.url               | 无     | FE 的 HTTP 地址。从 Spark Connector 1.1.0 版本开始支持，与 `starrocks.fenodes` 等价，两者填一个即可。在 Spark Connector 1.1.0 及以后版本，推荐使用该参数，`starrocks.fenodes` 在后续版本可能会淘汰。 |
| starrocks.fe.jdbc.url               | 无     | FE 的 MySQL Server 连接地址。格式为 `jdbc:mysql://<fe_host>:<fe_query_port>`。<br />**注意**<br />在 Spark Connector 1.1.0 及以后版本，该参数必填。    |
| user                                | 无     | StarRocks 集群账号的用户名。  |
| starrocks.user                      | 无     | StarRocks 集群账号的用户名。从 Spark Connector 1.1.0 版本开始支持，与 `user` 等价，两者填一个即可。在 Spark Connector 1.1.0 及以后版本，推荐使用该参数，`user` 在后续版本可能会淘汰。   |
| password                            | 无     | StarRocks 集群账号的用户密码。  |
| starrocks.password                  | 无     | StarRocks 集群账号的用户密码。 从 Spark Connector 1.1.0 版本开始支持，与 `password` 等价，两者填一个即可。在 Spark Connector 1.1.0 及以后版本，推荐使用该参数，`password` 在后续版本可能会淘汰。   |
| starrocks.filter.query.in.max.count | 100    | 谓词下推中，IN 表达式支持的取值数量上限。如果 IN 表达式中指定的取值数量超过该上限，则 IN 表达式中指定的条件过滤在 Spark 侧处理。  |

### Spark RDD 专有参数

以下参数仅适用于 Spark RDD 读取方式。

| 参数名称                        | 默认值 | 说明                                                         |
| ------------------------------- | ------ | ------------------------------------------------------------ |
| starrocks.request.auth.user     | 无     | StarRocks 集群账号的用户名。                                 |
| starrocks.request.auth.password | 无     | StarRocks 集群账号的用户密码。                               |
| starrocks.read.field            | 无     | 指定从 StarRocks 表中读取哪些列的数据。多个列名之间使用逗号 (,) 分隔。 |

## 数据类型映射关系

### Spark Connector 1.1.0 及以上版本

| StarRocks 数据类型 | Spark 数据类型           |
|----------------- |-------------------------|
| BOOLEAN          | DataTypes.BooleanType   |
| TINYINT          | DataTypes.ByteType      |
| SMALLINT         | DataTypes.ShortType     |
| INT              | DataTypes.IntegerType   |
| BIGINT           | DataTypes.LongType      |
| LARGEINT         | DataTypes.StringType    |
| FLOAT            | DataTypes.FloatType     |
| DOUBLE           | DataTypes.DoubleType    |
| DECIMAL          | DecimalType             |
| CHAR             | DataTypes.StringType    |
| VARCHAR          | DataTypes.StringType    |
| STRING           | DataTypes.StringType    |
| DATE             | DataTypes.DateType      |
| DATETIME         | DataTypes.TimestampType |
| ARRAY            | Unsupported datatype    |
| HLL              | Unsupported datatype    |
| BITMAP           | Unsupported datatype    |

### Spark Connector 1.0.0 版本

| StarRocks 数据类型  | Spark 数据类型          |
| ------------------ | --------------------- |
| BOOLEAN            | DataTypes.BooleanType |
| TINYINT            | DataTypes.ByteType    |
| SMALLINT           | DataTypes.ShortType   |
| INT                | DataTypes.IntegerType |
| BIGINT             | DataTypes.LongType    |
| LARGEINT           | DataTypes.StringType  |
| FLOAT              | DataTypes.FloatType   |
| DOUBLE             | DataTypes.DoubleType  |
| DECIMAL            | DecimalType           |
| CHAR               | DataTypes.StringType  |
| VARCHAR            | DataTypes.StringType  |
| DATE               | DataTypes.StringType  |
| DATETIME           | DataTypes.StringType  |
| ARRAY              | Unsupported datatype  |
| HLL                | Unsupported datatype  |
| BITMAP             | Unsupported datatype  |

Spark Connector 中，将 DATE 和 DATETIME 数据类型映射为 STRING 数据类型。因为 StarRocks 底层存储引擎处理逻辑，直接使用 DATE 和 DATETIME 数据类型时，覆盖的时间范围无法满足需求。所以，使用 STRING 数据类型直接返回对应的时间可读文本。

## Spark Connector 升级

### 1.0.0 升级至 1.1.0

- 自 1.1.1 版本开始，Spark connector 不再提供 MySQL 官方 JDBC 驱动程序 `mysql-connector-java`，因为该驱动程序使用 GPL 许可证，存在一些限制。然而，Spark连接器仍然需要 MySQL JDBC 驱动程序才能连接到 StarRocks 以获取表的元数据，因此您需要手动将驱动程序添加到 Spark 类路径中。您可以在 [MySQL 官网](https://dev.mysql.com/downloads/connector/j/) 或 [Maven 中央仓库](https://repo1.maven.org/maven2/mysql/mysql-connector-java/)上找到这个驱动程序。

- 1.1.0 版本需要通过 JDBC 访问 StarRocks 以获取更详细的表信息，因此必须配置 `starrocks.fe.jdbc.url`。

- 1.1.0 版本调整了一些参数命名，目前同时保留了调整前后的参数，只需配置一个即可，但是推荐使用新的，旧的参数在之后的版本可能会淘汰：
  - `starrocks.fenodes` 调整为 `starrocks.fe.http.url`。
  - `user` 调整为 `starrocks.user`。
  - `password` 调整为 `starrocks.password`。

- 1.1.0 版本基于 Spark 3.x 调整了部分类型映射：
  - StarRocks 的 `DATE` 映射为 Spark 的 `DataTypes.DateType`，原来是 `DataTypes.StringType`。
  - StarRocks 的 `DATETIME` 映射为 Spark 的 `DataTypes.TimestampType`，原来是 `DataTypes.StringType`。

## 使用示例

假设您的 StarRocks 集群中已创建数据库 `test`，并且您拥有 `root` 账号权限。示例的参数配置基于 Spark Connector 1.1.0 版本。

### 数据样例

执行如下步骤，准备数据样例：

1. 进入 `test` 数据库，创建一张名为 `score_board` 的表。

   ```SQL
   MySQL [test]> CREATE TABLE `score_board`
   (
       `id` int(11) NOT NULL COMMENT "",
       `name` varchar(65533) NULL DEFAULT "" COMMENT "",
       `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
   )
   ENGINE=OLAP
   PRIMARY KEY(`id`)
   COMMENT "OLAP"
   DISTRIBUTED BY HASH(`id`) BUCKETS 1
   PROPERTIES (
       "replication_num" = "3"
   );
   ```

2. 向 `score_board` 表中插入数据。

   ```SQL
   MySQL [test]> INSERT INTO score_board
   VALUES
       (1, 'Bob', 21),
       (2, 'Stan', 21),
       (3, 'Sam', 22),
       (4, 'Tony', 22),
       (5, 'Alice', 22),
       (6, 'Lucy', 23),
       (7, 'Polly', 23),
       (8, 'Tom', 23),
       (9, 'Rose', 24),
       (10, 'Jerry', 24),
       (11, 'Jason', 24),
       (12, 'Lily', 25),
       (13, 'Stephen', 25),
       (14, 'David', 25),
       (15, 'Eddie', 26),
       (16, 'Kate', 27),
       (17, 'Cathy', 27),
       (18, 'Judy', 27),
       (19, 'Julia', 28),
       (20, 'Robert', 28),
       (21, 'Jack', 29);
   ```

3. 查询 `score_board` 表的数据。

   ```SQL
   MySQL [test]> SELECT * FROM score_board;
   +------+---------+-------+
   | id   | name    | score |
   +------+---------+-------+
   |    1 | Bob     |    21 |
   |    2 | Stan    |    21 |
   |    3 | Sam     |    22 |
   |    4 | Tony    |    22 |
   |    5 | Alice   |    22 |
   |    6 | Lucy    |    23 |
   |    7 | Polly   |    23 |
   |    8 | Tom     |    23 |
   |    9 | Rose    |    24 |
   |   10 | Jerry   |    24 |
   |   11 | Jason   |    24 |
   |   12 | Lily    |    25 |
   |   13 | Stephen |    25 |
   |   14 | David   |    25 |
   |   15 | Eddie   |    26 |
   |   16 | Kate    |    27 |
   |   17 | Cathy   |    27 |
   |   18 | Judy    |    27 |
   |   19 | Julia   |    28 |
   |   20 | Robert  |    28 |
   |   21 | Jack    |    29 |
   +------+---------+-------+
   21 rows in set (0.01 sec)
   ```

### 使用 Spark SQL 读取数据

1. 进入 Spark 的可执行程序目录，运行如下命令：

   ```Plain
   sh spark-sql
   ```

2. 运行如下命令，在数据库 `test` 中的表 `score_board` 上创建一个名为 `spark_starrocks` 的临时视图：

   ```SQL
   spark-sql> CREATE TEMPORARY VIEW spark_starrocks
              USING starrocks
              OPTIONS
              (
                  "starrocks.table.identifier" = "test.score_board",
                  "starrocks.fe.http.url" = "<fe_host>:<fe_http_port>",
                  "starrocks.fe.jdbc.url" = "jdbc:mysql://<fe_host>:<fe_query_port>",
                  "starrocks.user" = "root",
                  "starrocks.password" = ""
              );
   ```

3. 运行如下命令，从临时视图中读取数据：

   ```SQL
   spark-sql> SELECT * FROM spark_starrocks;
   ```

   返回数据如下：

   ```SQL
   1        Bob        21
   2        Stan        21
   3        Sam        22
   4        Tony        22
   5        Alice        22
   6        Lucy        23
   7        Polly        23
   8        Tom        23
   9        Rose        24
   10        Jerry        24
   11        Jason        24
   12        Lily        25
   13        Stephen        25
   14        David        25
   15        Eddie        26
   16        Kate        27
   17        Cathy        27
   18        Judy        27
   19        Julia        28
   20        Robert        28
   21        Jack        29
   Time taken: 1.883 seconds, Fetched 21 row(s)
   22/08/09 15:29:36 INFO thriftserver.SparkSQLCLIDriver: Time taken: 1.883 seconds, Fetched 21 row(s)
   ```

### 使用 Spark DataFrame 读取数据

1. 进入 Spark 的可执行程序目录，运行如下命令：

   ```Plain
   sh spark-shell
   ```

2. 运行如下命令，在 Spark 中根据 StarRocks 数据库 `test` 中的表 `score_board` 创建一个名为 `starrocksSparkDF` 的 DataFrame：

   ```Scala
   scala> val starrocksSparkDF = spark.read.format("starrocks")
              .option("starrocks.table.identifier", s"test.score_board")
              .option("starrocks.fe.http.url", s"<fe_host>:<fe_http_port>")
              .option("starrocks.fe.jdbc.url", s"jdbc:mysql://<fe_host>:<fe_query_port>")
              .option("starrocks.user", s"root")
              .option("starrocks.password", s"")
              .load()
   ```

3. 从 DataFrame 中读取数据。例如，如果要读取前 10 行数据，需要运行如下命令：

   ```Scala
   scala> starrocksSparkDF.show(10)
   ```

   返回数据如下：

   ```Scala
   +---+-----+-----+
   | id| name|score|
   +---+-----+-----+
   |  1|  Bob|   21|
   |  2| Stan|   21|
   |  3|  Sam|   22|
   |  4| Tony|   22|
   |  5|Alice|   22|
   |  6| Lucy|   23|
   |  7|Polly|   23|
   |  8|  Tom|   23|
   |  9| Rose|   24|
   | 10|Jerry|   24|
   +---+-----+-----+
   only showing top 10 rows
   ```

  > **说明**
  >
  > 如果不指定要读取的行数，则默认读取前 20 行数据。

### 使用 Spark RDD 读取数据

1. 进入 Spark 的可执行程序目录，运行如下命令：

   ```Plain
   sh spark-shell
   ```

2. 运行如下命令，在 Spark 中根据 StarRocks 数据库 `test` 中的表 `score_board` 创建一个名为 `starrocksSparkRDD` 的 RDD：

   ```Scala
   scala> import com.starrocks.connector.spark._
   scala> val starrocksSparkRDD = sc.starrocksRDD
              (
              tableIdentifier = Some("test.score_board"),
              cfg = Some(Map(
                  "starrocks.fenodes" -> "<fe_host>:<fe_http_port>",
                  "starrocks.request.auth.user" -> "root",
                  "starrocks.request.auth.password" -> ""
              ))
              )
   ```

3. 从 RDD 中读取数据。例如，如果要读取前 10 个 元素 (Element) 的数据，需要运行如下命令：

   ```Scala
   scala> starrocksSparkRDD.take(10)
   ```

   返回数据如下：

   ```Scala
   res0: Array[AnyRef] = Array([1, Bob, 21], [2, Stan, 21], [3, Sam, 22], [4, Tony, 22], [5, Alice, 22], [6, Lucy, 23], [7, Polly, 23], [8, Tom, 23], [9, Rose, 24], [10, Jerry, 24])
   ```

   如果要读取整个 RDD，需要运行如下命令：

   ```Scala
   scala> starrocksSparkRDD.collect()
   ```

   返回数据如下：

   ```Scala
   res1: Array[AnyRef] = Array([1, Bob, 21], [2, Stan, 21], [3, Sam, 22], [4, Tony, 22], [5, Alice, 22], [6, Lucy, 23], [7, Polly, 23], [8, Tom, 23], [9, Rose, 24], [10, Jerry, 24], [11, Jason, 24], [12, Lily, 25], [13, Stephen, 25], [14, David, 25], [15, Eddie, 26], [16, Kate, 27], [17, Cathy, 27], [18, Judy, 27], [19, Julia, 28], [20, Robert, 28], [21, Jack, 29])
   ```

## 最佳实践

使用 Spark Connector 从 StarRocks 读取数据的时候，可以通过 `starrocks.filter.query` 参数指定过滤条件来做合理的分区、分桶、前缀索引裁剪，减少拉取数据的开销。这里以 Spark DataFrame 为例进行介绍，通过查看执行计划来验证实际数据裁剪的效果。

### 环境配置

| 组件            | 版本                                                         |
| --------------- | ------------------------------------------------------------ |
| Spark           | Spark 2.4.4 和 Scala 2.11.12 (OpenJDK 64-Bit Server VM, Java 1.8.0_302) |
| StarRocks       | 2.2.0                                                       |
| Spark Connector | starrocks-spark2_2.11-1.0.0.jar                              |

### 数据样例

执行如下步骤，准备数据样例：

1. 进入 `test` 数据库，创建一张名为 `mytable` 的表。

   ```SQL
   MySQL [test]> CREATE TABLE `mytable`
   (
       `k` int(11) NULL COMMENT "bucket",
       `b` int(11) NULL COMMENT "",
       `dt` datetime NULL COMMENT "",
       `v` int(11) NULL COMMENT ""
   )
   ENGINE=OLAP
   DUPLICATE KEY(`k`,`b`, `dt`)
   COMMENT "OLAP"
   PARTITION BY RANGE(`dt`)
   (
       PARTITION p202201 VALUES [('2022-01-01 00:00:00'), ('2022-02-01 00:00:00')),
       PARTITION p202202 VALUES [('2022-02-01 00:00:00'), ('2022-03-01 00:00:00')),
       PARTITION p202203 VALUES [('2022-03-01 00:00:00'), ('2022-04-01 00:00:00'))
   )
   DISTRIBUTED BY HASH(`k`) BUCKETS 3
   PROPERTIES (
       "replication_num" = "3"
   );
   ```

2. 向 `mytable` 表中插入数据。

   ```SQL
   MySQL [test]> INSERT INTO mytable
   VALUES
        (1, 11, '2022-01-02 08:00:00', 111),
        (2, 22, '2022-02-02 08:00:00', 222),
        (3, 33, '2022-03-02 08:00:00', 333);
   ```

3. 查询 `mytable` 表的数据。

   ```SQL
   MySQL [test]> select * from mytable;
   +------+------+---------------------+------+
   | k    | b    | dt                  | v    |
   +------+------+---------------------+------+
   |    1 |   11 | 2022-01-02 08:00:00 |  111 |
   |    2 |   22 | 2022-02-02 08:00:00 |  222 |
   |    3 |   33 | 2022-03-02 08:00:00 |  333 |
   +------+------+---------------------+------+
   3 rows in set (0.01 sec)
   ```

### 全表扫描

1. 在 Spark 可执行程序目录下，通过如下命令，根据数据库 `test` 中的表 `mytable` 创建一个名为 `df` 的 DataFrame：

   ```Scala
   scala>  val df = spark.read.format("starrocks")
           .option("starrocks.table.identifier", s"test.mytable")
           .option("starrocks.fenodes", s"<fe_host>:<fe_http_port>")
           .option("user", s"root")
           .option("password", s"")
           .load()
   ```

2. 查看 StarRocks 的 FE 日志文件 **fe.log**，找到 Spark 读取数据所用的 SQL 语句，如下所示：

   ```SQL
   2022-08-09 18:57:38,091 INFO (nioEventLoopGroup-3-10|196) [TableQueryPlanAction.executeWithoutPassword():126] receive SQL statement [select `k`,`b`,`dt`,`v` from `test`.`mytable`] from external service [ user ['root'@'%']] for database [test] table [mytable]
   ```

3. 在数据库 `test` 下，使用 EXPLAIN 来获取 SELECT `k`,`b`,`dt`,`v` from `test`.`mytable` 语句的执行计划，如下所示：

   ```Scala
   MySQL [test]> EXPLAIN select `k`,`b`,`dt`,`v` from `test`.`mytable`;
   +-----------------------------------------------------------------------+
   | Explain String                                                        |
   +-----------------------------------------------------------------------+
   | PLAN FRAGMENT 0                                                       |
   |  OUTPUT EXPRS:1: k | 2: b | 3: dt | 4: v                              |
   |   PARTITION: UNPARTITIONED                                            |
   |                                                                       |
   |   RESULT SINK                                                         |
   |                                                                       |
   |   1:EXCHANGE                                                          |
   |                                                                       |
   | PLAN FRAGMENT 1                                                       |
   |  OUTPUT EXPRS:                                                        |
   |   PARTITION: RANDOM                                                   |
   |                                                                       |
   |   STREAM DATA SINK                                                    |
   |     EXCHANGE ID: 01                                                   |
   |     UNPARTITIONED                                                     |
   |                                                                       |
   |   0:OlapScanNode                                                      |
   |      TABLE: mytable                                                   |
   |      PREAGGREGATION: ON                                               |
   |      partitions=3/3                                                   |
   |      rollup: mytable                                                  |
   |      tabletRatio=9/9                                                  |
   |      tabletList=41297,41299,41301,41303,41305,41307,41309,41311,41313 |
   |      cardinality=3                                                    |
   |      avgRowSize=4.0                                                   |
   |      numNodes=0                                                       |
   +-----------------------------------------------------------------------+
   26 rows in set (0.00 sec)
   ```

这里未作剪裁。因此，会扫描所有包含数据的 3 个分区 (`partitions=3/3` )、以及这个三个分区中所有的 9 个 Tablet (`tabletRatio=9/9`)。

### 分区裁剪

1. 在 Spark 可执行程序目录下，通过如下命令，根据数据库 `test` 中的表 `mytable` 创建一个名为 `df` 的 DataFrame，命令中使用 `starrocks.filter.query` 参数指定过滤条件为 `dt='2022-01-02 08:00:00'`，以做分区剪裁：

   ```Scala
   scala> val df = spark.read.format("starrocks")
          .option("starrocks.table.identifier", s"test.mytable")
          .option("starrocks.fenodes", s"<fe_host>:<fe_http_port>")
          .option("user", s"root")
          .option("password", s"")
          .option("starrocks.filter.query", "dt='2022-01-02 08:00:00'")
          .load()
   ```

2. 查看 StarRocks 的 FE 日志文件 **fe.log**，找到 Spark 读取数据所用的 SQL 语句，如下所示：

   ```SQL
   2022-08-09 19:02:31,253 INFO (nioEventLoopGroup-3-14|204) [TableQueryPlanAction.executeWithoutPassword():126] receive SQL statement [select `k`,`b`,`dt`,`v` from `test`.`mytable` where dt='2022-01-02 08:00:00'] from external service [ user ['root'@'%']] for database [test] table [mytable]
   ```

3. 在数据库 `test` 下，使用 EXPLAIN 来获取 SELECT `k`,`b`,`dt`,`v` from `test`.`mytable` where dt='2022-01-02 08:00:00' 语句的执行计划，如下所示：

   ```Scala
   MySQL [test]> EXPLAIN select `k`,`b`,`dt`,`v` from `test`.`mytable` where dt='2022-01-02 08:00:00';
   +------------------------------------------------+
   | Explain String                                 |
   +------------------------------------------------+
   | PLAN FRAGMENT 0                                |
   |  OUTPUT EXPRS:1: k | 2: b | 3: dt | 4: v       |
   |   PARTITION: UNPARTITIONED                     |
   |                                                |
   |   RESULT SINK                                  |
   |                                                |
   |   1:EXCHANGE                                   |
   |                                                |
   | PLAN FRAGMENT 1                                |
   |  OUTPUT EXPRS:                                 |
   |   PARTITION: RANDOM                            |
   |                                                |
   |   STREAM DATA SINK                             |
   |     EXCHANGE ID: 01                            |
   |     UNPARTITIONED                              |
   |                                                |
   |   0:OlapScanNode                               |
   |      TABLE: mytable                            |
   |      PREAGGREGATION: ON                        |
   |      PREDICATES: 3: dt = '2022-01-02 08:00:00' |
   |      partitions=1/3                            |
   |      rollup: mytable                           |
   |      tabletRatio=3/3                           |
   |      tabletList=41297,41299,41301              |
   |      cardinality=1                             |
   |      avgRowSize=20.0                           |
   |      numNodes=0                                |
   +------------------------------------------------+
   27 rows in set (0.01 sec)
   ```

这里只做了分区剪裁，未做分桶剪裁。因此，会扫描 3 个分区里的 1 个分区 (`partitions=1/3`)、以及该分区下的所有 Tablet (`tabletRatio=3/3`)。

### 分桶裁剪

1. 在 Spark 可执行程序目录下，通过如下命令，根据数据库 `test` 中的表 `mytable` 创建一个名为 `df` 的 DataFrame，命令中使用 `starrocks.filter.query` 参数指定过滤条件为 `k=1`，以做分桶剪裁：

   ```Scala
   scala> val df = spark.read.format("starrocks")
          .option("starrocks.table.identifier", s"test.mytable")
          .option("starrocks.fenodes", s"<fe_host>:<fe_http_port>")
          .option("user", s"root")
          .option("password", s"")
          .option("starrocks.filter.query", "k=1")
          .load()
   ```

2. 查看 StarRocks 的 FE 日志文件 **fe.log**，找到 Spark 读取数据所用的 SQL 语句，如下所示：

   ```SQL
   2022-08-09 19:04:44,479 INFO (nioEventLoopGroup-3-16|208) [TableQueryPlanAction.executeWithoutPassword():126] receive SQL statement [select `k`,`b`,`dt`,`v` from `test`.`mytable` where k=1] from external service [ user ['root'@'%']] for database [test] table [mytable]
   ```

3. 在数据库 `test` 数据库下，使用 EXPLAIN 来获取 SELECT `k`,`b`,`dt`,`v` from `test`.`mytable` where k=1 语句的执行计划，如下所示：

   ```Scala
   MySQL [test]> EXPLAIN select `k`,`b`,`dt`,`v` from `test`.`mytable` where k=1;
   +------------------------------------------+
   | Explain String                           |
   +------------------------------------------+
   | PLAN FRAGMENT 0                          |
   |  OUTPUT EXPRS:1: k | 2: b | 3: dt | 4: v |
   |   PARTITION: UNPARTITIONED               |
   |                                          |
   |   RESULT SINK                            |
   |                                          |
   |   1:EXCHANGE                             |
   |                                          |
   | PLAN FRAGMENT 1                          |
   |  OUTPUT EXPRS:                           |
   |   PARTITION: RANDOM                      |
   |                                          |
   |   STREAM DATA SINK                       |
   |     EXCHANGE ID: 01                      |
   |     UNPARTITIONED                        |
   |                                          |
   |   0:OlapScanNode                         |
   |      TABLE: mytable                      |
   |      PREAGGREGATION: ON                  |
   |      PREDICATES: 1: k = 1                |
   |      partitions=3/3                      |
   |      rollup: mytable                     |
   |      tabletRatio=3/9                     |
   |      tabletList=41299,41305,41311        |
   |      cardinality=1                       |
   |      avgRowSize=20.0                     |
   |      numNodes=0                          |
   +------------------------------------------+
   27 rows in set (0.01 sec)
   ```

这里未做分区剪裁，只做了分桶裁剪。因此，会扫描所有包含数据的 3 个分区 (`partitions=3/3`)、以及这 3 个分区下符合 `k = 1` 的 Hash 值的所有 3 个 Tablet (`tabletRatio=3/9`)。

### 分区分桶剪裁

1. 在 Spark 可执行程序目录下，通过如下命令，根据数据库 `test` 中的表 `mytable` 创建一个名为 `df` 的 DataFrame，命令中使用 `starrocks.filter.query` 参数指定过滤条件为 `k=7` 和 `dt='2022-01-02 08:00:00'`，以做分区、分桶剪裁：

   ```Scala
   scala> val df = spark.read.format("starrocks")
          .option("starrocks.table.identifier", s"test.mytable")
          .option("starrocks.fenodes", s"<fe_host>:<fe_http_port>")
          .option("user", s"")
          .option("password", s"")
          .option("starrocks.filter.query", "k=7 and dt='2022-01-02 08:00:00'")
          .load()
   ```

2. 查看 StarRocks 的 FE 日志文件 **fe.log**，找到 Spark 读取数据所用的 SQL 语句，如下所示：

   ```SQL
   2022-08-09 19:06:34,939 INFO (nioEventLoopGroup-3-18|212) [TableQueryPlanAction.executeWithoutPassword():126] receive SQL statement [select `k`,`b`,`dt`,`v` from `test`.`mytable` where k=7 and dt='2022-01-02 08:00:00'] from external service [ user ['root'@'%']] for database [test] t
   able [mytable]
   ```

3. 在数据库 `test` 下，使用 EXPLAIN 来获取 SELECT `k`,`b`,`dt`,`v` from `test`.`mytable` where k=7 and dt='2022-01-02 08:00:00' 语句的执行计划，如下所示：

   ```Scala
   MySQL [test]> EXPLAIN select `k`,`b`,`dt`,`v` from `test`.`mytable` where k=7 and dt='2022-01-02 08:00:00';
   +----------------------------------------------------------+
   | Explain String                                           |
   +----------------------------------------------------------+
   | PLAN FRAGMENT 0                                          |
   |  OUTPUT EXPRS:1: k | 2: b | 3: dt | 4: v                 |
   |   PARTITION: RANDOM                                      |
   |                                                          |
   |   RESULT SINK                                            |
   |                                                          |
   |   0:OlapScanNode                                         |
   |      TABLE: mytable                                      |
   |      PREAGGREGATION: ON                                  |
   |      PREDICATES: 1: k = 7, 3: dt = '2022-01-02 08:00:00' |
   |      partitions=1/3                                      |
   |      rollup: mytable                                     |
   |      tabletRatio=1/3                                     |
   |      tabletList=41301                                    |
   |      cardinality=1                                       |
   |      avgRowSize=20.0                                     |
   |      numNodes=0                                          |
   +----------------------------------------------------------+
   17 rows in set (0.00 sec)
   ```

这里同时做了分区剪裁和分桶裁剪。因此，只会扫描 3 个分区中的 1 个分区 (`partitions=1/3`)、以及该分区下的 1 个 Tablet (`tabletRatio=1/3`)。

### 前缀索引过滤

1. 往 1 个分区插入更多数据，如下所示：

   ```Scala
   MySQL [test]> INSERT INTO mytable
   VALUES
       (1, 11, "2022-01-02 08:00:00", 111), 
       (3, 33, "2022-01-02 08:00:00", 333), 
       (3, 33, "2022-01-02 08:00:00", 333), 
       (3, 33, "2022-01-02 08:00:00", 333);
   ```

2. 查询 `mytable` 表的数据，如下所示：

   ```Scala
   MySQL [test]> SELECT * FROM mytable;
   +------+------+---------------------+------+
   | k    | b    | dt                  | v    |
   +------+------+---------------------+------+
   |    1 |   11 | 2022-01-02 08:00:00 |  111 |
   |    1 |   11 | 2022-01-02 08:00:00 |  111 |
   |    3 |   33 | 2022-01-02 08:00:00 |  333 |
   |    3 |   33 | 2022-01-02 08:00:00 |  333 |
   |    3 |   33 | 2022-01-02 08:00:00 |  333 |
   |    2 |   22 | 2022-02-02 08:00:00 |  222 |
   |    3 |   33 | 2022-03-02 08:00:00 |  333 |
   +------+------+---------------------+------+
   7 rows in set (0.01 sec)
   ```

3. 在 Spark 可执行程序目录下，通过如下命令，根据数据库 `test` 中的表 `mytable` 创建一个名为 `df` 的 DataFrame，命令中使用 `starrocks.filter.query` 参数指定过滤条件为 `k=1`，以做前缀索引过滤：

   ```Scala
   scala> val df = spark.read.format("starrocks")
          .option("starrocks.table.identifier", s"test.mytable")
          .option("starrocks.fenodes", s"<fe_host>:<fe_http_port>")
          .option("user", s"root")
          .option("password", s"")
          .option("starrocks.filter.query", "k=1")
          .load()
   ```

4. 在数据库 `test` 下，开启 Profile 上报:

   ```SQL
   MySQL [test]> SET enable_profile = true;
   Query OK, 0 rows affected (0.00 sec)
   ```

5. 使用浏览器打开 `http://<fe_host>:<http_http_port>/query` 页面，查看 SELECT * FROM mytable where k=1 语句的 Profile，如下所示：

   ```SQL
   OLAP_SCAN (plan_node_id=0):
     CommonMetrics:
        - CloseTime: 1.255ms
        - OperatorTotalTime: 1.404ms
        - PeakMemoryUsage: 0.00 
        - PullChunkNum: 8
        - PullRowNum: 2
          - __MAX_OF_PullRowNum: 2
          - __MIN_OF_PullRowNum: 0
        - PullTotalTime: 148.60us
        - PushChunkNum: 0
        - PushRowNum: 0
        - PushTotalTime: 0ns
        - SetFinishedTime: 136ns
        - SetFinishingTime: 129ns
     UniqueMetrics:
        - Predicates: 1: k = 1
        - Rollup: mytable
        - Table: mytable
        - BytesRead: 88.00 B
          - __MAX_OF_BytesRead: 88.00 B
          - __MIN_OF_BytesRead: 0.00 
        - CachedPagesNum: 0
        - CompressedBytesRead: 844.00 B
          - __MAX_OF_CompressedBytesRead: 844.00 B
          - __MIN_OF_CompressedBytesRead: 0.00 
        - CreateSegmentIter: 18.582us
        - IOTime: 4.425us
        - LateMaterialize: 17.385us
        - PushdownPredicates: 3
        - RawRowsRead: 2
          - __MAX_OF_RawRowsRead: 2
          - __MIN_OF_RawRowsRead: 0
        - ReadPagesNum: 12
          - __MAX_OF_ReadPagesNum: 12
          - __MIN_OF_ReadPagesNum: 0
        - RowsRead: 2
          - __MAX_OF_RowsRead: 2
          - __MIN_OF_RowsRead: 0
        - ScanTime: 154.367us
        - SegmentInit: 95.903us
          - BitmapIndexFilter: 0ns
          - BitmapIndexFilterRows: 0
          - BloomFilterFilterRows: 0
          - ShortKeyFilterRows: 3
            - __MAX_OF_ShortKeyFilterRows: 3
            - __MIN_OF_ShortKeyFilterRows: 0
          - ZoneMapIndexFilterRows: 0
        - SegmentRead: 2.559us
          - BlockFetch: 2.187us
          - BlockFetchCount: 2
            - __MAX_OF_BlockFetchCount: 2
            - __MIN_OF_BlockFetchCount: 0
          - BlockSeek: 7.789us
          - BlockSeekCount: 2
            - __MAX_OF_BlockSeekCount: 2
            - __MIN_OF_BlockSeekCount: 0
          - ChunkCopy: 25ns
          - DecompressT: 0ns
          - DelVecFilterRows: 0
          - IndexLoad: 0ns
          - PredFilter: 353ns
          - PredFilterRows: 0
          - RowsetsReadCount: 7
          - SegmentsReadCount: 3
            - __MAX_OF_SegmentsReadCount: 2
            - __MIN_OF_SegmentsReadCount: 0
          - TotalColumnsDataPageCount: 8
            - __MAX_OF_TotalColumnsDataPageCount: 8
            - __MIN_OF_TotalColumnsDataPageCount: 0
        - UncompressedBytesRead: 508.00 B
          - __MAX_OF_UncompressedBytesRead: 508.00 B
          - __MIN_OF_UncompressedBytesRead: 0.00 
   ```

`k = 1` 能够命中前缀索引，因此，读取数据的过程中过滤掉 3 行 (`ShortKeyFilterRows: 3`)。
