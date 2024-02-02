---
displayed_sidebar: "English"
---

# Read data from StarRocks using Spark connector

StarRocks provides a self-developed connector named StarRocks Connector for Apache Spark™ (Spark connector for short) to help you read data from a StarRocks table by using Spark. You can use Spark for complex processing and machine learning on the data you have read from StarRocks.

The Spark connector supports three reading methods: Spark SQL, Spark DataFrame, and Spark RDD.

You can use Spark SQL to create a temporary view on the StarRocks table, and then directly read data from the StarRocks table by using that temporary view.

You can also map the StarRocks table to a Spark DataFrame or a Spark RDD, and then read data from the Spark DataFrame or Spark RDD. We recommend the use of a Spark DataFrame.

> **NOTICE**
>
> Only users with the SELECT privilege on a StarRocks table can read data from this table. You can follow the instructions provided in [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) to grant the privilege to a user.

## Usage notes

- You can filter data on StarRocks before you read the data, thereby reducing the amount of data transferred.
- If the overhead of reading data is substantial, you can employ appropriate table design and filter conditions to prevent Spark from reading an excessive amount of data at a time. As such, you can reduce I/O pressure on your disk and network connection, thereby ensuring routine queries can be run properly.

## Version requirements

| Spark connector | Spark         | StarRocks       | Java | Scala |
|---------------- | ------------- | --------------- | ---- | ----- |
| 1.1.2           | 3.2, 3.3, 3.4, 3.5 | 2.5 and later   | 8    | 2.12  |
| 1.1.1           | 3.2, 3.3, 3.4 | 2.5 and later   | 8    | 2.12  |
| 1.1.0           | 3.2, 3.3, 3.4 | 2.5 and later   | 8    | 2.12  |
| 1.0.0           | 3.x           | 1.18 and later  | 8    | 2.12  |
| 1.0.0           | 2.x           | 1.18 and later  | 8    | 2.11  |

> **NOTICE**
>
> - Please see [Upgrade Spark connector](#upgrade-spark-connector) for behaviour changes among different connector versions.
> - The connector does not provide MySQL JDBC driver since version 1.1.1, and you need import the driver to the Spark classpath manually. You can find the driver on [Maven Central](https://repo1.maven.org/maven2/mysql/mysql-connector-java/).
> - In version 1.0.0, the Spark connector only supports reading data from StarRocks. From version 1.1.0 onwards, the Spark connector supports both reading data from and writing data to StarRocks.
> - Version 1.0.0 differs from version 1.1.0 in terms of parameters and data type mappings. See [Upgrade Spark connector](#upgrade-spark-connector).
> - In general cases, no new features will be added to version 1.0.0. We recommend that you upgrade your Spark connector at your earliest opportunity.

## Obtain Spark connector

Use one of the following methods to obtain the Spark connector **.jar** package that suits your business needs:

- Download a compiled package.
- Use Maven to add the dependencies required by the Spark connector. (This method is supported only for Spark connector 1.1.0 and later.)
- Manually compile a package.

### Spark connector 1.1.0 and later

Spark connector **.jar** packages are named in the following format:

`starrocks-spark-connector-${spark_version}_${scala_version}-${connector_version}.jar`

For example, if you want to use Spark connector 1.1.0 with Spark 3.2 and Scala 2.12, you can choose `starrocks-spark-connector-3.2_2.12-1.1.0.jar`.

> **NOTICE**
>
> In normal cases, the latest Spark connector version can be used with the most recent three Spark versions.

#### Download a compiled package

You can obtain Spark connector **.jar** packages of various versions at [Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks).

#### Add Maven dependencies

Configure the dependencies required by the Spark connector as follows:

> **NOTICE**
>
> You must replace `spark_version`, `scala_version`, and `connector_version` with the Spark version, Scala version, and Spark connector version you use.

```xml
<dependency>
  <groupId>com.starrocks</groupId>
  <artifactId>starrocks-spark-connector-${spark_version}_${scala_version}</artifactId>
  <version>${connector_version}</version>
</dependency>
```

For example, if you want to use Spark connector 1.1.0 with Spark 3.2 and Scala 2.12, configure the dependencies as follows:

```xml
<dependency>
  <groupId>com.starrocks</groupId>
  <artifactId>starrocks-spark-connector-3.2_2.12</artifactId>
  <version>1.1.0</version>
</dependency>
```

#### Manually compile a package

1. Download [the Spark connector code](https://github.com/StarRocks/starrocks-connector-for-apache-spark).

2. Use the following command to compile the Spark connector:

   > **NOTICE**
   >
   > You must replace `spark_version` with the Spark version you use.

   ```shell
   sh build.sh <spark_version>
   ```

   For example, if you want to use the Spark connector with Spark 3.2, compile the Spark connector as follows:

   ```shell
   sh build.sh 3.2
   ```

3. Go to the `target/` path, in which a Spark connector **.jar** package like `starrocks-spark-connector-3.2_2.12-1.1.0-SNAPSHOT.jar` is generated upon compilation.

   > **NOTICE**
   >
   > If you are using a Spark connector version that is not officially released, the name of the generated Spark connector **.jar** package contains `SNAPSHOT` as a suffix.

### Spark connector 1.0.0

#### Download a compiled package

- [Spark 2.x](https://cdn-thirdparty.starrocks.com/spark/starrocks-spark2_2.11-1.0.0.jar)
- [Spark 3.x](https://cdn-thirdparty.starrocks.com/spark/starrocks-spark3_2.12-1.0.0.jar)

#### Manually compile a package

1. Download [the Spark connector code](https://github.com/StarRocks/starrocks-connector-for-apache-spark/tree/spark-1.0).

   > **NOTICE**
   >
   > You must switch to `spark-1.0`.

2. Take one of the following actions to compile the Spark connector:

   - If you are using Spark 2.x, run the following command, which compiles the Spark connector to suit Spark 2.3.4 by default:

     ```Plain
     sh build.sh 2
     ```

   - If you are using Spark 3.x, run the following command, which compiles the Spark connector to suit Spark 3.1.2 by default:

     ```Plain
     sh build.sh 3
     ```

3. Go to the `output/` path, in which the `starrocks-spark2_2.11-1.0.0.jar` file is generated upon compilation. Then, copy the file to the classpath of Spark:

   - If your Spark cluster runs in `Local` mode, place the file into the `jars/` path.
   - If your Spark cluster runs in `Yarn` mode, place the file into the pre-deployment package.

You can use the Spark connector to read data from StarRocks only after you place the file into the specified location.

## Parameters

This section describes the parameters you need to configure when you use the Spark connector to read data from StarRocks.

### Common parameters

The following parameters apply to all three reading methods: Spark SQL, Spark DataFrame, and Spark RDD.

| Parameter                            | Default value     | Description                                                    |
| ------------------------------------ | ----------------- | ------------------------------------------------------------ |
| starrocks.fenodes                    | None              | The HTTP URL of the FE in your StarRocks cluster. Format `<fe_host>:<fe_http_port>`. You can specify multiple URLs, which must be separated by a comma (,). |
| starrocks.table.identifier           | None              | The name of the StarRocks table. Format: `<database_name>.<table_name>`. |
| starrocks.request.retries            | 3                 | The maximum number of times that Spark can retry to send a read request o StarRocks. |
| starrocks.request.connect.timeout.ms | 30000             | The maximum amount of time after which a read request sent to StarRocks times out. |
| starrocks.request.read.timeout.ms    | 30000             | The maximum amount of time after which the reading for a request sent to StarRocks times out. |
| starrocks.request.query.timeout.s    | 3600              | The maximum amount of time after which a query of data from StarRocks times out. The default timeout period is 1 hour. `-1` means that no timeout period is specified. |
| starrocks.request.tablet.size        | Integer.MAX_VALUE | The number of StarRocks tablets grouped into each Spark RDD partition. A smaller value of this parameter indicates that a larger number of Spark RDD partitions will be generated. A larger number of Spark RDD partitions means higher parallelism on Spark but greater pressure on StarRocks. |
| starrocks.batch.size                 | 4096              | The maximum number of rows that can be read from BEs at a time. Increasing the value of this parameter can reduce the number of connections established between Spark and StarRocks, thereby mitigating extra time overheads caused by network latency. |
| starrocks.exec.mem.limit             | 2147483648        | The maximum amount of memory allowed per query. Unit: bytes. The default memory limit is 2 GB. |
| starrocks.deserialize.arrow.async    | false             | Specifies whether to support asynchronously converting the Arrow memory format to RowBatches required for the iteration of the Spark connector. |
| starrocks.deserialize.queue.size     | 64                | The size of the internal queue that holds tasks for asynchronously converting the Arrow memory format to RowBatches. This parameter is valid when `starrocks.deserialize.arrow.async` is set to `true`. |
| starrocks.filter.query               | None              | The condition based on which you want to filter data on StarRocks. You can specify multiple filter conditions, which must be joined by `and`. StarRocks filters the data from the StarRocks table based on the specified filter conditions before the data is read by Spark. |
| starrocks.timezone | Default timezone of JVM | Supported since 1.1.1. The timezone used to convert StarRocks `DATETIME` to Spark `TimestampType`. The default is the timezone of JVM returned by `ZoneId#systemDefault()`. The format could be a timezone name such as `Asia/Shanghai`, or a zone offset such as `+08:00`. |

### Parameters for Spark SQL and Spark DataFrame

The following parameters apply only to the Spark SQL and Spark DataFrame reading methods.

| Parameter                           | Default value | Description                                                    |
| ----------------------------------- | ------------- | ------------------------------------------------------------ |
| starrocks.fe.http.url               | None          | The HTTP IP address of the FE. This parameter is supported from Spark connector 1.1.0 onwards. This parameter is equivalent to `starrocks.fenodes`. You only need to configure one of them. In Spark connector 1.1.0 and later, we recommend that you use `starrocks.fe.http.url` because `starrocks.fenodes` may be deprecated. |
| starrocks.fe.jdbc.url               | None          | The address that is used to connect to the MySQL server of the FE. Format: `jdbc:mysql://<fe_host>:<fe_query_port>`.<br />**NOTICE**<br />In Spark connector 1.1.0 and later, this parameter is mandatory.   |
| user                                | None          | The username of your StarRocks cluster account. The user needs the [SELECT privilege](../sql-reference/sql-statements/account-management/GRANT.md) on the StarRocks table.   |
| starrocks.user                      | None          | The username of your StarRocks cluster account. This parameter is supported from Spark connector 1.1.0 onwards. This parameter is equivalent to `user`. You only need to configure one of them. In Spark connector 1.1.0 and later, we recommend that you use `starrocks.user` because `user` may be deprecated.   |
| password                            | None          | The password of your StarRocks cluster account.    |
| starrocks.password                  | None          | The password of your StarRocks cluster account. This parameter is supported from Spark connector 1.1.0 onwards. This parameter is equivalent to `password`. You only need to configure one of them. In Spark connector 1.1.0 and later, we recommend that you use `starrocks.password` because `password` may be deprecated.   |
| starrocks.filter.query.in.max.count | 100           | The maximum number of values supported by the IN expression during predicate pushdown. If the number of values specified in the IN expression exceeds this limit, the filter conditions specified in the IN expression are processed on Spark.   |

### Parameters for Spark RDD

The following parameters apply only to the Spark RDD reading method.

| Parameter                       | Default value | Description                                                    |
| ------------------------------- | ------------- | ------------------------------------------------------------ |
| starrocks.request.auth.user     | None          | The username of your StarRocks cluster account.              |
| starrocks.request.auth.password | None          | The password of your StarRocks cluster account.              |
| starrocks.read.field            | None          | The StarRocks table column from which you want to read data. You can specify multiple columns, which must be separated by a comma (,). |

## Data type mapping between StarRocks and Spark

### Spark connector 1.1.0 and later

| StarRocks data type | Spark data type           |
|-------------------- |-------------------------- |
| BOOLEAN             | DataTypes.BooleanType     |
| TINYINT             | DataTypes.ByteType        |
| SMALLINT            | DataTypes.ShortType       |
| INT                 | DataTypes.IntegerType     |
| BIGINT              | DataTypes.LongType        |
| LARGEINT            | DataTypes.StringType      |
| FLOAT               | DataTypes.FloatType       |
| DOUBLE              | DataTypes.DoubleType      |
| DECIMAL             | DecimalType               |
| CHAR                | DataTypes.StringType      |
| VARCHAR             | DataTypes.StringType      |
| STRING              | DataTypes.StringType      |
| DATE                | DataTypes.DateType        |
| DATETIME            | DataTypes.TimestampType   |
| JSON | DataTypes.StringType <br> **NOTE:** <br> **Supported since version 1.1.2**, and require StarRocks at least 2.5.13, 3.0.3, 3.1 or later |
| ARRAY               | Unsupported datatype      |
| HLL                 | Unsupported datatype      |
| BITMAP              | Unsupported datatype      |

### Spark connector 1.0.0

| StarRocks data type  | Spark data type        |
| -------------------- | ---------------------- |
| BOOLEAN              | DataTypes.BooleanType  |
| TINYINT              | DataTypes.ByteType     |
| SMALLINT             | DataTypes.ShortType    |
| INT                  | DataTypes.IntegerType  |
| BIGINT               | DataTypes.LongType     |
| LARGEINT             | DataTypes.StringType   |
| FLOAT                | DataTypes.FloatType    |
| DOUBLE               | DataTypes.DoubleType   |
| DECIMAL              | DecimalType            |
| CHAR                 | DataTypes.StringType   |
| VARCHAR              | DataTypes.StringType   |
| DATE                 | DataTypes.StringType   |
| DATETIME             | DataTypes.StringType   |
| ARRAY                | Unsupported datatype   |
| HLL                  | Unsupported datatype   |
| BITMAP               | Unsupported datatype   |

The processing logic of the underlying storage engine used by StarRocks cannot cover an expected time range when DATE and DATETIME data types are directly used. Therefore, the Spark connector maps the DATE and DATETIME data types from StarRocks to the STRING data type from Spark, and generates readable string texts matching the date and time data read from StarRocks.

## Upgrade Spark connector

### Upgrade from version 1.0.0 to version 1.1.0

- Since 1.1.1, the Spark connector does not provide `mysql-connector-java` which is the official JDBC driver for MySQL, because of the limitations of the GPL license used by `mysql-connector-java`.
  However, the Spark connector still needs the `mysql-connector-java` to connect to StarRocks for the table metadata, so you need to add the driver to the Spark classpath manually. You can find the
  driver on [MySQL site](https://dev.mysql.com/downloads/connector/j/) or [Maven Central](https://repo1.maven.org/maven2/mysql/mysql-connector-java/).
  
- In version 1.1.0, the Spark connector uses JDBC to access StarRocks to obtain more detailed table information. Therefore, you must configure `starrocks.fe.jdbc.url`.

- In version 1.1.0, some parameters are renamed. Both the old and new parameters are retained for now. For each pair of equivalent parameters, you only need to configure one of them, but we recommend that you use the new one because the old one may be deprecated.
  - `starrocks.fenodes` is renamed as `starrocks.fe.http.url`.
  - `user` is renamed as `starrocks.user`.
  - `password` is renamed as `starrocks.password`.

- In version 1.1.0, the mappings of some data types are adjusted based on Spark 3.x:
  - `DATE` in StarRocks is mapped to `DataTypes.DateType` (originally `DataTypes.StringType`) in Spark.
  - `DATETIME` in StarRocks is mapped to `DataTypes.TimestampType` (originally `DataTypes.StringType`) in Spark.

## Examples

The following examples assume you have created a database named `test` in your StarRocks cluster and you have the permissions of user `root`. The parameter settings in the examples are based on Spark Connector 1.1.0.

### Data example

Do as follows to prepare a sample table:

1. Go to the `test` database and create a table named `score_board`.

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
   DISTRIBUTED BY HASH(`id`)
   PROPERTIES (
       "replication_num" = "3"
   );
   ```

2. Insert data into the `score_board` table.

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

3. Query the `score_board` table.

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

### Read data using Spark SQL

1. Run the following command in the Spark directory to start Spark SQL:

   ```Plain
   sh spark-sql
   ```

2. Run the following command to create a temporary view named `spark_starrocks` on the `score_board` table which belongs to the `test` database:

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

3. Run the following command to read data from the temporary view:

   ```SQL
   spark-sql> SELECT * FROM spark_starrocks;
   ```

   Spark returns the following data:

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

### Read data using Spark DataFrame

1. Run the following command in the Spark directory to start Spark Shell:

   ```Plain
   sh spark-shell
   ```

2. Run the following command to create a DataFrame named `starrocksSparkDF` on the `score_board` table which belongs to the `test` database:

   ```Scala
   scala> val starrocksSparkDF = spark.read.format("starrocks")
              .option("starrocks.table.identifier", s"test.score_board")
              .option("starrocks.fe.http.url", s"<fe_host>:<fe_http_port>")
              .option("starrocks.fe.jdbc.url", s"jdbc:mysql://<fe_host>:<fe_query_port>")
              .option("starrocks.user", s"root")
              .option("starrocks.password", s"")
              .load()
   ```

3. Read data from the DataFrame. For example, if you want to read the first 10 rows, run the following command:

   ```Scala
   scala> starrocksSparkDF.show(10)
   ```

   Spark returns the following data:

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

   > **NOTE**
   >
   > By default, if you do not specify the number of rows you want to read, Spark returns the first 20 rows.

### Read data using Spark RDD

1. Run the following command in the Spark directory to start Spark Shell:

   ```Plain
   sh spark-shell
   ```

2. Run the following command to create an RDD named `starrocksSparkRDD` on the `score_board` table which belongs to the `test` database.

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

3. Read data from the RDD. For example, if you want to read the first 10 elements, run the following command:

   ```Scala
   scala> starrocksSparkRDD.take(10)
   ```

   Spark returns the following data:

   ```Scala
   res0: Array[AnyRef] = Array([1, Bob, 21], [2, Stan, 21], [3, Sam, 22], [4, Tony, 22], [5, Alice, 22], [6, Lucy, 23], [7, Polly, 23], [8, Tom, 23], [9, Rose, 24], [10, Jerry, 24])
   ```

   To read the entire RDD, run the following command:

   ```Scala
   scala> starrocksSparkRDD.collect()
   ```

   Spark returns the following data:

   ```Scala
   res1: Array[AnyRef] = Array([1, Bob, 21], [2, Stan, 21], [3, Sam, 22], [4, Tony, 22], [5, Alice, 22], [6, Lucy, 23], [7, Polly, 23], [8, Tom, 23], [9, Rose, 24], [10, Jerry, 24], [11, Jason, 24], [12, Lily, 25], [13, Stephen, 25], [14, David, 25], [15, Eddie, 26], [16, Kate, 27], [17, Cathy, 27], [18, Judy, 27], [19, Julia, 28], [20, Robert, 28], [21, Jack, 29])
   ```

## Best practices

When you read data from StarRocks using the Spark connector, you can use the `starrocks.filter.query` parameter to specify filter conditions based on which Spark prunes partitions, buckets, and prefix indexes to reduce the cost of data pulling. This section uses Spark DataFrame as an example to show how this is achieved.

### Environment setup

| Component       | Version                                                      |
| --------------- | ------------------------------------------------------------ |
| Spark           | Spark 2.4.4 and Scala 2.11.12 (OpenJDK 64-Bit Server VM, Java 1.8.0_302) |
| StarRocks       | 2.2.0                                                        |
| Spark connector | starrocks-spark2_2.11-1.0.0.jar                              |

### Data example

Do as follows to prepare a sample table:

1. Go to the `test` database and create a table named  `mytable`.

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
   DISTRIBUTED BY HASH(`k`)
   PROPERTIES (
       "replication_num" = "3"
   );
   ```

2. Insert data into `mytable`.

   ```SQL
   MySQL [test]> INSERT INTO mytable
   VALUES
        (1, 11, '2022-01-02 08:00:00', 111),
        (2, 22, '2022-02-02 08:00:00', 222),
        (3, 33, '2022-03-02 08:00:00', 333);
   ```

3. Query the `mytable` table.

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

### Full table scan

1. Run the following command in the Spark directory to create a DataFrame named `df` on `mytable` table which belongs to the `test` database:

   ```Scala
   scala>  val df = spark.read.format("starrocks")
           .option("starrocks.table.identifier", s"test.mytable")
           .option("starrocks.fenodes", s"<fe_host>:<fe_http_port>")
           .option("user", s"root")
           .option("password", s"")
           .load()
   ```

2. View the FE log file **fe.log** of your StarRocks cluster, and find the SQL statement executed to read data. Example:

   ```SQL
   2022-08-09 18:57:38,091 INFO (nioEventLoopGroup-3-10|196) [TableQueryPlanAction.executeWithoutPassword():126] receive SQL statement [select `k`,`b`,`dt`,`v` from `test`.`mytable`] from external service [ user ['root'@'%']] for database [test] table [mytable]
   ```

3. In the `test` database, use EXPLAIN to obtain the execution plan of the SELECT `k`,`b`,`dt`,`v` from `test`.`mytable` statement:

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

In this example, no pruning is performed. Therefore, Spark scans all of the three partitions (as suggested by `partitions=3/3`) that hold data, and scans all of the 9 tablets (as suggested by `tabletRatio=9/9`) in those three partitions.

### Partition pruning

1. Run the following command, in which you use the `starrocks.filter.query` parameter to specify a filter condition `dt='2022-01-02 08:00:00` for partition pruning, in the Spark directory to create a DataFrame named `df` on the `mytable` table which belongs to the `test` database:

   ```Scala
   scala> val df = spark.read.format("starrocks")
          .option("starrocks.table.identifier", s"test.mytable")
          .option("starrocks.fenodes", s"<fe_host>:<fe_http_port>")
          .option("user", s"root")
          .option("password", s"")
          .option("starrocks.filter.query", "dt='2022-01-02 08:00:00'")
          .load()
   ```

2. View the FE log file **fe.log** of your StarRocks cluster, and find the SQL statement executed to read data. Example:

   ```SQL
   2022-08-09 19:02:31,253 INFO (nioEventLoopGroup-3-14|204) [TableQueryPlanAction.executeWithoutPassword():126] receive SQL statement [select `k`,`b`,`dt`,`v` from `test`.`mytable` where dt='2022-01-02 08:00:00'] from external service [ user ['root'@'%']] for database [test] table [mytable]
   ```

3. In the `test` database, use EXPLAIN to obtain the execution plan of the SELECT `k`,`b`,`dt`,`v` from `test`.`mytable` where dt='2022-01-02 08:00:00' statement:

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

In this example, only partition pruning is performed, whereas bucket pruning is not. Therefore, Spark scans one of the three partitions (as suggested by `partitions=1/3`) and all of the tablets (as suggested by `tabletRatio=3/3`) in that partition.

### Bucket pruning

1. Run the following command, in which you use the `starrocks.filter.query` parameter to specify a filter condition `k=1` for bucket pruning, in the Spark directory to create a DataFrame named `df` on the `mytable` table which belongs to the `test` database:

   ```Scala
   scala> val df = spark.read.format("starrocks")
          .option("starrocks.table.identifier", s"test.mytable")
          .option("starrocks.fenodes", s"<fe_host>:<fe_http_port>")
          .option("user", s"root")
          .option("password", s"")
          .option("starrocks.filter.query", "k=1")
          .load()
   ```

2. View the FE log file  **fe.log** of your StarRocks cluster, and find the SQL statement executed to read data. Example:

   ```SQL
   2022-08-09 19:04:44,479 INFO (nioEventLoopGroup-3-16|208) [TableQueryPlanAction.executeWithoutPassword():126] receive SQL statement [select `k`,`b`,`dt`,`v` from `test`.`mytable` where k=1] from external service [ user ['root'@'%']] for database [test] table [mytable]
   ```

3. In the `test` database, use EXPLAIN to obtain the execution plan of the SELECT `k`,`b`,`dt`,`v` from `test`.`mytable` where k=1 statement:

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

In this example, only bucket pruning is performed, whereas partition pruning is not. Therefore, Spark scans all of the three partitions (as suggested by `partitions=3/3`) that hold data, and scans all of the three tablets (as suggested by `tabletRatio=3/9`) to retrieve Hash values that meet the `k = 1` filter condition within those three partitions.

### Partition pruning and bucket pruning

1. Run the following command, in which you use the `starrocks.filter.query` parameter to specify two filter conditions `k=7` and `dt='2022-01-02 08:00:00'` for bucket pruning and partition pruning, in the Spark directory to create a DataFrame named `df` on the `mytable` table on the `test` database:

   ```Scala
   scala> val df = spark.read.format("starrocks")
          .option("starrocks.table.identifier", s"test.mytable")
          .option("starrocks.fenodes", s"<fe_host>:<fe_http_port>")
          .option("user", s"")
          .option("password", s"")
          .option("starrocks.filter.query", "k=7 and dt='2022-01-02 08:00:00'")
          .load()
   ```

2. View the FE log file **fe.log** of your StarRocks cluster, and find the SQL statement executed to read data. Example:

   ```SQL
   2022-08-09 19:06:34,939 INFO (nioEventLoopGroup-3-18|212) [TableQueryPlanAction.executeWithoutPassword():126] receive SQL statement [select `k`,`b`,`dt`,`v` from `test`.`mytable` where k=7 and dt='2022-01-02 08:00:00'] from external service [ user ['root'@'%']] for database [test] t
   able [mytable]
   ```

3. In the `test` database, use EXPLAIN to obtain the execution plan of the SELECT `k`,`b`,`dt`,`v` from `test`.`mytable` where k=7 and dt='2022-01-02 08:00:00' statement:

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

In this example, both partition pruning and bucket pruning are performed. Therefore, Spark scans only one of the three partitions (as suggested by `partitions=1/3`) and only one tablet (as suggested by `tabletRatio=1/3`) in that partition.

### Prefix index filtering

1. Insert more data records into a partition of the `mytable` table which belongs to the `test` database:

   ```Scala
   MySQL [test]> INSERT INTO mytable
   VALUES
       (1, 11, "2022-01-02 08:00:00", 111), 
       (3, 33, "2022-01-02 08:00:00", 333), 
       (3, 33, "2022-01-02 08:00:00", 333), 
       (3, 33, "2022-01-02 08:00:00", 333);
   ```

2. Query the `mytable` table:

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

3. Run the following command, in which you use the `starrocks.filter.query` parameter to specify a filter condition `k=1` for prefix index filtering, in the Spark directory to create a DataFrame named `df` on the `mytable` table which belongs to the `test` database:

   ```Scala
   scala> val df = spark.read.format("starrocks")
          .option("starrocks.table.identifier", s"test.mytable")
          .option("starrocks.fenodes", s"<fe_host>:<fe_http_port>")
          .option("user", s"root")
          .option("password", s"")
          .option("starrocks.filter.query", "k=1")
          .load()
   ```

4. In the `test` database, set `is_report_success` to `true` to enable profile reporting:

   ```SQL
   MySQL [test]> SET is_report_success = true;
   Query OK, 0 rows affected (0.00 sec)
   ```

5. Use a browser to open the `http://<fe_host>:<http_http_port>/query` page, and view the profile of the SELECT * FROM mytable where k=1 statement. Example:

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

In this example, the filter condition `k = 1` can hit the prefix index. Therefore, Spark can filter out three rows (as suggested by `ShortKeyFilterRows: 3`).
