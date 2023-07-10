# Load data using Spark connector (recommended)

StarRocks provides a self-developed connector named StarRocks Connector for Apache Spark™ (Spark connector for short) to help you load data into a StarRocks table by using Spark. The basic principle is to accumulate the data and then load it all at a time into StarRocks through [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md). The Spark connector is implemented based on Spark DataSource V2. A DataSource can be created by using Spark DataFrames or Spark SQL. And both batch and structured streaming modes are supported.

## Version requirements

| Spark connector | Spark            | StarRocks     | Java | Scala |
| --------------- | ---------------- | ------------- | ---- | ----- |
| 1.1.0           | 3.2, 3.3, or 3.4 | 2.5 and later | 8    | 2.12  |

## Obtain Spark connector

You can obtain the Spark connector JAR file in the following ways:

- Directly download the compiled Spark Connector JAR file.
- Add the Spark connector as a dependency in your Maven project and then download the JAR file.
- Compile the source code of the Spark Connector into a JAR file by yourself.

The naming format of the Spark connector JAR file is `starrocks-spark-connector-${spark_version}_${scala_version}-${connector_version}.jar`.

For example, if you install Spark 3.2 and Scala 2.12 in your environment and you want to use Spark connector 1.1.0, you can use `starrocks-spark-connector-3.2_2.12-1.1.0.jar`.

> **NOTICE**
>
> In general, the latest version of the Spark connector only maintains compatibility with the three most recent versions of Spark.

### Download the compiled Jar file

Directly download the corresponding version of the Spark connector JAR from the [Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks).

### Maven Dependency

1. In your Maven project's `pom.xml` file, add the Spark connector as a dependency according to the following format. Replace `spark_version`, `scala_version`, and `connector_version` with the respective versions.

    ```xml
    <dependency>
    <groupId>com.starrocks</groupId>
    <artifactId>starrocks-spark-connector-${spark_version}_${scala_version}</artifactId>
    <version>${connector_version}</version>
    </dependency>
    ```

2. For example, if the version of Spark in your environment is 3.2, the version of Scala is 2.12, and you choose Spark connector 1.1.0, you need to add the following dependency:

    ```xml
    <dependency>
    <groupId>com.starrocks</groupId>
    <artifactId>starrocks-spark-connector-3.2_2.12</artifactId>
    <version>1.1.0</version>
    </dependency>
    ```

### Compile by yourself

1. Download the [Spark connector package](https://github.com/StarRocks/starrocks-connector-for-apache-spark).
2. Execute the following command to compile the source code of Spark connector into a JAR file. Note that  `spark_version` is replaced with the corresponding Spark version.

      ```bash
      sh build.sh <spark_version>
      ```

   For example, if the Spark version in your environment is 3.2, you need to execute the following command:

      ```bash
      sh build.sh 3.2
      ```

3. Go to the `target/`  directory to find the Spark connector JAR file, such as `starrocks-spark-connector-3.2_2.12-1.1.0-SNAPSHOT.jar` , generated upon compilation.

> **NOTE**
>
> The name of Spark connector which is not formally released contains the `SNAPSHOT` suffix.

## Parameters

| Parameter                                      | Required | Default value | Description                                                  |
| ---------------------------------------------- | -------- | ------------- | ------------------------------------------------------------ |
| starrocks.fe.http.url                          | YES      | None          | The HTTP URL of the FE in your StarRocks cluster. You can specify multiple URLs, which must be separated by a comma (,). Format: `<fe_host1>:<fe_http_port1>,<fe_host2>:<fe_http_port2>`. |
| starrocks.fe.jdbc.url                          | YES      | None          | The address that is used to connect to the MySQL server of the FE. Format: `jdbc:mysql://<fe_host>:<fe_query_port>`. |
| starrocks.table.identifier                     | YES      | None          | The name of the StarRocks table. Format: `<database_name>.<table_name>`. |
| starrocks.user                                 | YES      | None          | The username of your StarRocks cluster account.              |
| starrocks.password                             | YES      | None          | The password of your StarRocks cluster account.              |
| starrocks.write.label.prefix                   | NO       | spark-        | The label prefix used by Stream Load.                        |
| starrocks.write.enable.transaction-stream-load | NO       | TRUE          | Whether to use [the transactional interface of Stream Load](../Stream_Load_transaction_interface.md) to load data. This feature is supported in StarRocks v2.4 and later. |
| starrocks.write.buffer.size                    | NO       | 104857600     | The maximum size of data that can be accumulated in memory before being sent to StarRocks at a time. Setting this parameter to a larger value can improve loading performance but may increase loading latency. |
| starrocks.write.flush.interval.ms              | NO       | 300000        | The interval at which data is sent to StarRocks. This parameter is used to control the loading latency. |
| starrocks.columns                              | NO       | None          | The StarRocks table column into which you want to load data. You can specify multiple columns, which must be separated by commas (,), for example, `"c0,c1,c2"`. |
| starrocks.write.properties.*                   | NO       | None          | The parameters that are used to control Stream Load behavior.  For example, the parameter `starrocks.write.properties.format` specifies the format of the data to be loaded, such as CSV or JSON. For a list of supported parameters and their descriptions, see [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md). |
| starrocks.write.properties.format              | NO       | CSV           | The file format based on which the Spark connector transforms each batch of data before the data is sent to StarRocks. Valid values: CSV and JSON. |
| starrocks.write.properties.row_delimiter       | NO       | \n            | The row delimiter for CSV-formatted data.                    |
| starrocks.write.properties.column_separator    | NO       | \t            | The column separator for CSV-formatted data.                 |
| starrocks.write.num.partitions                 | NO       | None          | The number of partitions into which Spark can write data in parallel. When the data volume is small, you can reduce the number of partitions to lower the loading concurrency and frequency. The default value for this parameter is determined by Spark. However, this method may cause Spark Shuffle cost. |
| starrocks.write.partition.columns              | NO       | None          | The partitioning columns in Spark. The parameter takes effect only when `starrocks.write.num.partitions` is specified. If this parameter is not specified, all columns being written are used for partitioning. |

## Data type mapping between Spark and StarRocks

| Spark data type | StarRocks data type |
| --------------- | ------------------- |
| BooleanType     | BOOLEAN             |
| ByteType        | TINYINT             |
| ShortType       | SMALLINT            |
| IntegerType     | INT                 |
| LongType        | BIGINT              |
| StringType      | LARGEINT            |
| FloatType       | FLOAT               |
| DoubleType      | DOUBLE              |
| DecimalType     | DECIMAL             |
| StringType      | CHAR                |
| StringType      | VARCHAR             |
| StringType      | STRING              |
| DateType        | DATE                |
| TimestampType   | DATETIME            |

## Examples

The following examples show how to use the Spark connector to load data into a StarRocks table with Spark DataFrames or Spark SQL. The Spark DataFrames supports both Batch and Structured Streaming modes.

For more examples, see [Spark Connector Examples](https://github.com/StarRocks/starrocks-connector-for-apache-spark/tree/main/src/test/java/com/starrocks/connector/spark/examples).

### Preparations

#### Create a StarRocks table

Create a database `test` and create a Primary Key table `score_board`.

```sql
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

#### Set up your Spark environment

Note that the following examples are run in Spark 3.2.4 and use `spark-shell` and `spark-sql`.  Before running the examples, make sure to place the Spark connector JAR file in the `$SPARK_HOME/jars` directory.

### Load data with Spark DataFrames

The following two examples explain how to load data with Spark DataFrames Batch or Structured Streaming mode.

#### Batch

Construct data in memory and load data into the StarRocks table.

1. Write the following scala code snippet in `spark-shell`:

    ```Scala
    // 1. Create a DataFrame from a sequence.
    val data = Seq((1, "starrocks", 100), (2, "spark", 100))
    val df = data.toDF("id", "name", "score")

    // 2. Write to starrocks with the format "starrocks",
    // and replace the options with your own.
    df.write.format("starrocks")
        .option("starrocks.fe.http.url", "127.0.0.1:8038")
        .option("starrocks.fe.jdbc.url", "jdbc:mysql://127.0.0.1:9038")
        .option("starrocks.table.identifier", "test.score_board")
        .option("starrocks.user", "root")
        .option("starrocks.password", "")
        .mode("append")
        .save()
    ```

2. Query data in the StarRocks table.

    ```sql
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

Construct a streaming read of data from a CSV file and load data into the StarRocks table.

1. In the directory `csv-data`, create a CSV file `test.csv` with the following data:

    ```csv
    3,starrocks,100
    4,spark,100
    ```

2. Write the following scala code snippet in the `spark-shell`:

    ```Scala
    import org.apache.spark.sql.types.StructType

    // 1. Create a DataFrame from CSV.
    val schema = (new StructType()
            .add("id", "integer")
            .add("name", "string")
            .add("score", "integer")
        )
    val df = (spark.readStream
            .option("sep", ",")
            .schema(schema)
            .format("csv") 
            // Replace it with your path to the directory "csv-data".
            .load("/path/to/csv-data")
        )

    // 2. Write to starrocks with the format "starrocks", and replace the options with your own.
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

3. Query data in the StarRocks table.

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

### Load data with Spark SQL

The following example explains how to load data with Spark SQL by using the `INSERT INTO` statement in the [Spark SQL CLI](https://spark.apache.org/docs/latest/sql-distributed-sql-engine-spark-sql-cli.html).

1. Execute the following SQL statement in the `spark-sql`:

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

2. Query data in the StarRocks table.

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
