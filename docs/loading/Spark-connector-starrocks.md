# Load data using Spark connector (recommended)

StarRocks provides a self-developed connector named StarRocks Connector for Apache Spark™ (Spark connector for short) to help you load data into a StarRocks table by using Spark. The basic principle is to accumulate the data and then load it all at a time into StarRocks through [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md). The Spark connector is implemented based on Spark DataSource V2. A DataSource can be created by using Spark DataFrames or Spark SQL. And both batch and structured streaming modes are supported.

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

3. Go to the `target/` directory to find the Spark connector JAR file, such as `starrocks-spark-connector-3.2_2.12-1.1.0-SNAPSHOT.jar` , generated upon compilation.

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
| starrocks.write.enable.transaction-stream-load | NO       | TRUE          | Whether to use [the transactional interface of Stream Load](./Stream_Load_transaction_interface.md) to load data. This feature is supported in StarRocks v2.4 and later. |
| starrocks.write.buffer.size                    | NO       | 104857600     | The maximum size of data that can be accumulated in memory before being sent to StarRocks at a time. Setting this parameter to a larger value can improve loading performance but may increase loading latency. |
| starrocks.write.flush.interval.ms              | NO       | 300000        | The interval at which data is sent to StarRocks. This parameter is used to control the loading latency. |
| starrocks.columns                              | NO       | None          | The StarRocks table column into which you want to load data. You can specify multiple columns, which must be separated by commas (,), for example, `"c0,c1,c2"`. |
| starrocks.write.properties.*                   | NO       | None          | The parameters that are used to control Stream Load behavior.  For example, the parameter `starrocks.write.properties.format` specifies the format of the data to be loaded, such as CSV or JSON. For a list of supported parameters and their descriptions, see [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md). |
| starrocks.write.properties.format              | NO       | CSV           | The file format based on which the Spark connector transforms each batch of data before the data is sent to StarRocks. Valid values: CSV and JSON. |
| starrocks.write.properties.row_delimiter       | NO       | \n            | The row delimiter for CSV-formatted data.                    |
| starrocks.write.properties.column_separator    | NO       | \t            | The column separator for CSV-formatted data.                 |
| starrocks.write.num.partitions                 | NO       | None          | The number of partitions into which Spark can write data in parallel. When the data volume is small, you can reduce the number of partitions to lower the loading concurrency and frequency. The default value for this parameter is determined by Spark. However, this method may cause Spark Shuffle cost. |
| starrocks.write.partition.columns              | NO       | None          | The partitioning columns in Spark. The parameter takes effect only when `starrocks.write.num.partitions` is specified. If this parameter is not specified, all columns being written are used for partitioning. |

## Data type mapping between Spark and StarRocks

- The default data type mapping is as follows:

  | Spark data type | StarRocks data type                                          |
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
  | ArrayType       | ARRAY <br /> **NOTE:** <br /> **Supported since version 1.1.1**. For detailed steps, see [Load data into columns of ARRAY type](#load-data-into-columns-of-array-type). |

- You can also customize the data type mapping.

  For example, a StarRocks table contains BITMAP and HLL columns, but Spark does not support the two data types. You need to customize the corresponding data types in Spark. For detailed steps, see load data into [BITMAP](#load-data-into-columns-of-bitmap-type) and [HLL](#load-data-into-columns-of-hll-type) columns. **BITMAP and HLL are supported since version 1.1.1**.

## Upgrade Spark connector

### Upgrade from version 1.1.0 to 1.1.1

- Since 1.1.1, the Spark connector does not provide `mysql-connector-java` which is the official JDBC driver for MySQL, because of the limitations of the GPL license used by `mysql-connector-java`.
  However, the Spark connector still needs the MySQL JDBC driver to connect to StarRocks for the table metadata, so you need to add the driver to the Spark classpath manually. You can find the
  driver on [MySQL site](https://dev.mysql.com/downloads/connector/j/) or [Maven Central](https://repo1.maven.org/maven2/mysql/mysql-connector-java/).
- Since 1.1.1, the connector uses Stream Load interface by default rather than Stream Load transaction interface in version 1.1.0. If you still want to use Stream Load transaction interface, you
  can set the option `starrocks.write.max.retries` to `0`. Please see the description of `starrocks.write.enable.transaction-stream-load` and `starrocks.write.max.retries`
  for details.

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

Note that the following examples are run in Spark 3.2.4 and use `spark-shell`, `pyspark` and `spark-sql`.  Before running the examples, make sure to place the Spark connector JAR file in the `$SPARK_HOME/jars` directory.

### Load data with Spark DataFrames

The following two examples explain how to load data with Spark DataFrames Batch or Structured Streaming mode.

#### Batch

Construct data in memory and load data into the StarRocks table.

1. You can write the spark application using Scala or Python.

   For Scala, run the following code snippet in `spark-shell`:

    ```Scala
    // 1. Create a DataFrame from a sequence.
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

    For Python, run the following code snippet in `pyspark`:

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

2. You can write the Spark application using Scala or Python.

   For Scala, run the following code snippet in `spark-shell`:

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

    // 2. Write to StarRocks by configuring the format as "starrocks" and the following options. 
    // You need to modify the options according your own environment.
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

   For Python, run the following code snippet in `pyspark`:

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
    -- 1. Create a table by configuring the data source as  `starrocks` and the following options. 
    -- You need to modify the options according your own environment.
    CREATE TABLE `score_board`
    USING starrocks
    OPTIONS(
    "starrocks.fe.http.url"="127.0.0.1:8030",
    "starrocks.fe.jdbc.url"="jdbc:mysql://127.0.0.1:9030",
    "starrocks.table.identifier"="test.score_board",
    "starrocks.user"="root",
    "starrocks.password"=""
    );

    -- 2. Insert two rows into the table.
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

## Best Practices

### Load data to Primary Key table

This section will show how to load data to StarRocks Primary Key table to achieve partial updates, and conditional updates.
You can see [Change data through loading](../loading/Load_to_Primary_Key_tables.md) for the detailed introduction of these features.
These examples use Spark SQL.

#### Preparations

Create a database `test` and create a Primary Key table `score_board` in StarRocks.

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

#### Partial updates

This example will show how to only update data in the column `name` through loading:

1. Insert initial data to StarRocks table in MySQL client.

   ```sql
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

2. Create a Spark table `score_board` in Spark SQL client.

   - Set the option `starrocks.write.properties.partial_update` to `true` which tells the connector to do partial update.
   - Set the option `starrocks.columns` to `"id,name"` to tell the connector which columns to write.

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

3. Insert data into the table in Spark SQL client, and only update the column `name`.

   ```SQL
   INSERT INTO `score_board` VALUES (1, 'starrocks-update'), (2, 'spark-update');
   ```

4. Query the StarRocks table in MySQL client.

   You can see that only values for `name` change, and the values for `score` does not change.

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

#### Conditional updates

This example will show how to do conditional updates according to the values of column `score`. The update for an `id`
takes effect only when the new value for `score` is has a greater or equal to the old value.

1. Insert initial data to StarRocks table in MySQL client.

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

2. Create a Spark table `score_board` in the following ways.

   - Set the option `starrocks.write.properties.merge_condition` to `score` which tells the connector to use the column `score` as the condition.
   - Make sure that the Spark connector use Stream Load interface to load data, rather than Stream Load transaction interface, because the latter does not support this feature.

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

3. Insert data to the table in Spark SQL client, and update the row whose `id` is 1 with a smaller score value, and the row whose `id` is 2 with a larger score value.

   ```SQL
   INSERT INTO `score_board` VALUES (1, 'starrocks-update', 99), (2, 'spark-update', 101);
   ```

4. Query the StarRocks table in MySQL client.

   You can see that only the row whose `id` is 2 changes, and the row whose `id` is 1 does not change.

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

### Load data into columns of BITMAP type

[`BITMAP`](../sql-reference/sql-statements/data-types/BITMAP.md) is often used to accelerate count distinct, such as counting UV, see [Use Bitmap for exact Count Distinct](../using_starrocks/Using_bitmap.md).
Here we take the counting of UV as an example to show how to load data into columns of the `BITMAP` type. **`BITMAP` is supported since version 1.1.1**.

1. Create a StarRocks Aggregate table.

   In the database `test`, create an Aggregate table `page_uv` where the column `visit_users` is defined as the `BITMAP` type and configured with the aggregate function `BITMAP_UNION`.

    ```SQL
    CREATE TABLE `test`.`page_uv` (
      `page_id` INT NOT NULL COMMENT 'page ID',
      `visit_date` datetime NOT NULL COMMENT 'access time',
      `visit_users` BITMAP BITMAP_UNION NOT NULL COMMENT 'user ID'
    ) ENGINE=OLAP
    AGGREGATE KEY(`page_id`, `visit_date`)
    DISTRIBUTED BY HASH(`page_id`);
    ```

2. Create a Spark table.

    The schema of the Spark table is inferred from the StarRocks table, and the Spark does not support the `BITMAP` type. So you need to customize the corresponding column data type in Spark, for example as `BIGINT`, by configuring the option `"starrocks.column.types"="visit_users BIGINT"`. When using Stream Load to ingest data, the connector uses the [`to_bitmap`](../sql-reference/sql-functions/bitmap-functions/to_bitmap.md) function to convert the data of `BIGINT` type into `BITMAP` type.

    Run the following DDL in `spark-sql`:

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

3. Load data into StarRocks table.

    Run the following DML in `spark-sql`:

    ```SQL
    INSERT INTO `page_uv` VALUES
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 13),
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 23),
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 33),
       (1, CAST('2020-06-23 02:30:30' AS TIMESTAMP), 13),
       (2, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 23);
    ```

4. Calculate page UVs from the StarRocks table.

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

> **NOTICE:**
>
> The connector uses [`to_bitmap`](../sql-reference/sql-functions/bitmap-functions/to_bitmap.md)
> function to convert data of the `TINYINT`, `SMALLINT`, `INTEGER`, and `BIGINT` types in Spark to the `BITMAP` type in StarRocks, and uses
> [`bitmap_hash`](../sql-reference/sql-functions/bitmap-functions/bitmap_hash.md) function for other Spark data types.

### Load data into columns of HLL type

[`HLL`](../sql-reference/sql-statements/data-types/HLL.md) can be used for approximate count distinct, see [Use HLL for approximate count distinct](../using_starrocks/Using_HLL.md).

Here we take the counting of UV as an example to show how to load data into columns of the `HLL` type.  **`HLL` is supported since version 1.1.1**.

1. Create a StarRocks Aggregate table.

   In the database `test`, create an Aggregate table `hll_uv` where the column `visit_users` is defined as the `HLL` type and configured with the aggregate function `HLL_UNION`.

    ```SQL
    CREATE TABLE `hll_uv` (
    `page_id` INT NOT NULL COMMENT 'page ID',
    `visit_date` datetime NOT NULL COMMENT 'access time',
    `visit_users` HLL HLL_UNION NOT NULL COMMENT 'user ID'
    ) ENGINE=OLAP
    AGGREGATE KEY(`page_id`, `visit_date`)
    DISTRIBUTED BY HASH(`page_id`);
    ```

2. Create a Spark table.

   The schema of the Spark table is inferred from the StarRocks table, and the Spark does not support the `HLL` type. So you need to customize the corresponding column data type in Spark, for example as `BIGINT`, by configuring the option `"starrocks.column.types"="visit_users BIGINT"`. When using Stream Load to ingest data, the connector uses the [`hll_hash`](../sql-reference/sql-functions/aggregate-functions/hll_hash.md) function to convert the data of `BIGINT` type into `HLL` type.

    Run the following DDL in `spark-sql`:

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

3. Load data into StarRocks table.

    Run the following DML in `spark-sql`:

    ```SQL
    INSERT INTO `hll_uv` VALUES
       (3, CAST('2023-07-24 12:00:00' AS TIMESTAMP), 78),
       (4, CAST('2023-07-24 13:20:10' AS TIMESTAMP), 2),
       (3, CAST('2023-07-24 12:30:00' AS TIMESTAMP), 674);
    ```

4. Calculate page UVs from the StarRocks table.

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

### Load data into columns of ARRAY type

The following example explains how to load data into columns of the [`ARRAY`](../sql-reference/sql-statements/data-types/Array.md) type.

1. Create a StarRocks table.

   In the database `test`, create a Primary Key table `array_tbl` that includes one `INT` column and two `ARRAY` columns.

   ```SQL
   CREATE TABLE `array_tbl` (
       `id` INT NOT NULL,
       `a0` ARRAY<STRING>,
       `a1` ARRAY<ARRAY<INT>>
    )
    ENGINE=OLAP
    PRIMARY KEY(`id`)
    DISTRIBUTED BY HASH(`id`)
    ;
    ```

2. Write data to StarRocks.

   Because some versions of StarRocks does not provide the metadata of `ARRAY` column, the connector can not infer the corresponding Spark data type for this column. However, you can explicitly specify the corresponding Spark data type of the column in the option `starrocks.column.types`. In this example, you can configure the option as `a0 ARRAY<STRING>,a1 ARRAY<ARRAY<INT>>`.

   Run the following codes in `spark-shell`:

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

3. Query data in the StarRocks table.

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
