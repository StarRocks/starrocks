# Continuously load data from Apache Flink®

This topic describes how to load data from Apache Flink® to StarRocks.

## Overview

The flink-connector-jdbc tool provided by Apache Flink® may not meet your performance requirements in certain scenarios. Therefore we provide a new connector named flink-connector-starrocks, which can cache data and then load data at a time by using [Stream Load](./StreamLoad.md).

## Procedure

To load data from Apache Flink® into StarRocks by using flink-connector-starrocks, perform the following steps:

1. Download the [source code](https://github.com/StarRocks/flink-connector-starrocks) of flink-connector-starrocks.
2. Find a file named **pom.xml**. Add the following code snippet to **pom.xml** and replace `x.x.x` in the code snippet with the latest version number of flink-connector-starrocks.

    ```Plain_Text
    <dependency>
        <groupId>com.starrocks</groupId>
        <artifactId>flink-connector-starrocks</artifactId>
        <!-- for flink-1.11, flink-1.12 -->
        <version>x.x.x_flink-1.11</version>
        <!-- for flink-1.13 -->
        <version>x.x.x_flink-1.13</version>
    </dependency>
    ```

3. Use one of the following methods to load data  

    - Load data as raw JSON string streams.

        ```java
        // -------- sink with raw json string stream --------
        fromElements(new String[]{
            "{\"score\": \"99\", \"name\": \"stephen\"}",
            "{\"score\": \"100\", \"name\": \"lebron\"}"
        }).addSink(
            StarRocksSink.sink(
                // the sink options
                StarRocksSinkOptions.builder()
                    .withProperty("jdbc-url", "jdbc:mysql://fe1_ip:query_port,fe2_ip:query_port,fe3_ip:query_port,xxxxx")
                    .withProperty("load-url", "fe1_ip:http_port;fe2_ip:http_port;fe3_ip:http_port")
                    .withProperty("username", "xxx")
                    .withProperty("password", "xxx")
                    .withProperty("table-name", "xxx")
                    .withProperty("database-name", "xxx")
                    // Since 2.4, StarRocks support partial updates for Primary Key tables. You can specify the columns to be updated by configuring the following two properties.
                    // The '__op' column must be specified at the end of 'sink.properties.columns'.
                    // .withProperty("sink.properties.partial_update", "true")
                    // .withProperty("sink.properties.columns", "k1,k2,k3,__op")
                    .withProperty("sink.properties.format", "json")
                    .withProperty("sink.properties.strip_outer_array", "true")
                    .build()
            )
        ).setParallelism(1); // Define the parallelism of the sink. In the scenario of multiple paralel sinks, you need to guarantee the data order. 

        // -------- sink with stream transformation --------
        class RowData {
            public int score;
            public String name;
            public RowData(int score, String name) {
                ......
            }
        }
        fromElements(
            new RowData[]{
                new RowData(99, "stephen"),
                new RowData(100, "lebron")
            }
        ).addSink(
            StarRocksSink.sink(
                // the table structure
                TableSchema.builder()
                    .field("score", DataTypes.INT())
                    .field("name", DataTypes.VARCHAR(20))
                    .build(),
                // the sink options
                StarRocksSinkOptions.builder()
                    .withProperty("jdbc-url", "jdbc:mysql://fe1_ip:query_port,fe2_ip:query_port,fe3_ip:query_port,xxxxx")
                    .withProperty("load-url", "fe1_ip:http_port;fe2_ip:http_port;fe3_ip:http_port")
                    .withProperty("username", "xxx")
                    .withProperty("password", "xxx")
                    .withProperty("table-name", "xxx")
                    .withProperty("database-name", "xxx")
                    // Since 2.4, StarRocks support partial updates for Primary Key tables. You can specify the columns to be updated by configuring the following two properties.
                    // The '__op' column must be specified at the end of 'sink.properties.columns'.
                    // .withProperty("sink.properties.partial_update", "true")
                    // .withProperty("sink.properties.columns", "k1,k2,k3,__op")
                    .withProperty("sink.properties.format", "csv")  
                    .withProperty("sink.properties.column_separator", "\\x01")
                    .withProperty("sink.properties.row_delimiter", "\\x02")
                    .build(),
                // set the slots with streamRowData
                (slots, streamRowData) -> {
                    slots[0] = streamRowData.score;
                    slots[1] = streamRowData.name;
                }
            )
        );
        ```

    - Load data as tables.  

        ```java
        // create a table with `structure` and `properties`
        // Needed: Add `com.starrocks.connector.flink.table.StarRocksDynamicTableSinkFactory` to: `src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`
        tEnv.executeSql(
            "CREATE TABLE USER_RESULT(" +
                "name VARCHAR," +
                "score BIGINT" +
            ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='jdbc:mysql://fe1_ip:query_port,fe2_ip:query_port,fe3_ip:query_port?xxxxx'," +
                "'load-url'='fe1_ip:http_port;fe2_ip:http_port;fe3_ip:http_port'," +
                "'database-name' = 'xxx'," +
                "'table-name' = 'xxx'," +
                "'username' = 'xxx'," +
                "'password' = 'xxx'," +
                "'sink.buffer-flush.max-rows' = '1000000'," +
                "'sink.buffer-flush.max-bytes' = '300000000'," +
                "'sink.buffer-flush.interval-ms' = '5000'," +
                // Since 2.4, StarRocks support partial updates for Primary Key tables. You can specify the columns to be updated by configuring the following two properties.
                // The '__op' column must be specified at the end of 'sink.properties.columns'.
                // "'sink.properties.partial_update' = 'true'," +
                // "'sink.properties.columns' = 'k1,k2,k3'," + 
                "'sink.properties.column_separator' = '\\x01'," +
                "'sink.properties.row_delimiter' = '\\x02'," +
                "'sink.max-retries' = '3'," +
                // Stream load properties like `'sink.properties.columns' = 'k1, v1'`
                "'sink.properties.*' = 'xxx'," + 
                // Define the parallelism of the sink. In the scenario of multiple paralel sinks, you need to guarantee the data order.
                "'sink.parallelism' = '1'"
            ")"
        );
        ```

The following table describes the `sink` options that you can configure when you load data as tables.

| **Option**                    | **Required** | **Default value** | **Data type** | **Description**                                              |
| ----------------------------- | ------------ | ----------------- | ------------- | ------------------------------------------------------------ |
| connector                     | Yes          | NONE              | STRING        | The connector that you want to use. The value must be starrocks. |
| jdbc-url                      | Yes          | NONE              | STRING        | The URL that is used to query data from StarRocks.           |
| load-url                      | Yes          | NONE              | STRING        | The URL that is used to load all data in a time. Format: fe_ip:http_port;fe_ip:http_port. |
| database-name                 | Yes          | NONE              | STRING        | The name of the StarRocks database into which you want to load data. |
| table-name                    | Yes          | NONE              | STRING        | The name of the table that you want to use to load data into StarRocks. |
| username                      | Yes          | NONE              | STRING        | The username of the account that you want to use to load data into StarRocks. |
| password                      | Yes          | NONE              | STRING        | The password of the preceding account.                       |
| sink.semantic                 | No           | at-least-once     | STRING        | The semantics that is supported by your sink. Valid values: **at-least-once** and **exactly-once**. If you specify the value as exactly-once, `sink.buffer-flush.max-bytes`, `sink.buffer-flush.max-bytes`, and `sink.buffer-flush.interval-ms` are invalid. |
| sink.buffer-flush.max-bytes   | No           | 94371840(90M)     | STRING        | The maximum size of data that can be loaded into StarRocks at a time. Valid values: 64 MB to 10 GB. |
| sink.buffer-flush.max-rows    | No           | 500000            | STRING        | The maximum number of rows that can be loaded into StarRocks at a time. Valid values: 64000 to 5000000. |
| sink.buffer-flush.interval-ms | No           | 300000            | STRING        | The interval at which data is flushed. Valid values: 1000 to 3600000. Unit: ms. |
| sink.max-retries              | No           | 3                 | STRING        | The number of times that the system retries to perform the Stream Load. Valid values: 0 to 10. |
| sink.connect.timeout-ms       | No           | 1000              | STRING        | The period of time after which the stream load times out. Valid values: 100 to 60000. Unit: ms. |
| sink.properties.*             | No           | NONE              | STRING        | The properties of the stream load. The properties include k1, k2, and k3. Since 2.4, the flink-connector-starrocks supports partial updates for Primary Key tables. |

## Usage notes

When you load data from Apache Flink® into StarRocks, take note of the following points:

- If you specify the exactly-once semantics, the two-phase commit (2PC) protocol must be supported to ensure efficient data loading. StarRocks does not support this protocol. Therefore we need to rely on Apache Flink® to achieve exactly-once. The overall process is as follows:
  1. Save data and its label at each checkpoint that is completed at a specific checkpoint interval.
  2. After data and labels are saved, block the flushing of data cached in the state at the first invoke after each checkpoint is completed.

    If StarRocks unexpectedly exits, the operators for Apache Flink® sink streaming are blocked for a long time and Apache Flink® issues a monitoring alert or shuts down.

- By default, data is loaded in the CSV format. You can set the `sink.properties.row_delimiter` parameter to `\\x02` to specify a row separator and set the `sink.properties.column_separator` parameter to `\\x01` to specify a column separator.

- If data loading pauses, you can increase the memory of the Flink task.

  - If the version of Flink connector is 1.2.8 and later, it is recommended to specify the value of `sink.label-prefix`. Note that the label prefix must be unique among all types of loading in StarRocks, such as Flink jobs, Routine Load, and Broker Load.

    - If the label prefix is specified, the Flink connector will use the label prefix to clean up lingering transactions that may be generated in some Flink
      failure scenarios, such as the Flink job fails when a checkpoint is still in progress. These lingering transactions
      are generally in `PREPARED` status if you use `SHOW PROC '/transactions/<db_id>/running';` to view them in StarRocks. When the Flink job restores from checkpoint,
      the Flink connector will find these lingering transactions according to the label prefix and some information in
      checkpoint, and abort them. The Flink connector can not abort them when the Flink job exits because of the two-phase-commit
      mechanism to implement the exactly-once. When the Flink job exits, the Flink connector has not received the notification from
      Flink checkpoint coordinator whether the transactions should be included in a successful checkpoint, and it may
      lead to data loss if these transactions are aborted anyway. You can have an overview about how to achieve end-to-end exactly-once
      in Flink in this [blogpost](https://flink.apache.org/2018/02/28/an-overview-of-end-to-end-exactly-once-processing-in-apache-flink-with-apache-kafka-too/).

    - If the label prefix is not specified, lingering transactions will be cleaned up by StarRocks only after they time out. However the number of running transactions can reach the limitation of StarRocks `max_running_txn_num_per_db` if
      Flink jobs fail frequently before transactions time out. The timeout length is controlled by StarRocks FE configuration
      `prepared_transaction_default_timeout_second` whose default value is `86400` (1 day). You can set a smaller value to it
      to make transactions expired faster when the label prefix is not specified.

- If you are certain that the Flink job will eventually recover from checkpoint or savepoint after a long downtime because of stop or continuous failover,
  please adjust the following StarRocks configurations accordingly, to avoid data loss.

  - `prepared_transaction_default_timeout_second`: StarRocks FE configuration, default value is `86400`. The value of this configuration needs to be larger than the downtime
    of the Flink job. Otherwise, the lingering transactions that are included in a successful checkpoint may be aborted because of timeout before you restart the
    Flink job, which leads to data loss.

    Note that when you set a larger value to this configuration, it is better to specify the value of `sink.label-prefix` so that the lingering transactions can be cleaned according to the label prefix and some information in
      checkpoint, instead of due to timeout (which may cause data loss).

  - `label_keep_max_second` and `label_keep_max_num`: StarRocks FE configurations, default values are `259200` and `1000`
    respectively. For details, see [FE configurations](../loading/Loading_intro.md#fe-configurations). The value of `label_keep_max_second` needs to be larger than the downtime of the Flink job. Otherwise, the Flink connector can not check the state of transactions in StarRocks by using the transaction labels saved in the Flink's savepoint or checkpoint and figure out whether these transactions are committed, which may eventually lead to data loss.

  These configurations are mutable and can be modified by using `ADMIN SET FRONTEND CONFIG`:

  ```SQL
    ADMIN SET FRONTEND CONFIG ("prepared_transaction_default_timeout_second" = "3600");
    ADMIN SET FRONTEND CONFIG ("label_keep_max_second" = "259200");
    ADMIN SET FRONTEND CONFIG ("label_keep_max_num" = "1000");
  ```

### Flush Policy

The Flink connector will buffer the data in memory, and flush them in batch to StarRocks via Stream Load. How the flush
is triggered is different between at-least-once and exactly-once.

For at-least-once, the flush will be triggered when any of the following conditions are met:

- the bytes of buffered rows reaches the limit `sink.buffer-flush.max-bytes`
- the number of buffered rows reaches the limit `sink.buffer-flush.max-rows`. (Only valid for sink version V1)
- the elapsed time since the last flush reaches the limit `sink.buffer-flush.interval-ms`
- a checkpoint is triggered

For exactly-once, the flush only happens when a checkpoint is triggered.

### Monitoring load metrics

The Flink connector provides the following metrics to monitor loading.

| Metric                     | Type    | Description                                                     |
|--------------------------|---------|-----------------------------------------------------------------|
| totalFlushBytes          | counter | successfully flushed bytes.                                     |
| totalFlushRows           | counter | number of rows successfully flushed.                                      |
| totalFlushSucceededTimes | counter | number of times that the data is successfully flushed.  |
| totalFlushFailedTimes    | counter | number of times that the data fails to be flushed.                  |
| totalFilteredRows        | counter | number of rows filtered, which is also included in totalFlushRows.    |

## Examples

The following examples show how to use the Flink connector to load data into a StarRocks table with Flink SQL or Flink DataStream.

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
DISTRIBUTED BY HASH(`id`);
```

#### Set up Flink environment

- Download Flink binary [Flink 1.15.2](https://archive.apache.org/dist/flink/flink-1.15.2/flink-1.15.2-bin-scala_2.12.tgz), and unzip it to directory `flink-1.15.2`.
- Download [Flink connector 1.2.7](https://repo1.maven.org/maven2/com/starrocks/flink-connector-starrocks/1.2.7_flink-1.15/flink-connector-starrocks-1.2.7_flink-1.15.jar), and put it into the directory `flink-1.15.2/lib`.
- Run the following commands to start a Flink cluster:

    ```shell
    cd flink-1.15.2
    ./bin/start-cluster.sh
    ```

### Run with Flink SQL

- Run the following command to start a Flink SQL client.

    ```shell
    ./bin/sql-client.sh
    ```

- Create a Flink table `score_board`, and insert values into the table via Flink SQL Client.
Note you must define the primary key in the Flink DDL if you want to load data into a Primary Key table of StarRocks. It's optional for other types of StarRocks tables.

    ```SQL
    CREATE TABLE `score_board` (
        `id` INT,
        `name` STRING,
        `score` INT,
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'starrocks',
        'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
        'load-url' = '127.0.0.1:8030',
        'database-name' = 'test',
        
        'table-name' = 'score_board',
        'username' = 'root',
        'password' = ''
    );

    INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 100);
    ```

### Run with Flink DataStream

There are several ways to implement a Flink DataStream job according to the type of the input records, such as a CSV Java `String`, a JSON Java `String` or a custom Java object.

- The input records are CSV-format `String`. See [LoadCsvRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/main/examples/src/main/java/com/starrocks/connector/flink/examples/datastream/LoadCsvRecords.java) for a complete example.

    ```java
    /**
     * Generate CSV-format records. Each record has three values separated by "\t". 
     * These values will be loaded to the columns `id`, `name`, and `score` in the StarRocks table.
     */
    String[] records = new String[]{
            "1\tstarrocks-csv\t100",
            "2\tflink-csv\t100"
    };
    DataStream<String> source = env.fromElements(records);

    /**
     * Configure the connector with the required properties.
     * You also need to add properties "sink.properties.format" and "sink.properties.column_separator"
     * to tell the connector the input records are CSV-format, and the column separator is "\t".
     * You can also use other column separators in the CSV-format records,
     * but remember to modify the "sink.properties.column_separator" correspondingly.
     */
    StarRocksSinkOptions options = StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", jdbcUrl)
            .withProperty("load-url", loadUrl)
            .withProperty("database-name", "test")
            .withProperty("table-name", "score_board")
            .withProperty("username", "root")
            .withProperty("password", "")
            .withProperty("sink.properties.format", "csv")
            .withProperty("sink.properties.column_separator", "\t")
            .build();
    // Create the sink with the options.
    SinkFunction<String> starRockSink = StarRocksSink.sink(options);
    source.addSink(starRockSink);
    ```

- The input records are JSON-format `String`. See [LoadJsonRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/main/examples/src/main/java/com/starrocks/connector/flink/examples/datastream/LoadJsonRecords.java) for a complete example.

    ```java
    /**
     * Generate JSON-format records. 
     * Each record has three key-value pairs corresponding to the columns `id`, `name`, and `score` in the StarRocks table.
     */
    String[] records = new String[]{
            "{\"id\":1, \"name\":\"starrocks-json\", \"score\":100}",
            "{\"id\":2, \"name\":\"flink-json\", \"score\":100}",
    };
    DataStream<String> source = env.fromElements(records);

    /** 
     * Configure the connector with the required properties.
     * You also need to add properties "sink.properties.format" and "sink.properties.strip_outer_array"
     * to tell the connector the input records are JSON-format and to strip the outermost array structure. 
     */
    StarRocksSinkOptions options = StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", jdbcUrl)
            .withProperty("load-url", loadUrl)
            .withProperty("database-name", "test")
            .withProperty("table-name", "score_board")
            .withProperty("username", "root")
            .withProperty("password", "")
            .withProperty("sink.properties.format", "json")
            .withProperty("sink.properties.strip_outer_array", "true")
            .build();
    // Create the sink with the options.
    SinkFunction<String> starRockSink = StarRocksSink.sink(options);
    source.addSink(starRockSink);
    ```

- The input records are custom Java objects. See [LoadCustomJavaRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/main/examples/src/main/java/com/starrocks/connector/flink/examples/datastream/LoadCustomJavaRecords.java) for a complete example.

  - In this example, the input record is a simple POJO `RowData`.

      ```java
      public static class RowData {
              public int id;
              public String name;
              public int score;
    
              public RowData() {}
    
              public RowData(int id, String name, int score) {
                  this.id = id;
                  this.name = name;
                  this.score = score;
              }
          }
      ```

  - The main program is as follows:

    ```java
    // Generate records which use RowData as the container.
    RowData[] records = new RowData[]{
            new RowData(1, "starrocks-rowdata", 100),
            new RowData(2, "flink-rowdata", 100),
        };
    DataStream<RowData> source = env.fromElements(records);

    // Configure the connector with the required properties.
    StarRocksSinkOptions options = StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", jdbcUrl)
            .withProperty("load-url", loadUrl)
            .withProperty("database-name", "test")
            .withProperty("table-name", "score_board")
            .withProperty("username", "root")
            .withProperty("password", "")
            .build();

    /**
     * The Flink connector will use a Java object array (Object[]) to represent a row to be loaded into the StarRocks table,
     * and each element is the value for a column.
     * You need to define the schema of the Object[] which matches that of the StarRocks table.
     */
    TableSchema schema = TableSchema.builder()
            .field("id", DataTypes.INT().notNull())
            .field("name", DataTypes.STRING())
            .field("score", DataTypes.INT())
            // When the StarRocks table is a Primary Key table, you must specify notNull(), for example, DataTypes.INT().notNull(), for the primary key `id`.
            .primaryKey("id")
            .build();
    // Transform the RowData to the Object[] according to the schema.
    RowDataTransformer transformer = new RowDataTransformer();
    // Create the sink with the schema, options, and transformer.
    SinkFunction<RowData> starRockSink = StarRocksSink.sink(schema, options, transformer);
    source.addSink(starRockSink);
    ```

  - The `RowDataTransformer` in the main program is defined as follows:

    ```java
    private static class RowDataTransformer implements StarRocksSinkRowBuilder<RowData> {
    
        /**
         * Set each element of the object array according to the input RowData.
         * The schema of the array matches that of the StarRocks table.
         */
        @Override
        public void accept(Object[] internalRow, RowData rowData) {
            internalRow[0] = rowData.id;
            internalRow[1] = rowData.name;
            internalRow[2] = rowData.score;
            // When the StarRocks table is a Primary Key table, you need to set the last element to indicate whether the data loading is an UPSERT or DELETE operation.
            internalRow[internalRow.length - 1] = StarRocksSinkOP.UPSERT.ordinal();
        }
    }  
    ```

## Best practices

### Load data to a Primary Key table

This section will show how to load data to a StarRocks Primary Key table to achieve partial updates and conditional updates.
You can see [Change data through loading](https://docs.starrocks.io/en-us/latest/loading/Load_to_Primary_Key_tables) for the introduction of those features.
These examples use Flink SQL.

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

#### Partial update

This example will show how to load data only to columns `id` and `name`.

1. Insert two data rows into the StarRocks table `score_board` in MySQL client.

    ```SQL
    mysql> INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 100);

    mysql> select * from score_board;
    +------+-----------+-------+
    | id   | name      | score |
    +------+-----------+-------+
    |    1 | starrocks |   100 |
    |    2 | flink     |   100 |
    +------+-----------+-------+
    2 rows in set (0.02 sec)
    ```

2. Create a Flink table `score_board` in Flink SQL client.

   - Define the DDL which only includes the columns `id` and `name`.
   - Set the option `sink.properties.partial_update` to `true` which tells the Flink connector to perform partial updates.
   - If the Flink connector version `<=` 1.2.7, you also need to set the option `sink.properties.columns` to `id,name,__op` to tells the Flink connector which columns need to be updated. Note that you need to append the field `__op` at the end. The field `__op` indicates that the data loading is an UPSERT or DELETE operation, and its values are set by the connector automatically.

    ```SQL
    CREATE TABLE `score_board` (
        `id` INT,
        `name` STRING,
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'starrocks',
        'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
        'load-url' = '127.0.0.1:8030',
        'database-name' = 'test',
        'table-name' = 'score_board',
        'username' = 'root',
        'password' = '',
        'sink.properties.partial_update' = 'true',
        -- only for Flink connector version <= 1.2.7
        'sink.properties.columns' = 'id,name,__op'
    ); 
    ```

3. Insert two data rows into the Flink table. The primary keys of the data rows are as same as these of rows in the StarRocks table. but the values in the column `name` are modified.

    ```SQL
    INSERT INTO `score_board` VALUES (1, 'starrocks-update'), (2, 'flink-update');
    ```

4. Query the StarRocks table in MySQL client.
  
    ```SQL
    mysql> select * from score_board;
    +------+------------------+-------+
    | id   | name             | score |
    +------+------------------+-------+
    |    1 | starrocks-update |   100 |
    |    2 | flink-update     |   100 |
    +------+------------------+-------+
    2 rows in set (0.02 sec)
    ```

    You can see that only values for `name` change, and the values for `score` do not change.

#### Conditional update

This example will show how to do conditional update according to the value of column `score`. The update for an `id`
takes effect only when the new value for `score` is has a greater or equal to the old value.

1. Insert two data rows into the StarRocks table in MySQL client.

    ```SQL
    mysql> INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 100);
    
    mysql> select * from score_board;
    +------+-----------+-------+
    | id   | name      | score |
    +------+-----------+-------+
    |    1 | starrocks |   100 |
    |    2 | flink     |   100 |
    +------+-----------+-------+
    2 rows in set (0.02 sec)
    ```

2. Create a Flink table `score_board` in the following ways:
  
    - Define the DDL including all of columns.
    - Set the option `sink.properties.merge_condition` to `score` to tell the connector to use the column `score`
    as the condition.
    - Set the option `sink.version` to `V1` which tells the connector to use Stream Load.

    ```SQL
    CREATE TABLE `score_board` (
        `id` INT,
        `name` STRING,
        `score` INT,
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'starrocks',
        'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
        'load-url' = '127.0.0.1:8030',
        'database-name' = 'test',
        'table-name' = 'score_board',
        'username' = 'root',
        'password' = '',
        'sink.properties.merge_condition' = 'score',
        'sink.version' = 'V1'
        );
    ```

3. Insert two data rows into the Flink table. The primary keys of the data rows are as same as these of rows in the StarRocks table. The first data row has a smaller value in the column `score`, and the second data row has a larger  value in the column `score`.

    ```SQL
    INSERT INTO `score_board` VALUES (1, 'starrocks-update', 99), (2, 'flink-update', 101);
    ```

4. Query the StarRocks table in MySQL client.

    ```SQL
    mysql> select * from score_board;
    +------+--------------+-------+
    | id   | name         | score |
    +------+--------------+-------+
    |    1 | starrocks    |   100 |
    |    2 | flink-update |   101 |
    +------+--------------+-------+
    2 rows in set (0.03 sec)
    ```

   You can see that only the values of the second data row change, and the values of the first data row do not change.

### Load data into columns of BITMAP type

[`BITMAP`](https://docs.starrocks.io/en-us/latest/sql-reference/sql-statements/data-types/BITMAP) is often used to accelerate count distinct, such as counting UV, see [Use Bitmap for exact Count Distinct](https://docs.starrocks.io/en-us/latest/using_starrocks/Using_bitmap).
Here we take the counting of UV as an example to show how to load data into columns of the `BITMAP` type.

1. Create a StarRocks Aggregate table in MySQL client.

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

2. Create a Flink table in Flink SQL client.

    The column `visit_user_id` in the Flink table is of `BIGINT` type, and we want to load this column to the column `visit_users` of `BITMAP` type in the StarRocks table. So when defining the DDL of the Flink table, note that:
    - Because Flink does not support `BITMAP`, you need to define a column `visit_user_id` as `BIGINT` type to represent the column `visit_users` of `BITMAP` type in the StarRocks table.
    - You need to set the option `sink.properties.columns` to `page_id,visit_date,user_id,visit_users=to_bitmap(visit_user_id)`, which tells the connector the column mapping beween the Flink table and StarRocks table. Also you need to use [`to_bitmap`](https://docs.starrocks.io/en-us/latest/sql-reference/sql-functions/bitmap-functions/to_bitmap)
   function to tell the connector to convert the data of `BIGINT` type into `BITMAP` type.

    ```SQL
    CREATE TABLE `page_uv` (
        `page_id` INT,
        `visit_date` TIMESTAMP,
        `visit_user_id` BIGINT
    ) WITH (
        'connector' = 'starrocks',
        'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
        'load-url' = '127.0.0.1:8030',
        'database-name' = 'test',
        'table-name' = 'page_uv',
        'username' = 'root',
        'password' = '',
        'sink.properties.columns' = 'page_id,visit_date,visit_user_id,visit_users=to_bitmap(visit_user_id)'
    );
    ```

3. Load data into Flink table in Flink SQL client.

    ```SQL
    INSERT INTO `page_uv` VALUES
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 13),
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 23),
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 33),
       (1, CAST('2020-06-23 02:30:30' AS TIMESTAMP), 13),
       (2, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 23);
    ```

4. Calculate page UVs from the StarRocks table in MySQL client.

    ```SQL
    MySQL [test]> SELECT `page_id`, COUNT(DISTINCT `visit_users`) FROM `page_uv` GROUP BY `page_id`;
    +---------+-----------------------------+
    | page_id | count(DISTINCT visit_users) |
    +---------+-----------------------------+
    |       2 |                           1 |
    |       1 |                           3 |
    +---------+-----------------------------+
    2 rows in set (0.05 sec)
    ```

### Load data into columns of HLL type

[`HLL`](../sql-reference/sql-statements/data-types/HLL) can be used for approximate count distinct, see [Use HLL for approximate count distinct](../using_starrocks/Using_HLL).

Here we take the counting of UV as an example to show how to load data into columns of the `HLL` type.

1. Create a StarRocks Aggregate table

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

2. Create a Flink table in Flink SQL client.

   The column `visit_user_id` in the Flink table is of `BIGINT` type, and we want to load this column to the column `visit_users` of `HLL` type in the StarRocks table. So when defining the DDL of the Flink table, note that:
    - Because Flink does not support `BITMAP`, you need to define a column `visit_user_id` as `BIGINT` type to represent the column `visit_users` of `HLL` type in the StarRocks table.
    - You need to set the option `sink.properties.columns` to `page_id,visit_date,user_id,visit_users=hll_hash(visit_user_id)` which tells the connector the column mapping between Flink table and StarRocks table.  Also you need to use [`hll_hash`](../sql-reference/sql-functions/aggregate-functions/hll_hash) function to tell the connector to convert the data of `BIGINT` type into `HLL` type.

    ```SQL
    CREATE TABLE `hll_uv` (
        `page_id` INT,
        `visit_date` TIMESTAMP,
        `visit_user_id` BIGINT
    ) WITH (
        'connector' = 'starrocks',
        'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
        'load-url' = '127.0.0.1:8030',
        'database-name' = 'test',
        'table-name' = 'hll_uv',
        'username' = 'root',
        'password' = '',
        'sink.properties.columns' = 'page_id,visit_date,visit_user_id,visit_users=hll_hash(visit_user_id)'
    );
    ```

3. Load data into Flink table in Flink SQL client.

    ```SQL
    INSERT INTO `hll_uv` VALUES
       (3, CAST('2023-07-24 12:00:00' AS TIMESTAMP), 78),
       (4, CAST('2023-07-24 13:20:10' AS TIMESTAMP), 2),
       (3, CAST('2023-07-24 12:30:00' AS TIMESTAMP), 674);
    ```

4. Calculate page UVs from the StarRocks table in MySQL client.

    ```SQL
    mysql> SELECT `page_id`, COUNT(DISTINCT `visit_users`) FROM `hll_uv` GROUP BY `page_id`;
    **+---------+-----------------------------+
    | page_id | count(DISTINCT visit_users) |
    +---------+-----------------------------+
    |       3 |                           2 |
    |       4 |                           1 |
    +---------+-----------------------------+
    2 rows in set (0.04 sec)
    ```
>>>>>>> 69fcffe9c ([Doc] fix file without .md (#33063))
