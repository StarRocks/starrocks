# Continuously load data from Apache Flink®

<<<<<<< HEAD
This topic describes how to load data from Apache Flink® to StarRocks.
=======
StarRocks provides a self-developed connector named Flink connector for Apache Flink® (Flink connector for short) to help you load data into a StarRocks table by using Flink. The basic principle is to accumulate the data and then load it all at a time into StarRocks through [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD).

The Flink connector supports DataStream API, Table API & SQL, and Python API. It has a higher and more stable performance than [flink-connector-jdbc](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/jdbc/) provided by Apache Flink®.

> **NOTICE**
>
> Loading data into StarRocks tables with Flink connector needs SELECT and INSERT privileges. If you do not have these privileges, follow the instructions provided in [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) to grant these privileges to the user that you use to connect to your StarRocks cluster.

## Version requirements

| Connector | Flink                    | StarRocks     | Java | Scala     |
|-----------|--------------------------|---------------| ---- |-----------|
| 1.2.7     | 1.11,1.12,1.13,1.14,1.15 | 2.1 and later | 8    | 2.11,2.12 |

## Obtain Flink connector

You can obtain the Flink connector JAR file in the following ways:

- Directly download the compiled Flink connector JAR file.
- Add the Flink connector as a dependency in your Maven project and then download the JAR file.
- Compile the source code of the Flink connector into a JAR file by yourself.

The naming format of the Flink connector JAR file is as follows:

- Since Flink 1.15, it's `flink-connector-starrocks-${connector_version}_flink-${flink_version}.jar`. For example, if you install Flink 1.15 and you want to use Flink connector 1.2.7, you can use `flink-connector-starrocks-1.2.7_flink-1.15.jar`.

- Prior to Flink 1.15, it's `flink-connector-starrocks-${connector_version}_flink-${flink_version}_${scala_version}.jar`. For example, if you install Flink 1.14 and Scala 2.12 in your environment, and you want to use Flink connector 1.2.7, you can use `flink-connector-starrocks-1.2.7_flink-1.14_2.12.jar`.
>>>>>>> e8682efc09 (edit language (#30359))

> **NOTICE**
>
> You can load data into StarRocks tables only as a user who has the INSERT privilege on those StarRocks tables. If you do not have the INSERT privilege, follow the instructions provided in [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) to grant the INSERT privilege to the user that you use to connect to your StarRocks cluster.

## Overview

The flink-connector-jdbc tool provided by Apache Flink® may not meet your performance requirements in certain scenarios. Therefore we provide a new connector named flink-connector-starrocks, which can cache data and then load data at a time by using [Stream Load](./StreamLoad.md).

## Procedure

To load data from Apache Flink® into StarRocks by using flink-connector-starrocks, perform the following steps:

1. Download the [source code](https://github.com/StarRocks/flink-connector-starrocks) of flink-connector-starrocks.
2. Find a file named **pom.xml**. Add the following code snippet to **pom.xml** and replace `x.x.x` in the code snippet with the latest version number of flink-connector-starrocks.

    ```Plain%20Text
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

<<<<<<< HEAD
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
=======
3. Go to the `target/` directory to find the Flink connector JAR file, such as `flink-connector-starrocks-1.2.7_flink-1.15-SNAPSHOT.jar`, generated upon compilation.

> **NOTE**
>
> The name of Flink connector which is not formally released contains the `SNAPSHOT` suffix.

## Options

| **Option**                        | **Required** | **Default value** | **Description**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|-----------------------------------|--------------|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| connector                         | Yes          | NONE              | The connector that you want to use. The value must be "starrocks".                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| jdbc-url                          | Yes          | NONE              | The address that is used to connect to the MySQL server of the FE.  You can specify multiple addresss, which must be separated by a comma (,). Format: `jdbc:mysql://<fe_host1>:<fe_query_port1>,<fe_host2>:<fe_query_port2>,<fe_host3>:<fe_query_port3>`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| load-url                          | Yes          | NONE              | The HTTP URL of the FE in your StarRocks cluster. You can specify multiple URLs, which must be separated by a semicolon (;). Format: `<fe_host1>:<fe_http_port1>;<fe_host2>:<fe_http_port2>`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| database-name                     | Yes          | NONE              | The name of the StarRocks database into which you want to load data.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| table-name                        | Yes          | NONE              | The name of the table that you want to use to load data into StarRocks.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| username                          | Yes          | NONE              | The username of the account that you want to use to load data into StarRocks.  The account needs [SELECT and INSERT privileges](../sql-reference/sql-statements/account-management/GRANT.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| password                          | Yes          | NONE              | The password of the preceding account.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| sink.version                      | No           | AUTO              | The implementation to use. This parameter is supported from Flink connector version 1.2.4 onwards. <ul><li>`V1`: Use [Stream Load](../loading/StreamLoad.md) interface to load data. Connectors before 1.2.4 only support this mode. </li> <li>`V2`: Use [Transaction Stream Load](../loading/Stream_Load_transaction_interface.md) interface to load data. It requires StarRocks to be at least version 2.4. Recommends `V2` because it optimizes the memory usage and provides a more stable exactly-once implementation. </li> <li>`AUTO`: If the version of StarRocks supports transaction Stream Load, will choose `V2` automatically, otherwise choose `V1` </li></ul> |
| sink.label-prefix                 | No           | NONE              | The label prefix used by Stream Load.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| sink.semantic                     | No           | at-least-once     | The semantics that is supported by your sink. Valid values: **at-least-once** and **exactly-once**.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| sink.buffer-flush.max-bytes       | No           | 94371840(90M)     | The maximum size of data that can be accumulated in memory before being sent to StarRocks at a time. The maximum value ranges from 64 MB to 10 GB. Setting this parameter to a larger value can improve loading performance but may increase loading latency.                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| sink.buffer-flush.max-rows        | No           | 500000            | The maximum number of rows that can be accumulated in memory before being sent to StarRocks at a time. This parameter is available only when you set `sink.version` to `V1` and set `sink.semantic` to `at-least-once`. Valid values: 64000 to 5000000.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| sink.buffer-flush.interval-ms     | No           | 300000            | The interval at which data is flushed. This parameter is available only when you set `sink.semantic` to `at-least-once`. Valid values: 1000 to 3600000. Unit: ms.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| sink.max-retries                  | No           | 3                 | The number of times that the system retries to perform the Stream Load job. This parameter is available only when you set `sink.version` to `V1`. Valid values: 0 to 10.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| sink.connect.timeout-ms           | No           | 1000              | The timeout for establishing HTTP connection. Valid values: 100 to 60000. Unit: ms.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| sink.wait-for-continue.timeout-ms | No           | 10000             | Supported since 1.2.7. The timeout for waiting response of HTTP 100-continue from the FE. Valid values: `3000` to `600000`. Unit: ms                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| sink.ignore.update-before         | No           | true              | Supported since version 1.2.8. Whether to ignore `UPDATE_BEFORE` records from Flink when loading data to Primary Key tables. If this parameter is set to false, the record is treated as a delete operation to StarRocks table.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| sink.properties.*                 | No           | NONE              | The parameters that are used to control Stream Load behavior. For example, the parameter `sink.properties.format` specifies the format used for Stream Load, such as CSV or JSON. For a list of supported parameters and their descriptions, see [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md).                                                                                                                                                                                                                                                                                                                                 |
| sink.properties.format            | No           | csv               | The format used for Stream Load. The Flink connector will transform each batch of data to the format before sending them to StarRocks. Valid values: `csv` and `json`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| sink.properties.row_delimiter     | No           | \n                | The row delimiter for CSV-formatted data.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| sink.properties.column_separator  | No           | \t                | The column separator for CSV-formatted data.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| sink.properties.max_filter_ratio  | No           | 0                 | The maximum error tolerance of the Stream Load. It's the maximum percentage of data records that can be filtered out due to inadequate data quality. Valid values: `0` to `1`. Default value: `0`. See [Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md) for details.                                                                                                                                                                                                                                                                                                                                                                      |
| sink.parallelism                  | No           | NONE              | The parallelism of the connector. Only available for Flink SQL. If not set, Flink planner will decide the parallelism. In the scenario of multi-parallelism, users need to guarantee data is written in the correct order.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |

## Data type mapping between Flink and StarRocks

| Flink data type                   | StarRocks data type   |
|-----------------------------------|-----------------------|
| BOOLEAN                           | BOOLEAN               |
| TINYINT                           | TINYINT               |
| SMALLINT                          | SMALLINT              |
| INTEGER                           | INTEGER               |
| BIGINT                            | BIGINT                |
| FLOAT                             | FLOAT                 |
| DOUBLE                            | DOUBLE                |
| DECIMAL                           | DECIMAL               |
| BINARY                            | INT                   |
| CHAR                              | STRING                |
| VARCHAR                           | STRING                |
| STRING                            | STRING                |
| DATE                              | DATE                  |
| TIMESTAMP_WITHOUT_TIME_ZONE(N)    | DATETIME              |
| TIMESTAMP_WITH_LOCAL_TIME_ZONE(N) | DATETIME              |
| ARRAY\<T\>                        | ARRAY\<T\>            |
| MAP\<KT,VT\>                      | JSON STRING           |
| ROW\<arg T...\>                   | JSON STRING           |
>>>>>>> e8682efc09 (edit language (#30359))

## Usage notes

When you load data from Apache Flink® into StarRocks, take note of the following points:

<<<<<<< HEAD
- If you specify the exactly-once semantics, the two-phase commit (2PC) protocol must be supported to ensure efficient data loading. StarRocks does not support this protocol. Therefore we need to rely on Apache Flink® to achieve exactly-once. The overall process is as follows:
  1. Save data and its label at each checkpoint that is completed at a specific checkpoint interval.
  2. After data and labels are saved, block the flushing of data cached in the state at the first invoke after each checkpoint is completed.

    If StarRocks unexpectedly exits, the operators for Apache Flink® sink streaming are blocked for a long time and Apache Flink® issues a monitoring alert or shuts down.
=======
- Since v2.4, StarRocks provides a Stream Load transaction interface. Since Flink connector version 1.2.4, the Sink is redesigned to implement exactly-once semantics based on transactional interfaces. Compared to the previous implementation based on non-transactional interfaces, the new implementation reduces memory usage and checkpoint overhead, thereby enhancing real-time performance and stability of loading. Starting from Flink connector version 1.2.4, the Sink implements transactional interfaces by default. To enable non-transactional interfaces, the `sink.version` needs to be configured as `V1`.
>>>>>>> e8682efc09 (edit language (#30359))

- By default, data is loaded in the CSV format. You can set the `sink.properties.row_delimiter` parameter to `\\x02` to specify a row separator and set the `sink.properties.column_separator` parameter to `\\x01` to specify a column separator.

- If data loading pauses, you can increase the memory of the Flink task.

- If the preceding code runs as expected and StarRocks can receive data, but the data loading fails, check whether your machine can access the HTTP port of the backends (BEs) in your StarRocks cluster. If you can successfully ping the HTTP port returned by the execution of the SHOW BACKENDS command in your StarRocks cluster, your machine can access the HTTP port of the BEs in your StarRocks cluster. For example, a machine has a public IP address and a private IP address, the HTTP ports of frontends (FEs) and BEs can be accessed through the public IP address of the FEs and BEs, the IP address that is bounded with your StarRocks cluster is the private IP address, and the value of `loadurl` for the Flink task is the HTTP port of the public IP address of the FEs. The FEs forwards the data loading task to the private IP address of the BEs. In this example, if the machine cannot ping the private IP address of the BEs, the data loading fails.
