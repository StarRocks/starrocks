# Load data by using flink-connector-starrocks

This topic describes how to load data from Apache Flink® to StarRocks.

## Overview

The flink-connector-jdbc tool provided by Apache Flink® may not meet your performance requirements in certain scenarios. Therefore we provide a new connector named flink-connector-starrocks, which can cache data and then load data at a time by using Stream Load.

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

        ```Plain_Text
        // -------- sink with raw json string stream --------

        fromElements(new String[]{

            "{\"score\": \"99\", \"name\": \"stephen\"}",

            "{\"score\": \"100\", \"name\": \"lebron\"}"

        }).addSink(

            StarRocksSink.sink(

                // the sink options

                StarRocksSinkOptions.builder()

                    .withProperty("jdbc-url", "jdbc:mysql://fe1_ip:query_port,fe2_ip:query_port,fe3_ip:query_port?xxxxx")

                    .withProperty("load-url", "fe1_ip:http_port;fe2_ip:http_port;fe3_ip:http_port")

                    .withProperty("username", "xxx")

                    .withProperty("password", "xxx")

                    .withProperty("table-name", "xxx")

                    .withProperty("database-name", "xxx")

                    .withProperty("sink.properties.format", "json")

                    .withProperty("sink.properties.strip_outer_array", "true")

                    .build()

            )

        );





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

                    .withProperty("jdbc-url", "jdbc:mysql://fe1_ip:query_port,fe2_ip:query_port,fe3_ip:query_port?xxxxx")

                    .withProperty("load-url", "fe1_ip:http_port;fe2_ip:http_port;fe3_ip:http_port")

                    .withProperty("username", "xxx")

                    .withProperty("password", "xxx")

                    .withProperty("table-name", "xxx")

                    .withProperty("database-name", "xxx")

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

        ```Plain_Text
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

                "'sink.properties.column_separator' = '\\x01'," +

                "'sink.properties.row_delimiter' = '\\x02'," +

                "'sink.max-retries' = '3'" +

                "'sink.properties.*' = 'xxx'" + // stream load properties like `'sink.properties.columns' = 'k1, v1'`

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
| sink.max-retries              | No           | 1                 | STRING        | The number of times that the system retries to perform the Stream Load. Valid values: 0 to 10. |
| sink.connect.timeout-ms       | No           | 1000              | STRING        | The period of time after which the stream load times out. Valid values: 100 to 60000. Unit: ms. |
| sink.properties.*             | No           | NONE              | STRING        | The properties of the stream load. The properties include k1, k2, and k3. |

## Usage notes

When you load data from Apache Flink® into StarRocks, take note of the following points:

- If you specify the exactly-once semantics, the two-phase commit (2PC) protocol must be supported to ensure efficient data loading. StarRocks does not support this protocol. Therefore we need to rely on Apache Flink® to achieve exactly-once. The overall process is as follows:
  1. Save data and its label at each checkpoint that is completed at a specific checkpoint interval.
  2. After data and labels are saved, block the flushing of data cached in the state at the first invoke after each checkpoint is completed.

    If StarRocks unexpectedly exits, the operators for Apache Flink® sink streaming are blocked for a long time and Apache Flink® issues a monitoring alert or shuts down.

- By default, data is loaded in the CSV format. You can set the `sink.properties.row_delimiter` parameter to `\\x02` to specify a row separator and set the `sink.properties.column_separator` parameter to `\\x01` to specify a column separator.

- If data loading pauses, you can increase the memory of the Flink task.

- If the preceding code runs as expected and StarRocks can receive data, but the data loading fails, check whether your machine can access the HTTP port of the backends (BEs) in your StarRocks cluster. If you can successfully ping the HTTP port returned by the execution of the SHOW BACKENDS command in your StarRocks cluster, your machine can access the HTTP port of the backends (BEs) in your StarRocks cluster. For example, a machine has a public IP address and a private IP address, the HTTP ports of frontends (FEs) and BEs can be accessed through the public IP address of the FEs and BEs, the IP address that is bounded with your StarRocks cluster is the private IP address, and the value of `loadurl` for the Flink task is the HTTP port of the public IP address of the FEs. The FEs forwards the data loading task to the private IP address of the BEs. In this example, if the machine cannot ping the private IP address of the BEs, the data loading fails.
