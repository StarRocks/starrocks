# 从 Apache Flink® 持续导入

## 功能简介

StarRocks 提供 flink-connector-starrocks，导入数据至 StarRocks，相比于 Flink 官方提供的 flink-connector-jdbc，导入性能更佳。
flink-connector-starrocks 的内部实现是通过缓存并批量由 [Stream Load](./StreamLoad.md) 导入。

## 支持的数据源

* CSV
* JSON

## 操作步骤

### 步骤一：添加 pom 依赖

[源码地址](https://github.com/StarRocks/flink-connector-starrocks)

将以下内容加入`pom.xml`:

点击 [版本信息](https://search.maven.org/search?q=g:com.starrocks) 查看页面Latest Version信息，替换下面x.x.x内容

```Plain text
<dependency>
    <groupId>com.starrocks</groupId>
    <artifactId>flink-connector-starrocks</artifactId>
    <!-- for flink-1.14 -->
    <version>x.x.x_flink-1.14_2.11</version>
    <version>x.x.x_flink-1.14_2.12</version>
    <!-- for flink-1.13 -->
    <version>x.x.x_flink-1.13_2.11</version>
    <version>x.x.x_flink-1.13_2.12</version>
    <!-- for flink-1.12 -->
    <version>x.x.x_flink-1.12_2.11</version>
    <version>x.x.x_flink-1.12_2.12</version>
    <!-- for flink-1.11 -->
    <version>x.x.x_flink-1.11_2.11</version>
    <version>x.x.x_flink-1.11_2.12</version>
</dependency>
```

### 步骤二：调用 flink-connector-starrocks

* 如您使用 Flink DataStream API，则需要参考如下命令。

    ```scala
    // -------- 原始数据为 json 格式 --------
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
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                // 设置并行度，多并行度情况下需要考虑如何保证数据有序性
                .withProperty("sink.parallelism", "1")
                .build()
        )
    );

    // -------- 原始数据为 CSV 格式 --------
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

* 如您使用 Flink Table API，则需要参考如下命令。

    ```scala
    // -------- 原始数据为 CSV 格式 --------
    // create a table with `structure` and `properties`
    // Needed: Add `com.starrocks.connector.flink.table.StarRocksDynamicTableSinkFactory`
    //         to: `src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`
    tEnv.executeSql(
        "CREATE TABLE USER_RESULT(" +
            "name VARCHAR," +
            "score BIGINT" +
        ") WITH ( " +
            "'connector' = 'starrocks'," +
            "'jdbc-url'='jdbc:mysql://fe1_ip:query_port,fe2_ip:query_port,fe3_ip:query_port,xxxxx'," +
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

## 参数说明

其中Sink选项如下：

| Option | Required | Default | Type | Description |
|  :-----:  | :-----:  | :-----:  | :-----:  | :-----:  |
| connector | YES | NONE | String |**starrocks**|
| jdbc-url | YES | NONE | String | this will be used to execute queries in starrocks. |
| load-url | YES | NONE | String | **fe_ip:http_port;fe_ip:http_port** separated with '**;**', which would be used to do the batch sinking. |
| database-name | YES | NONE | String | starrocks database name |
| table-name | YES | NONE | String | starrocks table name |
| username | YES | NONE | String | starrocks connecting username |
| password | YES | NONE | String | starrocks connecting password |
| sink.semantic | NO | **at-least-once** | String | **at-least-once** or **exactly-once**(**flush at checkpoint only** and options like **sink.buffer-flush.*** won't work either). |
| sink.buffer-flush.max-bytes | NO | 94371840(90M) | String | the max batching size of the serialized data, range: **[64MB, 10GB]**. |
| sink.buffer-flush.max-rows | NO | 500000 | String | the max batching rows, range: **[64,000, 5000,000]**. |
| sink.buffer-flush.interval-ms | NO | 300000 | String | the flushing time interval, range: **[1000ms, 3600000ms]**. |
| sink.max-retries | NO | 3 | String | max retry times of the stream load request, range: **[0, 10]**. |
| sink.connect.timeout-ms | NO | 1000 | String | Timeout in millisecond for connecting to the `load-url`, range: **[100, 60000]**. |
| sink.properties.format|  NO | CSV | String | The file format of data loaded into starrocks. Valid values: **CSV** and **JSON**. Default value: **CSV**. |
| sink.properties.* | NO | NONE | String | the stream load properties like **'sink.properties.columns' = 'k1, k2, k3'**,details in [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md)。 |
| sink.properties.ignore_json_size | NO |false| String | ignore the batching size (100MB) of json data |

## Flink 与 StarRocks 的数据类型映射关系

| Flink type | StarRocks type |
|  :-: | :-: |
| BOOLEAN | BOOLEAN |
| TINYINT | TINYINT |
| SMALLINT | SMALLINT |
| INTEGER | INTEGER |
| BIGINT | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| DECIMAL | DECIMAL |
| BINARY | INT |
| CHAR | STRING |
| VARCHAR | STRING |
| STRING | STRING |
| DATE | DATE |
| TIMESTAMP_WITHOUT_TIME_ZONE(N) | DATETIME |
| TIMESTAMP_WITH_LOCAL_TIME_ZONE(N) | DATETIME |
| ARRAY\<T\> | ARRAY\<T\> |
| MAP\<KT,VT\> | JSON STRING |
| ROW\<arg T...\> | JSON STRING |

>注意：当前不支持 Flink 的 BYTES、VARBINARY、TIME、INTERVAL、MULTISET、RAW，具体可参考 [Flink 数据类型](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/types/)。

## 注意事项

* 支持exactly-once的数据sink保证，需要外部系统的 two phase commit 机制。由于 StarRocks 无此机制，我们需要依赖flink的checkpoint-interval在每次checkpoint时保存批数据以及其label，在checkpoint完成后的第一次invoke中阻塞flush所有缓存在state当中的数据，以此达到精准一次。但如果StarRocks挂掉了，会导致用户的flink sink stream 算子长时间阻塞，并引起flink的监控报警或强制kill。

* 默认使用csv格式进行导入，用户可以通过指定`'sink.properties.row_delimiter' = '\\x02'`（此参数自 StarRocks-1.15.0 开始支持）与`'sink.properties.column_separator' = '\\x01'`来自定义行分隔符与列分隔符。

* 如果遇到导入停止的 情况，请尝试增加flink任务的内存。

* 如果代码运行正常且能接收到数据，但是写入不成功时请确认当前机器能访问BE的http_port端口，这里指能ping通集群show backends显示的ip:port。举个例子：如果一台机器有外网和内网ip，且FE/BE的http_port均可通过外网ip:port访问，集群里绑定的ip为内网ip，任务里loadurl写的FE外网ip:http_port，FE会将写入任务转发给BE内网ip:port，这时如果Client机器ping不通BE的内网ip就会写入失败。

## 导入数据可观测指标

| Name | Type | Description |
|  :-: | :-:  | :-:  |
| totalFlushBytes | counter | successfully flushed bytes. |
| totalFlushRows | counter | successfully flushed rows. |
| totalFlushSucceededTimes | counter | number of times that the data-batch been successfully flushed. |
| totalFlushFailedTimes | counter | number of times that the flushing been failed. |

flink-connector-starrocks 导入底层调用的 Stream Load实现，可以在 flink 日志中查看导入状态

* 日志中如果有 `http://$fe:${http_port}/api/$db/$tbl/_stream_load` 生成，表示成功触发了 Stream Load 任务，任务结果也会打印在 flink 日志中，返回值可参考 [Stream Load 任务状态](../loading/StreamLoad.md#创建导入作业)。

* 日志中如果没有上述信息，请在论坛提问 [StarRocks 论坛](https://forum.starrocks.com/)，我们会及时跟进。
