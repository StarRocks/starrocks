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
    <!-- for flink-1.15, connector 1.2.3+ -->
    <version>x.x.x_flink-1.15</version>
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
                //  自 2.4 版本，支持更新主键模型中的部分列。您可以通过以下两个属性指定需要更新的列，并且需要在 'sink.properties.columns' 的最后显式添加 '__op' 列。
                // .withProperty("sink.properties.partial_update", "true")
                // .withProperty("sink.properties.columns", "k1,k2,k3,__op")
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .build()
        )
    ).setParallelism(1); // 设置并行度，多并行度情况下需要考虑如何保证数据有序性。

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
                //  自 2.4 版本，支持更新主键模型中的部分列。您可以通过以下两个属性指定需要更新的列，并且需要在 'sink.properties.columns' 的最后显式添加 '__op' 列。
                // .withProperty("sink.properties.partial_update", "true")
                // .withProperty("sink.properties.columns", "k1,k2,k3,__op")
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
            // 自 2.4 版本，支持更新主键模型中的部分列。您可以通过以下两个属性指定需要更新的列，并且需要在 'sink.properties.columns' 的最后显式添加 '__op' 列。
            // "'sink.properties.partial_update' = 'true'," +
            // "'sink.properties.columns' = 'k1,k2,k3,__op'," + 
            "'sink.properties.column_separator' = '\\x01'," +
            "'sink.properties.row_delimiter' = '\\x02'," +
            "'sink.properties.*' = 'xxx'," + // Stream Load 属性，例如 `'sink.properties.columns' = 'k1, v1'`。
            "'sink.max-retries' = '3'," +
            "'sink.parallelism' = '1'" // 设置并行度，多并行度情况下需要考虑如何保证数据有序性。
        ")"
    );
    ```

## 参数说明

其中Sink选项如下：

| 参数                             | 是否必填 | 默认值        | 数据类型 | 描述                                                         |
| -------------------------------- | -------- | ------------- | -------- | ------------------------------------------------------------ |
| connector                        | 是       | 无          | String   | 固定设置为 `starrocks`。                                     |
| jdbc-url                         | 是      | 无          | String   | FE 的 MySQL Server 连接地址。格式为 `jdbc:mysql://<fe_host>:<fe_query_port>`。 |
| load-url                         | 是      | 无          | String   | FE 的 HTTP Server 连接地址。格式为 `<fe_host1>:<fe_http_port1>;<fe_host2>:<fe_http_port2>`，可以提供多个地址，使用英文分号 (;) 分隔。例如 `192.168.xxx.xxx:8030;192.168.xxx.xxx:8030`。 |
| database-name                    | 是      | 无          | String   | StarRocks 目标数据库的名称。                                 |
| table-name                       | 是      | 无          | String   | StarRocks 目标数据表的名称。                                 |
| username                         | 是      | 无          | String   | 用于访问 StarRocks 集群的用户名。该账号需具备 StarRocks 目标数据表的写权限。有关用户权限的说明，请参见[用户权限](https://docs.starrocks.io/zh-cn/latest/administration/User_privilege)。 |
| password                         | 是      | 无          | String   | 用于访问 StarRocks 集群的用户密码。                          |
| sink.semantic                    | 否       | at-least-once | String   | 数据 sink 至 StarRocks 的语义。<ul><li>`at-least-once`： 至少一次。</li><li>`exactly-once`：精确一次。</li></ul> |
| sink.version                     | 否       | AUTO          | String   | 实现 sink 的 exactly-once 语义的版本，仅适用于 1.2.4 及以上版本。<ul><li>`V2`：V2 版本，表示使用 [Stream Load 事务接口](./Flink-connector-starrocks.md)，StarRocks 2.4 及以上版本支持该接口。</li><li>`V1`：V1 版本，表示使用 Stream Load 非事务接口。</li><li>`AUTO`： 由 connector 自动选择版本，如果 connector 为 1.2.4 及以上版本，StarRocks 为 2.4 及以上版本，则使用 Stream Load 事务接口。反之，则使用 Stream Load 非事务接口。</li></ul>|
| sink.buffer-flush.max-bytes      | 否       | 94371840(90M) | String   | 数据攒批的大小，达到该阈值后将数据通过 Stream Load 批量写入 StarRocks。取值范围：[64MB, 10GB]。V1 版本 exactly-once 下只有 Flink checkpoint 触发时 flush，该参数不生效。|
| sink.buffer-flush.max-rows       | 否       | 500000        | String   | 数据攒批的条数，达到该阈值后将数据通过 Stream Load 批量写入 StarRocks。取值范围：[64000, 5000000]。V1 版本 exactly-once 下只有 Flink checkpoint 触发时 flush，该参数不生效。|
| sink.buffer-flush.interval-ms    | 否       | 300000        | String   | 数据攒批发送的间隔，用于控制数据写入 StarRocks 的延迟，取值范围：[1000, 3600000]。V1 版本 exactly-once 下只有 Flink checkpoint 触发时 flush，该参数不生效。|
| sink.max-retries                 | 否       | 3             | String   | Stream Load 失败后的重试次数。超过该数量上限，则数据导入任务报错。取值范围：[0, 10]。 |
| sink.connect.timeout-ms          | 否       | 1000          | String   | 连接 `load-url` 的超时时间。取值范围：[100, 60000]。 |
| sink.properties.*                | 否       | 无          | String   | Stream Load 的参数，控制导入行为，例如 `sink.properties.columns`，支持的参数和说明，请参见 [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md)。 <br> **说明** <br> 自 2.4 版本起，flink-connector-starrocks 支持主键模型的表进行部分更新。 |
| sink.properties.format           | 否       | CSV           | String   | Stream Load 导入时的数据格式。取值为 `CSV` 或者 `JSON`。      |
| sink.properties.timeout          | 否       | 600           | String   | Stream Load 超时时间，单位为秒。 exactly-once 下需要确保该值大于 Flink checkpoint 间隔。 |
| sink.properties.max_filter_ratio | 否       | 0             | String   | 用于指定导入作业的最大容错率，即导入作业能够容忍的因数据质量不合格而过滤掉的数据行所占的最大比例。取值范围：0~1。默认值：0。更多说明，请参见 [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD#opt_properties)。|

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

* 自 2.4 版本 StarRocks 开始支持[Stream Load 事务接口](./Stream_Load_transaction_interface.md)。自 Flink connector 1.2.4 版本起， Sink 基于事务接口重新设计实现了 exactly-once，相较于原来基于非事务接口的实现，降低了内存使用和 checkpoint 耗时，提高了作业的实时性和稳定性。
  自 Flink connector 1.2.4 版本起，sink 默认使用事务接口实现。如果需要使用非事务接口实现，则需要配置 `sink.version` 为`V1`。
   > **注意**
   >
   > 如果只升级 StarRocks 或 Flink connector，sink 会自动选择非事务接口实现。

* 基于Stream Load非事务接口实现的exactly-once，依赖flink的checkpoint-interval在每次checkpoint时保存批数据以及其label，在checkpoint完成后的第一次invoke中阻塞flush所有缓存在state当中的数据，以此达到精准一次。但如果StarRocks挂掉了，会导致用户的flink sink stream 算子长时间阻塞，并引起flink的监控报警或强制kill。

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
| totalFilteredRows | counter | number of rows filtered. totalFlushRows includes those rows. |

flink-connector-starrocks 导入底层调用的 Stream Load实现，可以在 flink 日志中查看导入状态

* 日志中如果有 `http://$fe:${http_port}/api/$db/$tbl/_stream_load` 生成，表示成功触发了 Stream Load 任务，任务结果也会打印在 flink 日志中，返回值可参考 [Stream Load 返回值](../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD#返回值)。

* 日志中如果没有上述信息，请在 [StarRocks 论坛](https://forum.starrocks.com/) 提问，我们会及时跟进。

## 常见问题

请参见 [FLink Connector 常见问题](../faq/loading/Flink_connector_faq)。
