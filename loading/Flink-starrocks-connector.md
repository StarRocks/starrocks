# 设计背景

flink的用户想要将数据sink到StarRocks当中，但是flink官方只提供了flink-connector-jdbc, 不足以满足导入性能要求，为此我们新增了一个flink-connector-starrocks，内部实现是通过缓存并批量由stream load导入。

## 使用方式

将`com.starrocks.table.connector.flink.StarRocksDynamicTableSinkFactory`加入到：`src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`。

将以下两部分内容加入`pom.xml`:

```plain text
<repositories>
    <repository>
        <id>starrocks-maven-releases</id>
        <url>http://starrocksvisitor:starrocksvisitor134@nexus.starrocks.com/repository/maven-releases/</url>
    </repository>
    <repository>
        <id>starrocks-maven-snapshots</id>
        <url>http://starrocksvisitor:starrocksvisitor134@nexus.starrocks.com/repository/maven-snapshots/</url>
    </repository>
</repositories>
```

```plain text
<dependency>
    <groupId>com.starrocks.connector</groupId>
    <artifactId>flink-connector-starrocks</artifactId>
    <version>1.0.32-SNAPSHOT</version>  <!-- for flink-1.11 ~ flink-1.12 -->
    <version>1.0.32_1.13-SNAPSHOT</version>  <!-- for flink-1.13 -->
</dependency>
```

使用方式如下：

```scala
// -------- sink with raw json string stream --------
fromElements(new String[]{
    "{\"score\": \"99\", \"name\": \"stephen\"}",
    "{\"score\": \"100\", \"name\": \"lebron\"}"
}).addSink(
    StarRocksSink.sink(
        // the sink options
        StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", "jdbc:mysql://fe_ip:query_port,fe_ip:query_port?xxxxx")
            .withProperty("load-url", "fe_ip:http_port;fe_ip:http_port")
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
            .withProperty("jdbc-url", "jdbc:mysql://fe_ip:query_port,fe_ip:query_port?xxxxx")
            .withProperty("load-url", "fe_ip:http_port;fe_ip:http_port")
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

或者：

```scala
// create a table with `structure` and `properties`
// Needed: Add `com.starrocks.connector.flink.table.StarRocksDynamicTableSinkFactory` to: `src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`
tEnv.executeSql(
    "CREATE TABLE USER_RESULT(" +
        "name VARCHAR," +
        "score BIGINT" +
    ") WITH ( " +
        "'connector' = 'starrocks'," +
        "'jdbc-url'='jdbc:mysql://fe_ip:query_port,fe_ip:query_port?xxxxx'," +
        "'load-url'='fe_ip:http_port;fe_ip:http_port'," +
        "'database-name' = 'xxx'," +
        "'table-name' = 'xxx'," +
        "'username' = 'xxx'," +
        "'password' = 'xxx'," +
        "'sink.properties.column_separator' = '\\x01'," +
        "'sink.properties.row_delimiter' = '\\x02'," +
        "'sink.properties.columns' = 'k1, v1'" +
    ")"
);
```

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
| sink.max-retries | NO | 1 | String | max retry times of the stream load request, range: **[0, 10]**. |
| sink.connect.timeout-ms | NO | 1000 | String | Timeout in millisecond for connecting to the `load-url`, range: **[100, 60000]**. |
| sink.properties.* | NO | NONE | String | the stream load properties like **'sink.properties.columns' = 'k1, k2, k3'**. |

### 注意事项

- 支持exactly-once的数据sink保证，需要外部系统的 two phase commit 机制。由于 StarRocks 无此机制，我们需要依赖flink的checkpoint-interval在每次checkpoint时保存批数据以及其label，在checkpoint完成后的第一次invoke中阻塞flush所有缓存在state当中的数据，以此达到精准一次。但如果StarRocks挂掉了，会导致用户的flink sink stream 算子长时间阻塞，并引起flink的监控报警或强制kill。

- 默认使用csv格式进行导入，用户可以通过指定`'sink.properties.row_delimiter' = '\\x02'`（此参数自 StarRocks-1.15.0 开始支持）与`'sink.properties.column_separator' = '\\x01'`来自定义行分隔符与列分隔符。

- 如果遇到导入停止的 情况，请尝试增加flink任务的内存。

### 完整示例

- 完整代码工程，参考 [https://github.com/StarRocks/demo](https://github.com/StarRocks/demo)
