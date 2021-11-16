# Design Background

The official flink-connector-jdbc provided by Flink is not enough to meet the import performance requirements, so we have added a new flink-connector-starrocks that is implemented internally by caching and bulk import by stream load.

## Usage

Add `com.starrocks.table.connector.flink.StarRocksDynamicTableSinkFactory` to：`src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`.

Add the following two parts to `pom.xml`:

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

Use as follows:

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

Or

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

The options of sink are as follows.

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

### Note

- To achieve exactly-once, the `two-phase commit mechanism` of the external system is required. Since StarRocks does not have this mechanism, we need to rely on Flink's checkpoint-interval to save the batch data and its label at each checkpoint, and block flush all the cached data at the first invoke after the checkpoint is completed. However, if StarRocks hangs, it will cause the user's Flink sink stream algorithm to block for a long time and cause Flink's monitoring alerts or force kills.

- CSV format is used for import by default, and users can customize the row separator and column separator by specifying `'sink.properties.row_delimiter' = '\\x02'` (supported in version above StarRocks-1.15.0) and `'sink.properties.column_separator' = '\\ x01'`.

- If you encounter an import stop, try increasing the memory of the Flink job.
