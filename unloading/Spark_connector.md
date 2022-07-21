# Spark StarRocks Connector

## Spark StarRocks Connector 总览

Spark StarRocks Connector 可以支持通过 Spark 读取 StarRocks 中存储的数据。

- 当前版本只支持从`StarRocks`中读取数据。
- 可以将`StarRocks`表映射为`DataFrame`或者`RDD`，推荐使用`DataFrame`。
- 支持在`StarRocks`端完成数据过滤，减少数据传输量。

## 版本要求

| Connector | Spark | StarRocks | Java | Scala |
| --------- | ----- | --------- | ---- | ----- |
| 1.0.0     | 2.x   | 1.18+     | 8    | 2.11  |
| 1.0.0     | 3.x   | 1.18+     | 8    | 2.12  |

## 编译部署

[Spark StarRocks Connector](https://github.com/StarRocks/spark-starrocks-connector/blob/main/docs/spark-starrocks-connector.md)

## 使用示例

[Spark Demo](https://github.com/StarRocks/demo/tree/master/SparkDemo)

### SQL

```sql
CREATE TEMPORARY VIEW spark_starrocks
USING starrocks
OPTIONS(
  "table.identifier" = "$STARROCKS_DATABASE_NAME.$STARROCKS_TABLE_NAME",
  "fenodes" = "$STARROCKS_FE_HOSTNAME:$STARROCKS_FE_RESTFUL_PORT",
  "user" = "$STARROCKS_USERNAME",
  "password" = "$STARROCKS_PASSWORD"
);

SELECT * FROM spark_starrocks;
```

### DataFrame

```scala
val starrocksSparkDF = spark.read.format("starrocks")
  .option("starrocks.table.identifier", "$STARROCKS_DATABASE_NAME.$STARROCKS_TABLE_NAME")
  .option("starrocks.fenodes", "$STARROCKS_FE_HOSTNAME:$STARROCKS_FE_RESTFUL_PORT")
  .option("user", "$STARROCKS_USERNAME")
  .option("password", "$STARROCKS_PASSWORD")
  .load()

starrocksSparkDF.show(5)
```

### RDD

```scala
import com.starrocks.connector.spark._
val starrocksSparkRDD = sc.starrocksRDD(
  tableIdentifier = Some("$STARROCKS_DATABASE_NAME.$STARROCKS_TABLE_NAME"),
  cfg = Some(Map(
    "starrocks.fenodes" -> "$STARROCKS_FE_HOSTNAME:$STARROCKS_FE_RESTFUL_PORT",
    "starrocks.request.auth.user" -> "$STARROCKS_USERNAME",
    "starrocks.request.auth.password" -> "$STARROCKS_PASSWORD"
  ))
)

starrocksSparkRDD.collect()
```

## 配置

### 通用配置项

| 配置项                               | 默认值            | 说明                                                         |
| ------------------------------------ | ----------------- | ------------------------------------------------------------ |
| starrocks.fenodes                    | --                | StarRocks FE 的 http 地址，支持多个地址，使用逗号 (,) 分隔。 |
| starrocks.table.identifier           | --                | StarRocks 表名，如 db1.tbl1。                                |
| starrocks.request.retries            | 3                 | 向 StarRocks 发送请求的重试次数。                            |
| starrocks.request.connect.timeout.ms | 30000             | 向 StarRocks 发送请求的连接超时时间。                        |
| starrocks.request.read.timeout.ms    | 30000             | 向 StarRocks 发送请求的读取超时时间。                        |
| starrocks.request.query.timeout.s    | 3600              | 查询 StarRocks 的超时时间，默认值为 1 小时，-1 表示无超时限制。 |
| starrocks.request.tablet.size        | Integer.MAX_VALUE | 一个 Spark RDD分区 对应的 StarRocks Tablet 个数。参数设置越小，生成的分区越多，进而提升 Spark 侧的并行度，但同时会对 StarRocks 造成更大的压力。 |
| starrocks.batch.size                 | 1024              | 单次从 BE 读取数据的最大行数。调大参数取值可减少 Spark 与 StarRocks 之间建立连接的次数，从而减轻网络延迟所带来的的额外时间开销。 |
| starrocks.exec.mem.limit             | 2147483648        | 单个查询的内存限制。默认为 2 GB，单位为字节。                |
| starrocks.deserialize.arrow.async    | false             | 是否支持异步转换 Arrow 格式到 spark-starrocks-connector 迭代所需的 RowBatch。 |
| starrocks.deserialize.queue.size     | 64                | 异步转换 Arrow 格式的内部处理队列，当 starrocks.deserialize.arrow.async 为 true 时生效。 |
| starrocks.filter.query               | --                | 过滤读取数据的表达式，此表达式透传给 StarRocks。StarRocks 使用此表达式完成源端数据过滤。 |

### SQL 和 Dataframe 专有配置

| Key                                 | Default Value | Comment                                                      |
| ----------------------------------- | ------------- | ------------------------------------------------------------ |
| user                                | --            | 访问StarRocks的用户名                                            |
| password                            | --            | 访问StarRocks的密码                                              |
| starrocks.filter.query.in.max.count | 100           | 谓词下推中，in表达式value列表元素最大数量。超过此数量，则in表达式条件过滤在Spark侧处理。 |

### RDD 专有配置

| Key                             | Default Value | Comment                                                      |
| ------------------------------- | ------------- | ------------------------------------------------------------ |
| starrocks.request.auth.user     | --            | 访问StarRocks的用户名                                            |
| starrocks.request.auth.password | --            | 访问StarRocks的密码                                              |
| starrocks.read.field            | --            | 读取StarRocks表的列名列表，多列之间使用逗号分隔                  |

## StarRocks 和 Spark 列类型映射关系

| StarRocks Type | Spark Type            |
| -------------- | --------------------- |
| BOOLEAN        | DataTypes.BooleanType |
| TINYINT        | DataTypes.ByteType    |
| SMALLINT       | DataTypes.ShortType   |
| INT            | DataTypes.IntegerType |
| BIGINT         | DataTypes.LongType    |
| LARGEINT       | DataTypes.StringType  |
| FLOAT          | DataTypes.FloatType   |
| DOUBLE         | DataTypes.DoubleType  |
| DECIMAL        | DecimalType           |
| DATE           | DataTypes.StringType  |
| DATETIME       | DataTypes.StringType  |
| CHAR           | DataTypes.StringType  |
| VARCHAR        | DataTypes.StringType  |
| ARRAY          | Unsupported datatype  |
| HLL            | Unsupported datatype  |
| BITMAP         | Unsupported datatype  |

- 注：Connector中，将`DATE`和`DATETIME`映射为`String`。由于`StarRocks`底层存储引擎处理逻辑，直接使用时间类型时，覆盖的时间范围无法满足需求。所以使用 `String` 类型直接返回对应的时间可读文本。
