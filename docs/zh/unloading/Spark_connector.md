# Spark StarRocks Connector

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

[Spark StarRocks Connector](https://github.com/StarRocks/starrocks-connector-for-apache-spark)

## 使用示例

[Spark Demo](https://github.com/StarRocks/demo/tree/master/SparkDemo)

### SQL

```sql
CREATE TEMPORARY VIEW spark_starrocks
USING starrocks
OPTIONS(
  "table.identifier" = "$YOUR_STARROCKS_DATABASE_NAME.$YOUR_STARROCKS_TABLE_NAME",
  "fenodes" = "$YOUR_STARROCKS_FE_HOSTNAME:$YOUR_STARROCKS_FE_RESTFUL_PORT",
  "user" = "$YOUR_STARROCKS_USERNAME",
  "password" = "$YOUR_STARROCKS_PASSWORD"
);

SELECT * FROM spark_starrocks;
```

### DataFrame

```scala
val starrocksSparkDF = spark.read.format("starrocks")
  .option("starrocks.table.identifier", "$YOUR_STARROCKS_DATABASE_NAME.$YOUR_STARROCKS_TABLE_NAME")
  .option("starrocks.fenodes", "$YOUR_STARROCKS_FE_HOSTNAME:$YOUR_STARROCKS_FE_RESTFUL_PORT")
  .option("user", "$YOUR_STARROCKS_USERNAME")
  .option("password", "$YOUR_STARROCKS_PASSWORD")
  .load()

starrocksSparkDF.show(5)
```

### RDD

```scala
import com.starrocks.connector.spark._
val starrocksSparkRDD = sc.starrocksRDD(
  tableIdentifier = Some("$YOUR_STARROCKS_DATABASE_NAME.$YOUR_STARROCKS_TABLE_NAME"),
  cfg = Some(Map(
    "starrocks.fenodes" -> "$YOUR_STARROCKS_FE_HOSTNAME:$YOUR_STARROCKS_FE_RESTFUL_PORT",
    "starrocks.request.auth.user" -> "$YOUR_STARROCKS_USERNAME",
    "starrocks.request.auth.password" -> "$YOUR_STARROCKS_PASSWORD"
  ))
)

starrocksSparkRDD.collect()
```

## 配置

### 通用配置项

| Key                                  | Default Value     | Comment                                                      |
| ------------------------------------ | ----------------- | ------------------------------------------------------------ |
| starrocks.fenodes                    | --                | StarRocks FE http 地址，支持多个地址，使用逗号分隔            |
| starrocks.table.identifier           | --                | StarRocks 表名，如：db1.tbl1                                 |
| starrocks.request.retries            | 3                 | 向StarRocks发送请求的重试次数                                    |
| starrocks.request.connect.timeout.ms | 30000             | 向StarRocks发送请求的连接超时时间                                |
| starrocks.request.read.timeout.ms    | 30000             | 向StarRocks发送请求的读取超时时间                                |
| starrocks.request.query.timeout.s    | 3600              | 查询StarRocks的超时时间，默认值为1小时，-1表示无超时限制             |
| starrocks.request.tablet.size        | Integer.MAX_VALUE | 一个RDD Partition对应的StarRocks Tablet个数。此数值设置越小，则会生成越多的Partition。从而提升Spark侧的并行度，但同时会对StarRocks造成更大的压力。 |
| starrocks.batch.size                 | 1024              | 一次从BE读取数据的最大行数。增大此数值可减少Spark与StarRocks之间建立连接的次数。从而减轻网络延迟所带来的的额外时间开销。 |
| starrocks.exec.mem.limit             | 2147483648        | 单个查询的内存限制。默认为 2GB，单位为字节                      |
| starrocks.deserialize.arrow.async    | false             | 是否支持异步转换Arrow格式到spark-starrocks-connector迭代所需的RowBatch                 |
| starrocks.deserialize.queue.size     | 64                | 异步转换Arrow格式的内部处理队列，当starrocks.deserialize.arrow.async为true时生效        |
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
