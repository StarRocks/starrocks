# Spark StarRocks Connector

The Spark StarRocks Connector reads data stored in StarRocks via Spark.

- The current version only supports reading data from `StarRocks`.
- Support mapping `StarRocks` tables to `DataFrame` or `RDD`.`DataFrame` is more recommended.
- Support data filtering on the `StarRocks` side to reduce the amount of data being  transferred.

## Version Requirements

| Connector | Spark  | Java | Scala |
| --------- | ----- | ---- | ----- |
| 1.0.0     | 2.x    | 8    | 2.11  |

### Usage examples

Code Reference: [https://github.com/StarRocks/demo/tree/master/SparkDemo](https://github.com/StarRocks/demo/tree/master/SparkDemo)

#### SQL

```sql
CREATE TEMPORARY VIEW spark_starrocks
USING starrocks
OPTIONS(
  "table.identifier"="$YOUR_STARROCKS_DATABASE_NAME.$YOUR_STARROCKS_TABLE_NAME",
  "fenodes"="$YOUR_STARROCKS_FE_HOSTNAME:$YOUR_STARROCKS_FE_RESFUL_PORT",
  "user"="$YOUR_STARROCKS_USERNAME",
  "password"="$YOUR_STARROCKS_PASSWORD"
);

SELECT * FROM spark_starrocks;
```

#### DataFrame

```scala
val starrocksSparkDF = spark.read.format("starrocks")
  .option("starrocks.table.identifier", "$YOUR_STARROCKS_DATABASE_NAME.$YOUR_STARROCKS_TABLE_NAME")
 .option("starrocks.fenodes", "$YOUR_STARROCKS_FE_HOSTNAME:$YOUR_STARROCKS_FE_RESFUL_PORT")
  .option("user", "$YOUR_STARROCKS_USERNAME")
  .option("password", "$YOUR_STARROCKS_PASSWORD")
  .load()

starrocksSparkDF.show(5)
```

#### RDD

```scala
import org.apache.starrocks.spark._
val starrocksSparkRDD = sc.starrocksRDD(
  tableIdentifier = Some("$YOUR_STARROCKS_DATABASE_NAME.$YOUR_STARROCKS_TABLE_NAME"),
  cfg = Some(Map(
    "starrocks.fenodes" -> "$YOUR_STARROCKS_FE_HOSTNAME:$YOUR_STARROCKS_FE_RESFUL_PORT",
    "starrocks.request.auth.user" -> "$YOUR_STARROCKS_USERNAME",
    "starrocks.request.auth.password" -> "$YOUR_STARROCKS_PASSWORD"
  ))
)

starrocksSparkRDD.collect()
```

### Configuration

#### General configuration

| Key                              | Default Value     | Comment                                                      |
| -------------------------------- | ----------------- | ------------------------------------------------------------ |
| starrocks.fenodes                    | --                | http address of StarRocks FE, multiple addresses supported, separated by commas            |
| starrocks.table.identifier           | --                | table name of StarRocks (e.g. db1.tbl1)                                 |
| starrocks.request.retries            | 3                 | number of retry requests sent to StarRocks                                    |
| starrocks.request.connect.timeout.ms | 30000             | requests connection timeout sent to StarRocks                                            |
| starrocks.request.read.timeout.ms    | 30000             | requests read timeout sent to StarRocks                                |
| starrocks.request.query.timeout.s    | 3600              | Query the timeout time of StarRocks, the default value is 1 hour, -1 means no timeout limit             |
| starrocks.request.tablet.size        | Integer.MAX_VALUE | The number of StarRocks Tablets for an RDD Partition. The smaller this value is, the more partitions will be generated, which increases Spark's parallelism and puts more pressure on StarRocks. |
| starrocks.batch.size                 | 1024              | The maximum number of data rows to read from BE at a time. Increasing this value reduces the number of connections established between Spark and StarRocks and therefore mitigates overhead caused by network latency. |
| starrocks.exec.mem.limit             | 2147483648        | Memory limit for a single query. Default to 2GB, in bytes                      |
| starrocks.deserialize.arrow.async    | false             | Whether to support asynchronous conversion of Arrow format to the RowBatch required for spark-starrocks-connector iteration.                 |
| starrocks.deserialize.queue.size     | 64                | Internal processing queue for asynchronous conversion of Arrow format, effective when `starrocks.deserialize.arrow.async` is true.        |

#### SQL and Dataframe Configuration

| Key                             | Default Value | Comment                                                      |
| ------------------------------- | ------------- | ------------------------------------------------------------ |
| user                            | --            | StarRocks username                                            |
| password                        | --            | StarRocks password                                             |
| starrocks.filter.query.in.max.count | 100           | The maximum number of elements of an “in” expression’s value list in the predicate pushdown. Beyond this number, the filtering of the `in` expression is handled in Spark. |

#### RDD Configuration

| Key                         | Default Value | Comment                                                      |
| --------------------------- | ------------- | ------------------------------------------------------------ |
| starrocks.request.auth.user     | --            | StarRocks username                                            |
| starrocks.request.auth.password | --            | StarRocks password                                             |
| starrocks.read.field            | --            | Retrieves a list of column names from the StarRocks table, with multiple columns separated by commas.                 |
| starrocks.filter.query          | --            | StarRocks uses this expression to complete the source-side data filtering. |

### StarRocks and Spark column type mapping

| StarRocks Type | Spark Type                       |
| ---------- | -------------------------------- |
| NULL_TYPE  | DataTypes.NullType               |
| BOOLEAN    | DataTypes.BooleanType            |
| TINYINT    | DataTypes.ByteType               |
| SMALLINT   | DataTypes.ShortType              |
| INT        | DataTypes.IntegerType            |
| BIGINT     | DataTypes.LongType               |
| FLOAT      | DataTypes.FloatType              |
| DOUBLE     | DataTypes.DoubleType             |
| DATE       | DataTypes.StringType             |
| DATETIME   | DataTypes.StringType             |
| BINARY     | DataTypes.BinaryType             |
| DECIMAL    | DecimalType                      |
| CHAR       | DataTypes.StringType             |
| LARGEINT   | DataTypes.StringType             |
| VARCHAR    | DataTypes.StringType             |
| DECIMALV2  | DecimalType                      |
| TIME       | DataTypes.DoubleType             |
| HLL        | Unsupported datatype             |

- Note: In Connector, `DATE` and `DATETIME` are mapped to `String`. Due to the operating mechanism of StarRock’s storage engine, the time range covered when using the time type cannot meet the demand. It’s recommended to use the `String` type to retrieve the data being read within the corresponding time.
