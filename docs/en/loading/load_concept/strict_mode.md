---
displayed_sidebar: "English"
---

# Strict mode

Strict mode is an optional property that you can configure for data loads. It affects the loading behavior and the final loaded data.

This topic introduces what strict mode is and how to set strict mode.

## Understand strict mode

During data loading, the data types of the source columns may not be completely consistent with the data types of the destination columns. In such cases, StarRocks performs conversions on the source column values that have inconsistent data types. Data conversions may fail due to various issues such as unmatched field data types and field length overflows. Source column values that fail to be properly converted are unqualified column values, and source rows that contain unqualified column values are referred to as "unqualified rows". Strict mode is used to control whether to filter out unqualified rows during data loading.

Strict mode works as follows:

- If strict mode is enabled, StarRocks loads only qualified rows. It filters out unqualified rows and returns details about the unqualified rows.
- If strict mode is disabled, StarRocks converts unqualified column values into `NULL` and loads unqualified rows that contain these `NULL` values together with qualified rows.

Note the following points:

- In actual business scenarios, both qualified and unqualified rows may contain `NULL` values. If the destination columns do not allow `NULL` values, StarRocks reports errors and filters out the rows that contain `NULL` values.

- The maximum percentage of unqualified rows that can be filtered out for a [Stream Load](../../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md), [Broker Load](../../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md), [Routine Load](../../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md), or [Spark Load](../../sql-reference/sql-statements/data-manipulation/SPARK_LOAD.md) job is controlled by an optional job property `max_filter_ratio`. [INSERT](../../sql-reference/sql-statements/data-manipulation/insert.md) does not support setting the `max_filter_ratio` property.

For example, you want to load four rows that hold `\N` (`\N` denotes a `NULL` value), `abc`, `2000`, and `1` values respectively in a column from a CSV-formatted data file into a StarRocks table, and the data type of the destination StarRocks table column is TINYINT [-128, 127].

- The source column value `\N` is processed into `NULL` upon conversion to TINYINT.

  > **NOTE**
  >
  > `\N` is always processed into `NULL` upon conversion regardless of the destination data type.

- The source column value `abc` is processed into `NULL`, because its data type is not TINYINT and the conversion fails.

- The source column value `2000` is processed into `NULL`, because it is beyond the range supported by TINYINT and the conversion fails.

- The source column value `1` can be properly converted to a TINYINT-type value `1`.

If strict mode is disabled, StarRocks loads all the four rows.

If strict mode is enabled, StarRocks loads only the rows that hold `\N` or `1` and filters out the rows that hold `abc` or `2000`. The rows filtered out are counted against the maximum percentage of rows that can be filtered out due to inadequate data quality as specified by the `max_filter_ratio` parameter.

### Final loaded data with strict mode disabled

| Source column value | Column value upon conversion to TINYINT | Load result when destination column allows NULL values | Load result when destination column does not allow NULL values |
| ------------------- | --------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------ |
| \N                 | NULL                                    | The value `NULL` is loaded.                            | An error is reported.                                        |
| abc                 | NULL                                    | The value `NULL` is loaded.                            | An error is reported.                                        |
| 2000                | NULL                                    | The value `NULL` is loaded.                            | An error is reported.                                        |
| 1                   | 1                                       | The value `1` is loaded.                               | The value `1` is loaded.                                     |

### Final loaded data with strict mode enabled

| Source column value | Column value upon conversion to TINYINT | Load result when destination column allows NULL values       | Load result when destination column does not allow NULL values |
| ------------------- | --------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| \N                 | NULL                                    | The value `NULL` is loaded.                                  | An error is reported.                                        |
| abc                 | NULL                                    | The value `NULL` is not allowed and therefore is filtered out. | An error is reported.                                        |
| 2000                | NULL                                    | The value `NULL` is not allowed and therefore is filtered out. | An error is reported.                                        |
| 1                   | 1                                       | The value `1` is loaded.                                     | The value `1` is loaded.                                     |

## Set strict mode

If you run a [Stream Load](../../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md), [Broker Load](../../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md), [Routine Load](../../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md), or [Spark Load](../../sql-reference/sql-statements/data-manipulation/SPARK_LOAD.md) job to load data, use the `strict_mode` parameter to set strict mode for the load job. Valid values are `true` and `false`. The default value is `false`. The value `true` enables strict mode, and the value `false` disables strict mode.

If you execute [INSERT](../../sql-reference/sql-statements/data-manipulation/insert.md) to load data, use the `enable_insert_strict` session variable to set strict mode. Valid values are `true` and `false`. The default value is `true`. The value `true` enables strict mode, and the value `false` disables strict mode.

Examples are as follows:

### Stream Load

```Bash
curl --location-trusted -u <username>:<password> \
    -H "strict_mode: {true | false}" \
    -T <file_name> -XPUT \
    http://<fe_host>:<fe_http_port>/api/<database_name>/<table_name>/_stream_load
```

For detailed syntax and parameters about Stream Load, see [STREAM LOAD](../../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md).

### Broker Load

```SQL
LOAD LABEL [<database_name>.]<label_name>
(
    DATA INFILE ("<file_path>"[, "<file_path>" ...])
    INTO TABLE <table_name>
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
)
PROPERTIES
(
    "strict_mode" = "{true | false}"
)
```

The preceding code snippet uses HDFS as an example. For detailed syntax and parameters about Broker Load, see [BROKER LOAD](../../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md).

### Routine Load

```SQL
CREATE ROUTINE LOAD [<database_name>.]<job_name> ON <table_name>
PROPERTIES
(
    "strict_mode" = "{true | false}"
) 
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>[,<kafka_broker2_ip>:<kafka_broker2_port>...]",
    "kafka_topic" = "<topic_name>"
)
```

The preceding code snippet uses Apache Kafka® as an example. For detailed syntax and parameters about Routine Load, see [CREATE ROUTINE LOAD](../../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md).

### Spark Load

```SQL
LOAD LABEL [<database_name>.]<label_name>
(
    DATA INFILE ("<file_path>"[, "<file_path>" ...])
    INTO TABLE <table_name>
)
WITH RESOURCE <resource_name>
(
    "spark.executor.memory" = "3g",
    "broker.username" = "<hdfs_username>",
    "broker.password" = "<hdfs_password>"
)
PROPERTIES
(
    "strict_mode" = "{true | false}"   
)
```

The preceding code snippet uses HDFS as an example. For detailed syntax and parameters about Spark Load, see [SPARK LOAD](../../sql-reference/sql-statements/data-manipulation/SPARK_LOAD.md).

### INSERT

```SQL
SET enable_insert_strict = {true | false};
INSERT INTO <table_name> ...
```

For detailed syntax and parameters about INSERT, see [INSERT](../../sql-reference/sql-statements/data-manipulation/insert.md).
