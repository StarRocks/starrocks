---
displayed_sidebar: "Chinese"
---

# 严格模式

严格模式 (Strict Mode) 是导入操作中的一个选配项，其设置会影响 StarRocks 对某些数据的导入行为和最终导入到 StarRocks 中的结果数据。

本文档主要介绍什么是严格模式、以及如何设置严格模式。

## 严格模式介绍

在导入过程中，原始列跟目标列的数据类型可能不完全一致，这种情况下，StarRocks 会对存在数据类型不一致的原始列值进行转换。转换过程中可能会发生字段类型不匹配、字段超长等转换失败的情况。转换失败的字段称为“错误字段”，包含错误字段的数据行称为“错误的数据行”。严格模式用于控制导入过程中是否会对这些错误的数据行进行过滤。

严格模式的过滤策略如下：

- 如果开启严格模式，StarRocks 会把错误的数据行过滤掉，只导入正确的数据行，并返回错误数据详情。

- 如果关闭严格模式，StarRocks 会把转换失败的错误字段转换成 `NULL` 值，并把这些包含 `NULL` 值的错误数据行跟正确的数据行一起导入。

注意以下两点：

- 实际导入过程中，正确的数据行和错误的数据行都有可能存在 `NULL` 值。如果目标列不允许 `NULL` 值，则 StarRocks 会报错，并把这些包含 `NULL` 值的数据行过滤掉。

- 对于 [Stream Load](../../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)、[Broker Load](../../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)、[Routine Load](../../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md) 和 [Spark Load](../../sql-reference/sql-statements/data-manipulation/SPARK_LOAD.md)，导入作业能够容忍的因数据质量不合格而过滤掉的错误数据行所占的最大比例，由作业的可选参数 `max_filter_ratio` 控制。[INSERT](../../sql-reference/sql-statements/data-manipulation/INSERT.md) 导入方式当前不支持 `max_filter_ratio` 参数。

下面以 CSV 格式的数据文件为例来说明严格模式的效果。假设目标列数据类型为 TINYINT [-128, 127]。以 `\N`（表示空值 null）、`abc`、`2000` 和 `1` 四个原始列值为例：

- 原始列值 `\N` 在转换为 TINYINT 类型后变为 `NULL`。

  > **NOTE**
  >
  > 原始列值 `\N` 不管目标列的数据类型是什么，转换后都变为 `NULL`。

- 原始列值 `abc` 由于数据类型与目标列的数据类型 TINYINT 不一致，因此转换失败，变为 `NULL`。

- 原始列值 `2000` 由于不在目标列的数据类型 TINYINT 的允许范围之内，因此转换失败，变为 `NULL`。

- 原始列值 `1` 可以正常转换为 TINYINT 类型的数值 `1`。

如果不开启严格模式，所有数据行都可以导入。如果开启严格模式，则只有包含 `\N` 或 `1` 的数据行可以导入，而包含 `abc` 或 `2000` 的数据行会被过滤掉。过滤掉的数据行记入导入作业参数 `max_filter_ratio` 允许的因数据质量不合格而过滤掉的数据行。

**关闭严格模式时的导入结果**

| 原始列值举例 | 转换为 TINYINT 类型后的列值 | 目标列允许空值时的导入结果 | 目标列不允许空值时的导入结果 |
| ------------ | --------------------------- | -------------------------- | ---------------------------- |
| \N          | NULL                        | 导入 `NULL` 值。           | 报错。                       |
| abc          | NULL                        | 导入 `NULL` 值。           | 报错。                       |
| 2000         | NULL                        | 导入 `NULL` 值。           | 报错。                       |
| 1            | 1                           | 导入 `1`。                 | 导入 `1`。                   |

**开启严格模式时的导入结果**

| 原始列值举例 | 转换为 TINYINT 类型后的列值 | 目标列允许空值的导入结果 | 目标列不允许空值的导入结果 |
| ------------ | --------------------------- | ------------------------ | -------------------------- |
| \N          | NULL                        | 导入 `NULL` 值。         | 报错。                     |
| abc          | NULL                        | `NULL` 值非法，过滤掉。  | 报错。                     |
| 2000         | NULL                        | `NULL` 值非法，过滤掉。  | 报错。                     |
| 1            | 1                           | 导入 `1`。               | 导入 `1`。                 |

## 设置严格模式

使用 [Stream Load](../../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)、[Broker Load](../../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)、[Routine Load](../../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md) 和 [Spark Load](../../sql-reference/sql-statements/data-manipulation/SPARK_LOAD.md) 执行数据导入时，需要通过参数 `strict_mode` 来设置严格模式。参数取值范围：`true` 和 `false`。默认值：`false`。`true` 表示开启，`false` 表示关闭。

使用 [INSERT](../../loading/InsertInto.md) 执行数据导入时，需要通过会话变量 `enable_insert_strict` 来设置严格模式。变量取值范围：`true` 和 `false`。默认值：`true`。`true` 表示开启，`false` 表示关闭。

下面介绍使用不同的导入方式时设置严格模式的方法。

### Stream Load

```Bash
curl --location-trusted -u <username>:<password> \
    -H "strict_mode: {true | false}" \
    -T <file_name> -XPUT \
    http://<fe_host>:<fe_http_port>/api/<database_name>/<table_name>/_stream_load
```

有关 Stream Load 的语法和参数说明，请参见 [STREAM LOAD](../../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)。

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

这里以 HDFS 数据源为例。有关 Broker Load 的语法和参数说明，请参见 [BROKER LOAD](../../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)。

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

这里以 Apache Kafka® 数据源为例。有关 Routine Load 的语法和参数说明，请参见 [CREATE ROUTINE LOAD](../../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md)。

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

这里以 HDFS 数据源为例。有关 Spark Load 的语法和参数说明，请参见 [SPARK LOAD](../../sql-reference/sql-statements/data-manipulation/SPARK_LOAD.md)。

### INSERT

```SQL
SET enable_insert_strict = {true | false};
INSERT INTO <table_name> ...
```

有关 INSERT 的语法和参数说明，请参见 [INSERT](../../sql-reference/sql-statements/data-manipulation/INSERT.md)。
