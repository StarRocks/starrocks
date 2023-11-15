# CREATE ROUTINE LOAD

## 功能

Routine Load 支持持续消费 Apache Kafka® 的消息并导入至 StarRocks 中，并且是一种基于 MySQL 协议的异步导入方式。

Routine Load 支持消费 Kafka 中 CSV 或 JSON 格式数据。Routine Load 支持通过无安全认证、SSL 加密和认证、SASL 认证机制访问 Kafka。

本文介绍 CREATE ROUTINE LOAD 的语法、参数说明和示例。

> Routine Load 的应用场景、基本原理和基本操作，请参见 [从 Apache Kafka® 持续导入](/loading/RoutineLoad.md)。

## 语法

```sql
CREATE ROUTINE LOAD [db.]job_name ON tbl_name
[load_properties]
[job_properties]
FROM data_source
[data_source_properties]
```

## 参数说明

1. **[db.]job_name**

    必填。导入作业的名称。导入作业的常见命名方式为表名+时间戳。同一数据库内，导入作业的名称必须唯一。您也可以在 `job_name` 前指定导入的数据库名称。

2. **tbl_name**

    必填。数据导入到目标表的名称。

3. **load_properties**

    选填。导入数据的属性。语法：

    ```sql
    [COLUMNS TERMINATED BY '<terminator>'],
    [COLUMNS ([<column_name> [, ...] ] [, column_assignment [, ...] ] )],
    [WHERE <expr>]
    [PARTITION ([ <partition_name> [, ...] ])]

    column_assignment:
    <column_name> = column_expression;
    ```

    1. **COLUMNS TERMINATED BY**

       指定列分隔符。对于 CSV 格式的数据，可以指定列分隔符，例如，将列分隔符指定为逗号","。

        ```sql
        COLUMNS TERMINATED BY ","
        ```

       默认为：\t。

    2. **COLUMNS**

       指定源数据中列和目标表中列的映射关系，以及定义衍生列的生成方式。

        1. 映射列

            按顺序指定源数据中各个列对应目标表中的哪些列。如果希望跳过某些列，可以指定一个不存在的列名。
            假设目标表有 3 列 k1, k2, v1。源数据有 4 列，其中第 1、2、4 列分别对应 k2, k1, v1。则书写如下：

            ```SQL
            COLUMNS (k2, k1, xxx, v1)
            ```

            其中 `xxx` 为不存在的列，用于跳过源数据中的第 3 列。

        2. 衍生列

            除了直接读取源数据的列之外，StarRocks 还提供对数据列进行加工操作。上述语法中的`column_assignment`用于指定衍生列。衍生列以 `col_name = expr` 形式表示。即支持通过 `expr` 计算得出目标表中对应列的值。
            衍生列通常排在映射列之后，虽然不是强制规定，但是 StarRocks 总是先解析映射列，再解析衍生列。
            接上一个示例，假设目标表还有第 4 列 v2，v2 由 k1 和 k2 的和产生。则可以书写如下：

            ```plain text
            COLUMNS (k2, k1, xxx, v1, v2 = k1 + k2)
            ```

        对于 CSV 格式的数据，`COLUMNS` 中映射列的个数必须要与源数据中的列个数一致。

    3. WHERE

        用于指定过滤条件，只有满足过滤条件的数据才会导入到 StarRocks 中。过滤条件中指定的列可以是映射列或衍生列。例如只希望导入 k1 大于 100 并且 k2 等于 1000 的列，则可以书写如下：

        ```plain text
        WHERE k1 > 100 and k2 = 1000
        ```

    4. PARTITION

        指定将数据导入到目标表的哪些分区中。如果不指定分区，则会自动导入到源数据对应的分区中。
        示例：

        ```plain text
        PARTITION(p1, p2, p3)
        ```

4. **job_properties**

    用于指定导入作业的通用参数。

    语法：

    ```sql
    PROPERTIES (
        "key1" = "val1",
        "key2" = "val2"
    )
    ```

    目前支持以下参数：

    1. `desired_concurrent_number`

        导入并发度。指定一个例行导入作业最多会被分成多少个子任务执行。取值必须大于 0。默认为 3。
        该并发度并不是实际的并发度。实际的并发度会由集群的节点数、负载情况、以及数据源的情况综合决定。
        示例：

        ```plain text
        "desired_concurrent_number" = "3"
        ```

    2. `max_batch_interval`

        任务的调度间隔，即任务多久执行一次，单位是「秒」。默认取值为 10s。取值范围： 5-60。建议导入间隔 10s 以上，导入频率过高可能会导致版本数过多报错。
        任务消费数据的时间由 **fe.conf** 中的 `routine_load_task_consume_second`参数指定，默认为 3s。
        任务执行超时时间由 **fe.conf** 中的 `routine_load_task_timeout_second`参数指定，默认为 15s。
        示例：

        ```plain text
        "max_batch_interval" = "20";
        ···

    3. `max_error_number/max_batch_rows`

        采样窗口内，允许的最大错误行数。取值必须大于等于 0。默认为 0，即不允许有错误行。
        采样窗口由 `max_batch_rows * 10`指定，默认为 `(200000 * 10 = 2000000)`。即在采样窗口内，如果错误行数大于 `max_error_number`，则会导致例行导入作业被暂停，需要人工检查数据质量问题。
        > 说明：被 WHERE 条件过滤掉的行不算错误行。

    4. `strict_mode`

        是否开启严格模式，默认为关闭。开启后，非空原始数据的列类型如果为 NULL，则会被过滤掉。配置方式为 `"strict_mode" = "true"`。

    5. `timezone`

        指定导入作业所使用的时区。默认为使用 Session 的 `timezone` 参数。该参数会影响所有导入涉及的和时区有关的函数结果。

    6. `format`

        指定导入数据的格式，默认是 CSV，支持 JSON 格式。

    7. `jsonpaths`

        指定导入 JSON 数据的方式，分为简单模式和匹配模式。如果设置了 `jsonpaths`参数，则为匹配模式导入，否则为简单模式导入，具体可参考示例。

    8. `strip_outer_array`

        BOOLEAN 类型。取值为 `true` 表示 JSON 数据以数组对象开始且将数组对象进行展平，默认值为 `false`。

    9. `json_root`

        `json_root` 为合法的 `jsonpaths` 字符串，用于指定 json document 的根节点，默认值为 ""。

5. **data_source**

    必填。数据源的类型。当前支持取值为 `KAFKA`。

6. **data_source_properties**

    指定数据源相关信息。

    语法：

    ```sql
    (
    "key1" = "val1",
    "key2" = "val2"
    )
    ```

    参数：
    1. `kafka_broker_list`
        Kafka 的 broker 连接信息。格式为 `ip:host`。多个broker之间以逗号分隔。
        示例：

        ```plaintext
        "kafka_broker_list" = "broker1:9092,broker2:9092"
        ```

    2. `kafka_topic`
        指定要订阅的 Kafka 的 topic。
        示例：

        ```plain text
        "kafka_topic" = "my_topic"
        ```

    3. `kafka_partitions/kafka_offsets`
        指定需要订阅的 kafka partition，以及对应的每个 partition 的起始 offset。offset 可以指定从大于等于 0 的具体 offset，或者：
            1. OFFSET_BEGINNING: 从有数据的位置开始订阅。
            2. OFFSET_END: 从末尾开始订阅。

        如果没有指定，则默认从 `OFFSET_END` 开始订阅 topic 下的所有 partition。
        示例：

        ```plain text
        "kafka_partitions" = "0,1,2,3",
        "kafka_offsets" = "101,0,OFFSET_BEGINNING,OFFSET_END"
        ```

        > 注意：上述属性设置，每设置一个分区需要设置对应分区的偏移量。

    4. `property`
        指定自定义 Kafka 参数。功能等同于kafka shell中 `--property` 参数。
        更多支持的自定义参数，请参阅 [librdkafka] (<https://github.com/edenhill/librdkafka>)  的官方 **CONFIGURATION** 文档中 client 端的配置项。
        示例:

        ```plain text
        "property.client.id" = "12345",
        "property.ssl.ca.location" = "FILE:ca-cert"
        ```

        4.1 使用SSL连接 Kafka  
        需要指定以下参数：  

        ```plain text
        "property.security.protocol" = "ssl",
        "property.ssl.ca.location" = "FILE:ca-cert",
        ```

        其中:
        "property.security.protocol" 用于指定连接方式为 SSL。
        "property.ssl.ca.location" 为 BE 访问 Kafka 时使用，指定 CA 证书的位置。

        如果 Kafka server 端开启了 client 认证，则还需设置：
        "property.ssl.certificate.location" = "FILE:client.pem",
        "property.ssl.key.location" = "FILE:client.key",
        "property.ssl.key.password" = "abcdefg",

        其中:
        "property.ssl.certificate.location" 指定 client 的 public key 的位置。
        "property.ssl.key.location" 指定 client 的 private key 的位置。
        "property.ssl.key.password" 指定 client 的 private key 的密码。

        4.2 使用SASL连接Kafka  
        需要指定以下参数：

        ```plain text
        "property.security.protocol"="SASL_PLAINTEXT",
        "property.sasl.mechanism"="PLAIN",
        "property.sasl.username"="admin",
        "property.sasl.password"="admin"
        ```

        其中：
        "property.security.protocol" 指定协议为 SASL_PLAINTEXT。
        "property.sasl.mechanism" 指定 SASL 的 认证方式为 PLAIN。
        "property.sasl.username" 指定 SASL 的用户名。
        "property.sasl.password" 指定 SASL 的密码。

        4.3 指定Kafka partition的默认起始offset  
        如果没有指定`kafka_partitions/kafka_offsets`，默认消费所有分区，此时可以指定`kafka_default_offsets`起始 offset。默认为 `OFFSET_END`，即从末尾开始订阅。
        值为
         1.OFFSET_BEGINNING: 从有数据的位置开始订阅。
         2.OFFSET_END: 从末尾开始订阅。
         示例：

        ```plaintext
        "property.kafka_default_offsets" = "OFFSET_BEGINNING"
        ```

        4.4 指定Kafka consumer group的group id  
        如果没有指定`group.id`，StarRocks会根据Routine Load的job name生成一个随机值，具体格式为`{job_name}_{random uuid}`，如`simple_job_0a64fe25-3983-44b2-a4d8-f52d3af4c3e8`。
         示例：

        ```plaintext
        "property.group.id" = "group_id_0"
        ```

导入数据格式样例

- 整型类（TINYINT/SMALLINT/INT/BIGINT/LARGEINT）：1, 1000, 1234
- 浮点类（FLOAT/DOUBLE/DECIMAL）：1.1, 0.23, .356
- 日期类（DATE/DATETIME）：2017-10-03, 2017-06-13 12: 34: 03
- 字符串类（CHAR/VARCHAR）（无引号）：I am a student, a
- NULL 值：\N

## 示例

### 示例1：创建 Routine Load 任务消费 Kafka 数据

为 `example_db` 的 `example_tbl` 创建一个名为 `test1` 的 Kafka 例行导入任务。指定列分隔符、 `group.id` 和 `client.id`，默认消费所有分区，且从有数据的位置（`OFFSET_BEGINNING`）开始订阅。

```sql
CREATE ROUTINE LOAD example_db.test1 ON example_tbl
COLUMNS TERMINATED BY ",",
COLUMNS(k1, k2, k3, v1, v2, v3 = k1 * 100)
PROPERTIES
(
    "desired_concurrent_number"="3",
    "max_batch_interval" = "20",
    "strict_mode" = "false"
)
FROM KAFKA
(
    "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
    "kafka_topic" = "my_topic",
    "property.group.id" = "xxx",
    "property.client.id" = "xxx",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
```

### 示例2：设置导入任务为严格模式

为 `example_db` 的 `example_tbl` 创建一个名为 `test1` 的 Kafka 例行导入任务。导入任务为严格模式。指定消费分区以及offsets。

```sql
CREATE ROUTINE LOAD example_db.test1 ON example_tbl
COLUMNS(k1, k2, k3, v1, v2, v3 = k1 * 100),
WHERE k1 > 100 and k2 like "%starrocks%"
PROPERTIES
(
    "desired_concurrent_number"="3",
    "max_batch_interval" = "20",
    "strict_mode" = "true"
)
FROM KAFKA
(
    "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
    "kafka_topic" = "my_topic",
    "kafka_partitions" = "0,1,2,3",
    "kafka_offsets" = "101,0,0,200"
);
```

### 示例3：Routine Load 任务增加 SSL 认证

通过 SSL 认证方式，从 Kafka 集群导入数据。同时设置 `client.id` 参数。导入任务为非严格模式，时区为 `Africa/Abidjan`。

```sql
CREATE ROUTINE LOAD example_db.test1 ON example_tbl
COLUMNS(k1, k2, k3, v1, v2, v3 = k1 * 100),
WHERE k1 > 100 and k2 like "%starrocks%"
PROPERTIES
(
    "desired_concurrent_number"="3",
    "max_batch_interval" = "20",
    "strict_mode" = "false",
    "timezone" = "Africa/Abidjan"
)
FROM KAFKA
(
    "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
    "kafka_topic" = "my_topic",
    "property.security.protocol" = "ssl",
    "property.ssl.ca.location" = "FILE:ca.pem",
    "property.ssl.certificate.location" = "FILE:client.pem",
    "property.ssl.key.location" = "FILE:client.key",
    "property.ssl.key.password" = "abcdefg",
    "property.client.id" = "my_client_id"
);
```

### 示例4：导入 JSON 格式数据

简单模式导入 JSON 数据，不指定 `jsonpaths`。

```sql
CREATE ROUTINE LOAD example_db.test_json_label_1 ON table1
COLUMNS(category,price,author)
PROPERTIES
(
    "desired_concurrent_number"="3",
    "max_batch_interval" = "20",
    "strict_mode" = "false",
    "format" = "json"
)
FROM KAFKA
(
    "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
    "kafka_topic" = "my_topic",
    "kafka_partitions" = "0,1,2",
    "kafka_offsets" = "0,0,0"
);
```

支持两种 JSON 数据格式：

1）`{"category":"a9jadhx","author":"test","price":895}`

2）`[
{"category":"a9jadhx","author":"test","price":895},
{"category":"axdfa1","author":"EvelynWaugh","price":1299}
]`

### 示例5：指定 `jsonpaths` 导入 JSON 格式数据

匹配模式导入 JSON 数据格式。

```sql
CREATE TABLE `example_tbl` (
`category` varchar(24) NULL COMMENT "",
`author` varchar(24) NULL COMMENT "",
`timestamp` bigint(20) NULL COMMENT "",
`dt` int(11) NULL COMMENT "",
`price` double REPLACE
) ENGINE=OLAP
AGGREGATE KEY(`category`,`author`,`timestamp`,`dt`)
COMMENT "OLAP"
PARTITION BY RANGE(`dt`)
(PARTITION p0 VALUES [("-2147483648"), ("20200509")),
PARTITION p20200509 VALUES [("20200509"), ("20200510")),
PARTITION p20200510 VALUES [("20200510"), ("20200511")),
PARTITION p20200511 VALUES [("20200511"), ("20200512")))
DISTRIBUTED BY HASH(`category`,`author`,`timestamp`) BUCKETS 4
PROPERTIES (
    "storage_type" = "COLUMN",
    "replication_num" = "1"
);

CREATE ROUTINE LOAD example_db.test1 ON example_tbl
COLUMNS(category, author, price, timestamp, dt=from_unixtime(timestamp, '%Y%m%d'))
PROPERTIES
(
    "desired_concurrent_number"="3",
    "max_batch_interval" = "20",
    "strict_mode" = "false",
    "format" = "json",
    "jsonpaths" = "[\"$.category\",\"$.author\",\"$.price\",\"$.timestamp\"]",
    "strip_outer_array" = "true"
)
FROM KAFKA
(
    "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
    "kafka_topic" = "my_topic",
    "kafka_partitions" = "0,1,2",
    "kafka_offsets" = "0,0,0"
);
```

JSON数据格式:

```plaintext
[
{"category":"11","title":"SayingsoftheCentury","price":895,"timestamp":1589191587},
{"category":"22","author":"2avc","price":895,"timestamp":1589191487},
{"category":"33","author":"3avc","title":"SayingsoftheCentury","timestamp":1589191387}
]
```

说明：

1）如果 JSON 数据是以数组开始，并且数组中每个对象是一条记录，则需要将 `strip_outer_array` 设置成 `true`，表示展平数组。

2）如果 JSON 数据是以数组开始，并且数组中每个对象是一条记录，在设置 `jsonpaths` 时，ROOT 节点实际上是数组中对象。

### 示例6：指定根节点 `json_root`

```sql
CREATE ROUTINE LOAD example_db.test1 ON example_tbl
COLUMNS(category, author, price, timestamp, dt=from_unixtime(timestamp, '%Y%m%d'))
PROPERTIES
(
    "desired_concurrent_number"="3",
    "max_batch_interval" = "20",
    "strict_mode" = "false",
    "format" = "json",
    "jsonpaths" = "[\"$.category\",\"$.author\",\"$.price\",\"$.timestamp\"]",
    "strip_outer_array" = "true",
    "json_root" = "$.RECORDS"
)
FROM KAFKA
(
    "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
    "kafka_topic" = "my_topic",
    "kafka_partitions" = "0,1,2",
    "kafka_offsets" = "0,0,0"
);
```

JSON 数据格式:

```plaintext
{
"RECORDS":[
{"category":"11","title":"SayingsoftheCentury","price":895,"timestamp":1589191587},
{"category":"22","author":"2avc","price":895,"timestamp":1589191487},
{"category":"33","author":"3avc","title":"SayingsoftheCentury","timestamp":1589191387}
]
}
```
