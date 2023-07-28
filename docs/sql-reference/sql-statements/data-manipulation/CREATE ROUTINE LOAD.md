# CREATE ROUTINE LOAD

## Description

Routine Load is an asynchronous loading method based on the MySQL protocol. It continuously consumes messages from Apache Kafka® and loads data into StarRocks. Routine Load can consume CSV, JSON, and Avro (supported since v3.0.1) data from a Kafka cluster and access Kafka via SSL encryption, SASL authentication, or unsecured authentication. This topic describes the syntax, parameters, and examples of the CREATE ROUTINE LOAD statement.

> **NOTE**
>
> - For information about the application scenarios, principles, and basic operations of Routine Load, see [Continuously load data from Apache Kafka®](../../../loading/RoutineLoad.md).
> - You can load data into StarRocks tables only as a user who has the INSERT privilege on those StarRocks tables. If you do not have the INSERT privilege, follow the instructions provided in [GRANT](../account-management/GRANT.md) to grant the INSERT privilege to the user that you use to connect to your StarRocks cluster.

## Syntax

```SQL
CREATE ROUTINE LOAD <database_name>.<job_name> ON <table_name>
[load_properties]
[job_properties]
FROM data_source
[data_source_properties]
```

## Parameters

### `database_name`, `job_name`, `table_name`

`database_name`

Optional. The name of the StarRocks database.

`job_name`

Required. The name of the Routine Load job. A table may receive data from multiple Routine Load jobs.  We recommend that you set a meaningful Routine Load job name by using identifiable information, for example, Kafka topic name and the approximate job creation time, to distinguish multiple Routine Load jobs. The name of the Routine Load job must be unique within the same database.

`table_name`

Required. The name of the StarRocks table to which data is loaded.

### `load_properties`

Optional. The properties of the data. Syntax:

```SQL
[COLUMNS TERMINATED BY '<column_separator>'],
[ROWS TERMINATED BY '<row_separator>'],
[COLUMNS (<column1_name>[, <column2_name>, <column_assignment>, ... ])],
[WHERE <expr>],
[PARTITION (<partition1_name>[, <partition2_name>, ...])]
[TEMPORARY PARTITION (<temporary_partition1_name>[, <temporary_partition2_name>, ...])]
```

`COLUMNS TERMINATED BY`

The column separator for CSV-formatted data. The default column separator is `\t` (Tab). For example, you can use `COLUMNS TERMINATED BY ","` to specify the column separator as a comma.

> **Note**
>
> - Ensure that the column separator specified here is the same as the column separator in the data to be ingested.
> - You can use a UTF-8 string, such as a comma (,), tab, or pipe (|), whose length does not exceed 50 bytes as a text delimiter.
> - Null values are denoted by using `\N`. For example, a data record consists of three columns, and the data record holds data in the first and third columns but does not hold data in the second column. In this situation, you need to use `\N` in the second column to denote a null value. This means the record must be compiled as `a,\N,b` instead of `a,,b`. `a,,b` denotes that the second column of the record holds an empty string.

`ROWS TERMINATED BY`

The row separator for CSV-formatted data. The default row separator is `\n`.

`COLUMNS`

The mapping between the columns in the source data and the columns in the StarRocks table. For more information, see [Column mapping](#column-mapping) in this topic.

- `column_name`: If a column of the source data can be mapped to a column of the StarRocks table without any computation, you only need to specify the column name. These columns can be referred to as mapped columns.
- `column_assignment`: If a column of the source data cannot be directly mapped to a column of the StarRocks table, and the column's values must be computed by using functions before data loading, you must specify the computation function in `expr`. These columns can be referred to as derived columns.
  It is recommended to place derived columns after mapped columns because StarRocks first parses mapped columns.

`WHERE`

The filter condition. Only data that meets the filter condition can be loaded into StarRocks. For example, if you only want to ingest rows whose `col1` value is greater than `100` and `col2` value is equal to `1000`, you can use `WHERE col1 > 100 and col2 = 1000`.

> **NOTE**
>
> The columns specified in the filter condition can be source columns or derived columns.

`PARTITION`

If a StarRocks table is distributed on partitions p0, p1, p2 and p3, and you want to load the data only to p1, p2, and p3 in StarRocks and filter out the data that will be stored in p0, then you can specify `PARTITION(p1, p2, p3)` as a filter condition. By default, if you do not specify this parameter, the data will be loaded into all the partitions. Example:

```SQL
PARTITION (p1, p2, p3)
```

`TEMPORARY PARTITION`

The name of the [temporary partition](../../../table_design/Temporary_partition.md) into which you want to load data. You can specify multiple temporary partitions, which must be separated by commas (,).

### `job_properties`

Required. The properties of the load job. Syntax:

```SQL
PROPERTIES ("<key1>" = "<value1>"[, "<key2>" = "<value2>" ...])
```

| **Property**              | **Required** | **Description**                                              |
| ------------------------- | ------------ | ------------------------------------------------------------ |
| desired_concurrent_number | No           | The expected task parallelism of a single Routine Load job. Default value: `3`. The actual task parallelism is determined by the minimum value of the multiple parameters: `min(alive_be_number, partition_number, desired_concurrent_number, max_routine_load_task_concurrent_num)`. <ul><li>`alive_be_number`: the number of alive BE nodes.</li><li>`partition_number`: the number of partitions to be consumed.</li><li>`desired_concurrent_number`: the expected task parallelism of a single Routine Load  job. Default value: `3`.</li><li>`max_routine_load_task_concurrent_num`: the default maximum task parallelism of a Routine Load job, which is `5`. See [FE dynamic parameter](../../../administration/Configuration.md#configure-fe-dynamic-parameters).</li></ul>The maximum actual task parallelism is determined by either the number of alive BE nodes or the number of partitions to be consumed.|
| max_batch_interval        | No           | The scheduling interval for a task, that is, how often a task is executed. Unit: seconds. Value range: `5` ~ `60`. Default value: `10`. It is recommended to set a value larger than `10`. If the scheduling is shorter than 10 seconds, too many tablet versions are generated due to an excessively high loading frequency. |
| max_batch_rows            | No           | This property is only used to define the window of error detection. The window is the number of rows of data consumed by a single Routine Load task. The value is `10 * max_batch_rows`. The default value is `10 * 200000 = 200000`. The Routine Load task detects error data in the error detection window. Error data refers to data that StarRocks cannot parse, such as invalid JSON-formatted data. |
| max_error_number          | No           | The maximum number of error data rows allowed within an error detection window. If the number of error data rows exceeds this value, the load job will pause. You can execute [SHOW ROUTINE LOAD](./SHOW%20ROUTINE%20LOAD.md)  and view the error logs by using `ErrorLogUrls`.  After that, you can correct the error in Kafka according to the error logs. The default value is `0`, which means error rows are not allowed.<br>**NOTE** <br>Error data rows do not include data rows that are filtered out by the WHERE clause. |
| strict_mode               | No           | Specifies whether to enable the [strict mode](../../../loading/load_concept/strict_mode.md). Valid values: `true` and `false`. Default value: `false`. When the strict mode is enabled, if the value for a column in the loaded data is `NULL` but the target table does not allow a `NULL` value for this column, the data row will be filtered out. |
| log_rejected_record_num | No | Specifies the maximum number of unqualified data rows that can be logged. This parameter is supported from v3.1 onwards. Valid values: `0`, `-1`, and any non-zero positive integer. Default value: `0`.<ul><li>The value `0` specifies that no data rows that are filtered out will be logged.</li><li>The value `-1` specifies that all data rows that are filtered out will be logged.</li><li>A non-zero positive integer such as `n` specifies that up to `n` data rows that are filtered out can be logged on each BE.</li></ul> |
| timezone                  | No           | The time zone used by the load job. Default value: `Asia/Shanghai`. The value of this parameter affects the results returned by functions such as strftime(), alignment_timestamp(), and from_unixtime(). The time zone specified by this parameter is a session-level time zone. For more information, see [Configure a time zone](../../../administration/timezone.md). |
| merge_condition           | No           | Specifies the name of the column you want to use as the condition to determine whether to update data. Data will be updated only when the value of the data to be loaded into this column is greater than or equal to the current value of this column. For more information, see [Change data through loading](../../../loading/Load_to_Primary_Key_tables.md).<br>**NOTE**<br>Only Primary Key tables support conditional updates. The column that you specify cannot be a primary key column. |
| format                    | No           | The format of the data to be loaded. Valid values: `CSV`, `JSON`, and `Avro` (supported since v3.0.1). Default value: `CSV`. |
| trim_space                | No           | Specifies whether to remove spaces preceding and following column separators from the data file when the data file is in CSV format. Type: BOOLEAN. Default value: `false`.<br>For some databases, spaces are added to column separators when you export data as a CSV-formatted data file. Such spaces are called leading spaces or trailing spaces depending on their locations. By setting the `trim_space` parameter, you can enable StarRocks to remove such unnecessary spaces during data loading.<br>Note that StarRocks does not remove the spaces (including leading spaces and trailing spaces) within a field wrapped in a pair of `enclose`-specified characters. For example, the following field values use pipe (<code class="language-text">&#124;</code>) as the column separator and double quotation marks (`"`) as the `enclose`-specified character: <code class="language-text">&#124; "Love StarRocks" &#124;</code>. If you set `trim_space` to `true`, StarRocks processes the preceding field values as <code class="language-text">&#124;"Love StarRocks"&#124;</code>. |
| enclose                   | No           | Specifies the character that is used to wrap the field values in the data file according to [RFC4180](https://www.rfc-editor.org/rfc/rfc4180) when the data file is in CSV format. Type: single-byte character. Default value: `NONE`. The most prevalent characters are single quotation mark (`'`) and double quotation mark (`"`).<br>All special characters (including row separators and column separators) wrapped by using the `enclose`-specified character are considered normal symbols. StarRocks can do more than RFC4180 as it allows you to specify any single-byte character as the `enclose`-specified character.<br>If a field value contains an `enclose`-specified character, you can use the same character to escape that `enclose`-specified character. For example, you set `enclose` to `"`, and a field value is `a "quoted" c`. In this case, you can enter the field value as `"a ""quoted"" c"` into the data file. |
| escape                    | No           | Specifies the character that is used to escape various special characters, such as row separators, column separators, escape characters, and `enclose`-specified characters, which are then considered by StarRocks to be common characters and are parsed as part of the field values in which they reside. Type: single-byte character. Default value: `NONE`. The most prevalent character is slash (`\`), which must be written as double slashes (`\\`) in SQL statements.<br>**NOTE**<br>The character specified by `escape` is applied to both inside and outside of each pair of `enclose`-specified characters.<br>Two examples are as follows:<br><ul><li>When you set `enclose` to `"` and `escape` to `\`, StarRocks parses `"say \"Hello world\""` into `say "Hello world"`.</li><li>Assume that the column separator is comma (`,`). When you set `escape` to `\`, StarRocks parses `a, b\, c` into two separate field values: `a` and `b, c`.</li></ul> |
| strip_outer_array         | No           | Specifies whether to strip the outermost array structure of the JSON-formatted data. Valid values: `true` and `false`. Default value: `false`. In real-world business scenarios, JSON-formatted data may have an outermost array structure as indicated by a pair of square brackets `[]`. In this situation, we recommend that you set this parameter to `true`, so StarRocks removes the outermost square brackets `[]` and loads each inner array as a separate data record. If you set this parameter to `false`, StarRocks parses the entire JSON-formatted data into one array and loads the array as a single data record. Use the JSON-formatted data `[{"category" : 1, "author" : 2}, {"category" : 3, "author" : 4} ]` as an example. If you set this parameter to `true`, `{"category" : 1, "author" : 2}` and `{"category" : 3, "author" : 4}` are parsed as two separate data records and are loaded into two StarRocks data rows. |
| jsonpaths                 | No           | The names of the fields that you want to load from JSON-formatted data. The value of this parameter is a valid JsonPath expression. For more information, see [Configure column mapping for loading JSON-formatted data](#configure-column-mapping-for-loading-json-formatted-or-avro-formatted-data) in this topic. |
| json_root                 | No           | The root element of the JSON-formatted data to load. StarRocks extracts the elements of the root node through `json_root` for parsing. By default, the value of this parameter is empty, indicating that all JSON-formatted data will be loaded. For more information, see [Specify the root element of the JSON-formatted data to be loaded](#specify-the-root-element-of-the-json-formatted-data-to-be-loaded) in this topic. |
| task_consume_second | No | The maximum time for each Routine Load task within the specified single Routine Load job to consume data. Unit: second. Unlike the [FE dynamic parameters](https://chat.openai.com/administration/Configuration.md) `routine_load_task_consume_second` (which applies to all Routine Load jobs within the cluster), this parameter is specific to an individual Routine Load job, which is more flexible. This parameter is introduced in v3.1.0.<ul> <li>When `task_consume_second` and `task_timeout_second` are not configured, StarRocks uses the FE dynamic parameters `routine_load_task_consume_second` and `routine_load_task_timeout_second` to control the load behavior.</li> <li>When only `task_consume_second` is configured, the default value for `task_timeout_second` is calculated as `task_consume_second` * 4.</li> <li>When only `task_timeout_second` is configured, the default value for `task_consume_second` is calculated as `task_timeout_second` / 4.</li> </ul> |
|task_timeout_second|No|The timeout duration for each Routine Load task within the specified Routine Load job. Unit: second. Unlike the [FE dynamic parameter](https://chat.openai.com/administration/Configuration.md) `routine_load_task_timeout_second` (which applies to all Routine Load jobs within the cluster), this parameter is specific to an individual Routine Load job, providing greater flexibility, which is more flexible. This parameter is introduced in v3.1.0. <ul> <li>When `task_consume_second` and `task_timeout_second` are not configured, StarRocks uses the FE dynamic parameters `routine_load_task_consume_second` and `routine_load_task_timeout_second` to control the load behavior.</li> <li>When only `task_timeout_second` is configured, the default value for `task_consume_second` is calculated as `task_timeout_second` / 4.</li> <li>When only `task_consume_second` is configured, the default value for `task_timeout_second` is calculated as `task_consume_second` * 4.</li> </ul>|

### `data_source`, `data_source_properties`

Required. The data source and relevant properties.

```sql
FROM <data_source>
 ("<key1>" = "<value1>"[, "<key2>" = "<value2>" ...])
```

`data_source`

Required. The source of the data you want to load. Valid value: `KAFKA`.

`data_source_properties`

The properties of the data source.

| Property          | Required | Description                                                  |
| ----------------- | -------- | ------------------------------------------------------------ |
| kafka_broker_list | Yes      | Kafka's broker connection information. The format is `<kafka_broker_ip>:<broker_ port>`. Multiple brokers are separated by commas (,). The default port used by Kafka brokers is `9092`. Example:`"kafka_broker_list" = ""xxx.xx.xxx.xx:9092,xxx.xx.xxx.xx:9092"`. |
| kafka_topic       | Yes      | The  Kafka topic to be consumed. A Routine Load job can only consume messages from one topic. |
| kafka_partitions  | No       | The Kafka partitions to be consumed, for example, `"kafka_partitions" = "0, 1, 2, 3"`. If this property is not specified, all partitions are consumed by default. |
| kafka_offsets     | No       | The starting offset from which to consume data in a Kafka partition as specified in `kafka_partitions`. If this property is not specified, the Routine Load job consumes data starting from the latest offsets in `kafka_partitions`. Valid values:<ul><li>A specific offset: consumes data starting from a specific offset.</li><li>`OFFSET_BEGINNING`: consumes data starting from the earliest offset possible.</li><li>`OFFSET_END`: consumes data starting from the latest offset.</li></ul> Multiple starting offsets are separated by commas (,), for example, `"kafka_offsets" = "1000, OFFSET_BEGINNING, OFFSET_END, 2000"`.|
| property.kafka_default_offsets| No| The default starting offset for all consumer partitions. The supported values for this property are same as those for the `kafka_offsets` property.|
| confluent.schema.registry.url|No |The URL of the Schema Registry where the Avro schema is registered. StarRocks retrieves the Avro schema by using this URL. The format is as follows:<br>`confluent.schema.registry.url = http[s]://[<schema-registry-api-key>:<schema-registry-api-secret>@]<hostname or ip address>[:<port>]`|

#### More data source-related properties

You can specify additional data source (Kafka) related properties, which are equivalent to using the Kafka command line `--property`. For more supported properties, see the properties for a Kafka consumer client in [librdkafka configuration properties](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).

> **NOTE**
>
> If the value of a property is a file name, add the keyword `FILE:` preceding the file name. For information about how to create a file, see [CREATE FILE](../Administration/CREATE%20FILE.md).

- **Specify the default initial offset for all the partitions to be consumed**

```SQL
"property.kafka_default_offsets" = "OFFSET_BEGINNING"
```

- **Specify the ID of the consumer group used by the Routine Load job**

```SQL
"property.group.id" = "group_id_0"
```

If `property.group.id` is not specified, StarRocks generates a random value based on the name of the Routine Load job, in the format of `{job_name}_{random uuid}`, such as `simple_job_0a64fe25-3983-44b2-a4d8-f52d3af4c3e8`.

- **Specify the authentication mechanism used by** **BEs** **when accessing Kafka.**
  - Access Kafka with SSL encryption and authentication:

  ```SQL
  -- Enable SSL.
  "property.security.protocol" = "ssl"
  -- File or directory path to CA certificate(s) for verifying the kafka broker's key. 
  "property.ssl.ca.location" = "FILE:ca-cert"
  -- If the Kafka server enables client authentication, the following three parameters are also required:
  -- Path to the client's public key used for authentication.
  "property.ssl.certificate.location" = "FILE:client.pem"
  -- Path to the client's private key used for authentication.
  "property.ssl.key.location" = "FILE:client.key"
  -- Password for the client's private key.
  "property.ssl.key.password" = "xxxxxx" 
  ```

- Access Kafka with SASL authentication:

  ```SQL
  -- enable SASL mechanism and non-encrypted channel
  "property.security.protocol" = "SASL_PLAINTEXT"
  -- specify the SASL mechanism as PLAIN which is a simple username/password authentication mechanism
  "property.sasl.mechanism" = "PLAIN" 
  -- SASL username
  "property.sasl.username" = "admin"
  -- SASL password
  "property.sasl.password" = "xxxxxx"
  ```

### FE and BE configuration items

For FE and BE configuration items related to Routine Load, see [configuration items](../../../administration/Configuration.md).

## Column mapping

### Configure column mapping for loading CSV-formatted data

If the columns of the  CSV-formatted data can be mapped one on one in sequence to the columns of the StarRocks table, you do not need to configure the column mapping between the data and the StarRocks table.

If the columns of the CSV-formatted data cannot be mapped one on one in sequence to the columns of the StarRocks table, you need to use the `columns` parameter to configure the column mapping between the data file and the StarRocks table. This includes the following two use cases:

- **Same number of columns but different column sequence. Also, the data from the data file does not need to be computed by functions before it is loaded into the matching StarRocks table columns.**

  - In the `columns` parameter, you need to specify the names of the StarRocks table columns in the same sequence as how the data file columns are arranged.

  - For example, the StarRocks table consists of three columns, which are `col1`, `col2`, and `col3` in sequence, and the data file also consists of three columns, which can be mapped to the StarRocks table columns `col3`, `col2`, and `col1` in sequence. In this case, you need to specify `"columns: col3, col2, col1"`.

- **Different number of columns and different column sequence. Also, the data from the data file needs to be computed by functions before it is loaded into the matching StarRocks table columns.**

  In the `columns` parameter, you need to specify the names of the StarRocks table columns in the same sequence as how the data file columns are arranged and specify the functions you want to use to compute the data. Two examples are as follows:

  - The StarRocks table consists of three columns, which are `col1`, `col2`, and `col3` in sequence. The data file consists of four columns, among which the first three columns can be mapped in sequence to the StarRocks table columns `col1`, `col2`, and `col3` and the fourth column cannot be mapped to any of the StarRocks table columns. In this case, you need to temporarily specify a name for the fourth column of the data file, and the temporary name must be different from any of the StarRocks table column names. For example, you can specify `"columns: col1, col2, col3, temp"`, in which the fourth column of the data file is temporarily named `temp`.
  - The StarRocks table consists of three columns, which are `year`, `month`, and `day` in sequence. The data file consists of only one column that accommodates date and time values in `yyyy-mm-dd hh:mm:ss` format. In this case, you can specify `"columns: col, year = year(col), month=month(col), day=day(col)"`, in which `col` is the temporary name of the data file column and the functions `year = year(col)`, `month=month(col)`, and `day=day(col)` are used to extract data from the data file column `col` and loads the data into the mapping StarRocks table columns. For example, `year = year(col)` is used to extract the `yyyy` data from the data file column `col` and loads the data into the StarRocks table column `year`.

For more examples, see [Configure column mapping](#configure-column-mapping).

### Configure column mapping for loading JSON-formatted or Avro-formatted data

> **NOTE**
>
> Since v3.0.1, StarRocks supports loading Avro data by using Routine Load. When you load JSON or Avro data, the configuration for column mapping and transformation is the same. Therefore, in this section, JSON data is used as an example to introduce the configuration.

If the keys of the JSON-formatted data have the same names as the columns of the StarRocks table, you can load the JSON-formatted data by using the simple mode. In simple mode, you do not need to specify the `jsonpaths` parameter. This mode requires that the JSON-formatted data must be an object as indicated by curly brackets `{}`, such as `{"category": 1, "author": 2, "price": "3"}`. In this example, `category`, `author`, and `price` are key names, and these keys can be mapped one on one by name to the columns `category`, `author`, and `price` of the StarRocks table. For examples, please see [simple mode](#Column names of the target table are consistent with JSON keys).

If the keys of the JSON-formatted data have different names than the columns of the StarRocks table, you can load the JSON-formatted data by using the matched mode. In matched mode, you need to use the `jsonpaths` and `COLUMNS` parameters to specify the column mapping between the JSON-formatted data and the StarRocks table:

- In the `jsonpaths` parameter, specify the JSON keys in the sequence as how they are arranged in the JSON-formatted data.
- In the `COLUMNS` parameter, specify the mapping between the JSON keys and the StarRocks table columns:
  - The column names specified in the `COLUMNS` parameter are mapped one on one in sequence to the JSON-formatted data.
  - The column names specified in the `COLUMNS` parameter are mapped one on one by name to the StarRocks table columns.

For examples, see [matched mode](#starrocks-table-column-names-different-from-json-key-names).

## Examples

### Load  CSV-formatted data

This section uses  CSV-formatted data as an example to describe how you can employ various parameter settings and combinations to meet your diverse loading requirements.

#### Prepare a dataset

Suppose you want to load CSV-formatted data from a Kafka topic named `ordertest1`.Every message in the dataset includes six columns: order ID, payment date, customer name, nationality, gender, and price.

```plaintext
2020050802,2020-05-08,Johann Georg Faust,Deutschland,male,895
2020050802,2020-05-08,Julien Sorel,France,male,893
2020050803,2020-05-08,Dorian Grey,UK,male,1262
2020050901,2020-05-09,Anna Karenina,Russia,female,175
2020051001,2020-05-10,Tess Durbeyfield,US,female,986
2020051101,2020-05-11,Edogawa Conan,japan,male,8924
```

#### Create a table

According to the columns of CSV-formatted data, create a table named `example_tbl1` in the database `example_db`.

```SQL
CREATE TABLE example_db.example_tbl1 ( 
    `order_id` bigint NOT NULL COMMENT "Order ID",
    `pay_dt` date NOT NULL COMMENT "Payment date", 
    `customer_name` varchar(26) NULL COMMENT "Customer name", 
    `nationality` varchar(26) NULL COMMENT "Nationality", 
    `gender` varchar(26) NULL COMMENT "Gender", 
    `price` double NULL COMMENT "Price") 
DUPLICATE KEY (order_id,pay_dt) 
DISTRIBUTED BY HASH(`order_id`); 
```

#### **Consume data starting from** **specified offsets for** **specified** **partitions**

If the Routine Load job needs to consume data starting from specified partitions and offsets, you need to configure the parameters `kafka_partitions` and `kafka_offsets`.

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1",
    "kafka_partitions" ="0,1,2,3,4", -- partitions to be consumed
    "kafka_offsets" = "1000, OFFSET_BEGINNING, OFFSET_END, 2000" -- corresponding initial offsets
);
```

#### **Improve loading performance by increasing task** **parallelism**

To improve loading performance and avoid accumulative consumption, you can increase task parallelism by increasing the `desired_concurrent_number` value when you create the Routine Load job. Task parallelism allows splitting one Routine Load job into as many parallel tasks as possible.

> **Note**
>
> For more ways to improve loading performance, see [Routine Load FAQ](../../../faq/loading/Routine_load_faq.md).

Note that the actual task parallelism is determined by the minimum value among the following multiple parameters:

```SQL
min(alive_be_number, partition_number, desired_concurrent_number, max_routine_load_task_concurrent_num)
```

> **Note**
>
> The maximum actual task parallelism is either the number of alive BE nodes or the number of partitions to be consumed.

Therefore, when the number of alive BE nodes and the number of partitions to be consumed are greater than the values of the other two parameters `max_routine_load_task_concurrent_num` and `desired_concurrent_number`, you can increase the values of the other two parameters to increase the actual task parallelism.

Assume that the number of partitions to be consumed is 7, the number of alive BE nodes is 5, and `max_routine_load_task_concurrent_num` is the default value `5`. If you want to increase the actual task parallelism, you can set `desired_concurrent_number` to `5` (the default value is `3`). In this case, the actual task parallelism `min(5,7,5,5)` is configured at `5`.

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
"desired_concurrent_number" = "5" -- set the value of desired_concurrent_number to 5
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### Configure column mapping

If the sequence of columns in the CSV-formatted data is inconsistent with the columns in the target table, assuming that the fifth column in the CSV-formatted data does not need to be imported to the target table, you need to specify the column mapping between the CSV-formatted data and the target table through the `COLUMNS` parameter.

#### Target database and table

Create the target table `example_tbl2` in the target database `example_db` according to the columns in the CSV-formatted data. In this scenario, you need to create five columns corresponding to the five columns in the CSV-formatted data, except for the fifth column that stores gender.

```SQL
CREATE TABLE example_db.example_tbl2 ( 
    `order_id` bigint NOT NULL COMMENT "Order ID",
    `pay_dt` date NOT NULL COMMENT "Payment date", 
    `customer_name` varchar(26) NULL COMMENT "Customer name", 
    `nationality` varchar(26) NULL COMMENT "Nationality", 
    `price` double NULL COMMENT "Price"
) 
DUPLICATE KEY (order_id,pay_dt) 
DISTRIBUTED BY HASH(order_id); 
```

#### Routine Load job

In this example, since the fifth column in the CSV-formatted data does not need to be loaded to the target table, the fifth column is temporarily named `temp_gender` in `COLUMNS`, and the other columns are directly mapped to the table `example_tbl2`.

```SQL
CREATE ROUTINE LOAD example_db.example_tbl2_ordertest1 ON example_tbl2
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, temp_gender, price)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### Set filter conditions

If you want to load only data that meets certain conditions, you can set filter conditions in the `WHERE` clause, for example, `price > 100.`

```SQL
CREATE ROUTINE LOAD example_db.example_tbl2_ordertest1 ON example_tbl2
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price),
WHERE price > 100 -- set the filter condition
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### Enable strict mode to filter out rows with NULL values

In `PROPERTIES`, you can set `"strict_mode" = "true"`, which means that the Routine Load job is in strict mode.  If there is a `NULL` value in a source column, but the destination StarRocks table column does not allow NULL values, the row that holds a NULL value in the source column is filtered out.

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
"strict_mode" = "true" -- enable the strict mode
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### Set error tolerance

If your business scenario has low tolerance for unqualified data, you need to set the error detection window and the maximum number of error data rows by configuring the parameters `max_batch_rows` and `max_error_number`. When the number of error data rows within an error detection window exceeds the value of `max_error_number`, the Routine Load job pauses.

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
"max_batch_rows" = "100000",-- The value of max_batch_rows multiplied by 10 equals the error detection window.
"max_error_number" = "100" -- The maximum number of error data rows allowed within an error detection window.
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### Configure SSL encryption

If you need to specify the security protocol used by BEs to access Kafka brokers, such as SSL encryption, you need to configure custom parameters `property.security.protocol` and `property.ssl.ca.location` to enable SSL encryption and specify the location of the CA certificate.

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
-- Enable SSL.
"property.security.protocol" = "ssl",
-- The location of the CA certificate.
"property.ssl.ca.location" = "FILE:ca-cert",
-- If authentication is enabled for Kafka clients, you need to configure the following properties:
-- The location of the Kafka client's public key.
"property.ssl.certificate.location" = "FILE:client.pem",
-- The location of the Kafka client's private key.
"property.ssl.key.location" = "FILE:client.key",
-- The password to the Kafka client's private key.
"property.ssl.key.password" = "abcdefg"
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### Setting trim_space, enclose, and escape

Suppose you want to load CSV-formatted data from a Kafka topic named `test_csv`. Every message in the dataset includes six columns: order ID, payment date, customer name, nationality, gender, and price.

```Plaintext
 "2020050802" , "2020-05-08" , "Johann Georg Faust" , "Deutschland" , "male" , "895"
 "2020050802" , "2020-05-08" , "Julien Sorel" , "France" , "male" , "893"
 "2020050803" , "2020-05-08" , "Dorian Grey\,Lord Henry" , "UK" , "male" , "1262"
 "2020050901" , "2020-05-09" , "Anna Karenina" , "Russia" , "female" , "175"
 "2020051001" , "2020-05-10" , "Tess Durbeyfield" , "US" , "female" , "986"
 "2020051101" , "2020-05-11" , "Edogawa Conan" , "japan" , "male" , "8924"
```

If you want to load all data from the Kafka topic `test_csv` into `example_tbl1`, with the intention of removing the spaces preceding and following column separators and setting `enclose` to `"` and `escape` to `\`, run the following command:

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_test_csv ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
    "trim_space"="true",
    "enclose"="\"",
    "escape"="\\",
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic"="test_csv",
    "property.kafka_default_offsets"="OFFSET_BEGINNING"
);
```

### Load  JSON-formatted data

#### StarRocks table column names consistent with JSON key names

##### Prepare a dataset

For example, the following JSON-formatted data exists in the Kafka topic `ordertest2`.

```SQL
{"commodity_id": "1", "customer_name": "Mark Twain", "country": "US","pay_time": 1589191487,"price": 875}
{"commodity_id": "2", "customer_name": "Oscar Wilde", "country": "UK","pay_time": 1589191487,"price": 895}
{"commodity_id": "3", "customer_name": "Antoine de Saint-Exupéry","country": "France","pay_time": 1589191487,"price": 895}
```

> **Note** Each JSON object must be in one Kafka message. Otherwise, an error that indicates a failure in parsing JSON-formatted data occurs.

##### Target database and table

Create table `example_tbl3` in the target database `example_db` in the StarRocks cluster. The column names are consistent with the keys names in the JSON-formatted data.

```SQL
CREATE TABLE example_db.example_tbl3 ( 
    commodity_id varchar(26) NULL, 
    customer_name varchar(26) NULL, 
    country varchar(26) NULL, 
    pay_time bigint(20) NULL, 
    price double SUM NULL COMMENT "Price") 
AGGREGATE KEY(commodity_id,customer_name,country,pay_time)
DISTRIBUTED BY HASH(commodity_id); 
```

##### Routine Load job

You can use the simple mode for the Routine Load job. That is, you do not need to specify `jsonpaths` and `COLUMNS` parameters when creating the Routine Load job. StarRocks extracts the keys of JSON-formatted data in the topic `ordertest2` of the Kafka cluster according to the column names of the target table `example_tbl3` and loads the JSON-formatted data into the target table.

```SQL
CREATE ROUTINE LOAD example_db.example_tbl3_ordertest2 ON example_tbl3
PROPERTIES
(
    "format" = "json"
 )
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest2"
);
```

> **Note**
>
> - If the outermost layer of the JSON-formatted data is an array structure, you need to set `"strip_outer_array"="true"` in `PROPERTIES` to strip the outermost array structure. Additionally, when you need to specify `jsonpaths`, the root element of the entire JSON-formatted data is the flattened JSON object because the outermost array structure of the JSON-formatted data is stripped.
> - You can use `json_root` to specify the root element of the JSON-formatted data.

#### StarRocks table column names different from JSON key names

##### Prepare a dataset

For example, the following JSON-formatted data exists in the topic `ordertest2` of the Kafka cluster.

```SQL
{"commodity_id": "1", "customer_name": "Mark Twain", "country": "US","pay_time": 1589191487,"price": 875}
{"commodity_id": "2", "customer_name": "Oscar Wilde", "country": "UK","pay_time": 1589191487,"price": 895}
{"commodity_id": "3", "customer_name": "Antoine de Saint-Exupéry","country": "France","pay_time": 1589191487,"price": 895}
```

##### Target database and table

Create a table named `example_tbl4` in the database `example_db` in the StarRocks cluster. The column `pay_dt` is a derived column whose values are generated by computing values of the key `pay_time` in the JSON-formatted data.

```SQL
CREATE TABLE example_db.example_tbl4 ( 
    `commodity_id` varchar(26) NULL, 
    `customer_name` varchar(26) NULL, 
    `country` varchar(26) NULL,
    `pay_time` bigint(20) NULL,  
    `pay_dt` date NULL, 
    `price` double SUM NULL) 
AGGREGATE KEY(`commodity_id`,`customer_name`,`country`,`pay_time`,`pay_dt`) 
DISTRIBUTED BY HASH(`commodity_id`); 
```

##### Routine Load job

You can use the matched mode for the Routine Load job. That is, you need to specify `jsonpaths` and `COLUMNS` parameters when creating the Routine Load job.

You need to specify the keys of the JSON-formatted data and arrange them in sequence in the `jsonpaths` parameter.

And since the values in the key `pay_time` of the JSON-formatted data need to be converted to the DATE type before the values are stored in the `pay_dt` column of the `example_tbl4` table, you need to specify the computation by using `pay_dt=from_unixtime(pay_time,'%Y%m%d')` in `COLUMNS`. The values of other keys in the JSON-formatted data can be directly mapped to the `example_tbl4` table.

```SQL
CREATE ROUTINE LOAD example_db.example_tbl4_ordertest2 ON example_tbl4
COLUMNS(commodity_id, customer_name, country, pay_time, pay_dt=from_unixtime(pay_time, '%Y%m%d'), price)
PROPERTIES
(
    "format" = "json",
    "jsonpaths" = "[\"$.commodity_id\",\"$.customer_name\",\"$.country\",\"$.pay_time\",\"$.price\"]"
 )
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest2"
);
```

> **Note**
>
> - If the outermost layer of the JSON data is an array structure, you need to set `"strip_outer_array"="true"` in the `PROPERTIES` to strip the outermost array structure. Additionally, when you need to specify `jsonpaths`, the root element of the entire JSON data is the flattened JSON object because the outermost array structure of the JSON data is stripped.
> - You can use `json_root` to specify the root element of the JSON-formatted data.

#### Specify the root element of the JSON-formatted data to be loaded

You need to use `json_root` to specify the root element of the JSON-formatted data to be loaded and the value must be a valid JsonPath expression.

##### Prepare a dataset

For example, the following JSON-formatted data exists in the topic `ordertest3` of the Kafka cluster. And the root element of the JSON-formatted data to be loaded is `$.RECORDS`.

```SQL
{"RECORDS":[{"commodity_id": "1", "customer_name": "Mark Twain", "country": "US","pay_time": 1589191487,"price": 875},{"commodity_id": "2", "customer_name": "Oscar Wilde", "country": "UK","pay_time": 1589191487,"price": 895},{"commodity_id": "3", "customer_name": "Antoine de Saint-Exupéry","country": "France","pay_time": 1589191487,"price": 895}]}
```

##### Target database and table

Create a table named `example_tbl3` in the database `example_db` in the StarRocks cluster.

```SQL
CREATE TABLE example_db.example_tbl3 ( 
    commodity_id varchar(26) NULL, 
    customer_name varchar(26) NULL, 
    country varchar(26) NULL, 
    pay_time bigint(20) NULL, 
    price double SUM NULL) 
AGGREGATE KEY(commodity_id,customer_name,country,pay_time) 
ENGINE=OLAP
DISTRIBUTED BY HASH(commodity_id); 
```

##### Routine Load job

You can set `"json_root" = "$.RECORDS"` in `PROPERTIES` to specify the root element of the JSON-formatted data to be loaded. Also, since the JSON-formatted data to be loaded is in an array structure, you must also set `"strip_outer_array" = "true"` to strip the outermost array structure.

```SQL
CREATE ROUTINE LOAD example_db.example_tbl3_ordertest3 ON example_tbl3
PROPERTIES
(
    "format" = "json",
    "json_root" = "$.RECORDS",
    "strip_outer_array" = "true"
 )
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest2"
);
```

### Load Avro-formatted data

Since v3.0.1, StarRocks supports loading Avro data by using Routine Load.

#### Avro schema is simple

Suppose the Avro schema is relatively simple, and you need to load all fields of the Avro data.

##### Prepare a dataset

###### Avro schema

1. Create the following Avro schema file `avro_schema1.avsc`:

      ```json
      {
          "type": "record",
          "name": "sensor_log",
          "fields" : [
              {"name": "id", "type": "long"},
              {"name": "name", "type": "string"},
              {"name": "checked", "type" : "boolean"},
              {"name": "data", "type": "double"},
              {"name": "sensor_type", "type": {"type": "enum", "name": "sensor_type_enum", "symbols" : ["TEMPERATURE", "HUMIDITY", "AIR-PRESSURE"]}}  
          ]
      }
      ```

2. Register the Avro schema in the [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html).

###### Avro data

Prepare the Avro data and send it to the Kafka topic `topic_1`.

##### Target database and table

According to the fields of Avro data, create a table `sensor_log1` in the target database `sensor` in the StarRocks cluster. The column names of the table must match the field names in the Avro data. For the data types mapping when Avro data is loaded into StarRocks, see [Data types mapping](#Data types mapping).

```SQL
CREATE TABLE sensor.sensor_log1 ( 
    `id` bigint NOT NULL COMMENT "sensor id",
    `name` varchar(26) NOT NULL COMMENT "sensor name", 
    `checked` boolean NOT NULL COMMENT "checked", 
    `data` double NULL COMMENT "sensor data", 
    `sensor_type` varchar(26) NOT NULL COMMENT "sensor type"
) 
ENGINE=OLAP 
DUPLICATE KEY (id) 
DISTRIBUTED BY HASH(`id`); 
```

##### Routine Load job

You can use the simple mode for the Routine Load job. That is, you do not need to specify the parameter `jsonpaths` when creating the Routine Load job. Execute the following statement to submit a Routine Load job named `sensor_log_load_job1` to consume the Avro messages in the Kafka topic `topic_1` and load the data into the table `sensor_log1` in the database `sensor`.

```sql
CREATE ROUTINE LOAD sensor.sensor_log_load_job1 ON sensor_log1  
PROPERTIES  
(  
  "format" = "avro"  
)  
FROM KAFKA  
(  
  "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>,...",
  "confluent.schema.registry.url" = "http://172.xx.xxx.xxx:8081",  
  "kafka_topic"= "topic_1",  
  "kafka_partitions" = "0,1,2,3,4,5",  
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"  
);
```

#### Avro schema contains a nested record-type field

Suppose the Avro schema contains a nested record-type field, and you need to load the subfield in a nested record-type field into StarRocks.

##### Prepare a dataset

###### Avro schema

1. Create the following Avro schema file `avro_schema2.avsc`. The outer Avro record includes five fields which are `id`, `name`, `checked`, `sensor_type`, and `data` in sequence. And the field `data` has a nested record `data_record`.

      ```JSON
      {
          "type": "record",
          "name": "sensor_log",
          "fields" : [
              {"name": "id", "type": "long"},
              {"name": "name", "type": "string"},
              {"name": "checked", "type" : "boolean"},
              {"name": "sensor_type", "type": {"type": "enum", "name": "sensor_type_enum", "symbols" : ["TEMPERATURE", "HUMIDITY", "AIR-PRESSURE"]}},
              {"name": "data", "type": 
                  {
                      "type": "record",
                      "name": "data_record",
                      "fields" : [
                          {"name": "data_x", "type" : "boolean"},
                          {"name": "data_y", "type": "long"}
                      ]
                  }
              }
          ]
      }
      ```

2. Register the Avro schema in the [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html).

###### Avro data

Prepare the Avro data and send it to the Kafka topic `topic_2`.

##### Target database and table

According to the fields of Avro data, create a table `sensor_log2` in the target database `sensor` in the StarRocks cluster.

Suppose that in addition to loading the fields `id`, `name`, `checked`, and `sensor_type` of the outer Record, you also need to load the subfield `data_y` in the nested Record `data_record`.

```sql
CREATE TABLE sensor.sensor_log2 ( 
    `id` bigint NOT NULL COMMENT "sensor id",
    `name` varchar(26) NOT NULL COMMENT "sensor name", 
    `checked` boolean NOT NULL COMMENT "checked", 
    `sensor_type` varchar(26) NOT NULL COMMENT "sensor type",
    `data_y` long NULL COMMENT "sensor data" 
) 
ENGINE=OLAP 
DUPLICATE KEY (id) 
DISTRIBUTED BY HASH(`id`); 
```

##### Routine Load job

Submit the load job, use `jsonpaths` to specify the fields of the Avro data that need to be loaded. Note that for the subfield `data_y` in the nested Record, you need to specify its `jsonpath` as `"$.data.data_y"`.

```sql
CREATE ROUTINE LOAD sensor.sensor_log_load_job2 ON sensor_log2  
PROPERTIES  
(  
  "format" = "avro",
  "jsonpaths" = "[\"$.id\",\"$.name\",\"$.checked\",\"$.sensor_type\",\"$.data.data_y\"]"
)  
FROM KAFKA  
(  
  "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>,...",
  "confluent.schema.registry.url" = "http://172.xx.xxx.xxx:8081",  
  "kafka_topic" = "topic_1",  
  "kafka_partitions" = "0,1,2,3,4,5",  
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"  
);
```

#### Avro schema contains a Union field

##### Prepare a dataset

Suppose the Avro schema contains a Union field, and you need to load the Union field into StarRocks.

###### Avro schema

1. Create the following Avro schema file `avro_schema3.avsc`. The outer Avro record includes five fields which are `id`, `name`, `checked`, `sensor_type`, and `data` in sequence. And the field `data` is of Union type and includes two elements, `null` and a nested record `data_record`.

      ```JSON
      {
          "type": "record",
          "name": "sensor_log",
          "fields" : [
              {"name": "id", "type": "long"},
              {"name": "name", "type": "string"},
              {"name": "checked", "type" : "boolean"},
              {"name": "sensor_type", "type": {"type": "enum", "name": "sensor_type_enum", "symbols" : ["TEMPERATURE", "HUMIDITY", "AIR-PRESSURE"]}},
              {"name": "data", "type": [null,
                    {
                        "type": "record",
                        "name": "data_record",
                        "fields" : [
                            {"name": "data_x", "type" : "boolean"},
                            {"name": "data_y", "type": "long"}
                        ]
                    }
                  ]
              }
          ]
      }
      ```

2. Register the Avro schema in the [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html).

###### Avro data

Prepare the Avro data and send it to the Kafka topic `topic_3`.

##### Target database and table

According to the fields of Avro data, create a table `sensor_log3` in the target database `sensor` in the StarRocks cluster.

Suppose that in addition to loading the fields `id`, `name`, `checked`, and `sensor_type` of the outer Record, you also need to load the field `data_y` of the element `data_record` in the Union type field `data`.

```sql
CREATE TABLE sensor.sensor_log3 ( 
    `id` bigint NOT NULL COMMENT "sensor id",
    `name` varchar(26) NOT NULL COMMENT "sensor name", 
    `checked` boolean NOT NULL COMMENT "checked", 
    `sensor_type` varchar(26) NOT NULL COMMENT "sensor type",
    `data_y` long NULL COMMENT "sensor data" 
) 
ENGINE=OLAP 
DUPLICATE KEY (id) 
DISTRIBUTED BY HASH(`id`); 
```

##### Routine Load job

Submit the load job, use `jsonpaths` to specify the fields that need to be loaded in the Avro data. Note that for the field `data_y`, you need to specify its `jsonpath` as `"$.data.data_y"`.

```sql
CREATE ROUTINE LOAD sensor.sensor_log_load_job3 ON sensor_log3  
PROPERTIES  
(  
  "format" = "avro",
  "jsonpaths" = "[\"$.id\",\"$.name\",\"$.checked\",\"$.sensor_type\",\"$.data.data_y\"]"
)  
FROM KAFKA  
(  
  "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>,...",
  "confluent.schema.registry.url" = "http://172.xx.xxx.xxx:8081",  
  "kafka_topic" = "topic_1",  
  "kafka_partitions" = "0,1,2,3,4,5",  
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"  
);
```

When the value for the Union type field `data` is `null`, the value loaded into the column `data_y` in the StarRocks table is `null`. When the value for the Union type field `data` is a data record, the value loaded into the column `data_y` is of Long type.
