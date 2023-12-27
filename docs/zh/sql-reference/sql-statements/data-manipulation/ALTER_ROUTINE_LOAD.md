---
displayed_sidebar: "Chinese"
---

# ALTER ROUTINE LOAD

## 功能

该语法用于修改 Routine Load 导入作业。注意，只能修改处于 `PAUSED` 状态的作业。您可以通过 [PAUSE ROUTINE LOAD](./PAUSE_ROUTINE_LOAD.md) 暂停 Routine Load 导入作业。

修改成功后，您可以：

- 通过 [SHOW ROUTINE LOAD](./SHOW_ROUTINE_LOAD.md) 检查修改后的作业详情。
- 通过 [RESUME ROUTINE LOAD](./RESUME_ROUTINE_LOAD.md) 重启该导入作业。

## **语法**

```SQL
ALTER ROUTINE LOAD FOR [db_name.]<job_name>
[load_properties]
[job_properties]
FROM data_source
[data_source_properties]
```

## 参数说明

- **`[<db_name>.]<job_name>`**

    指定要修改的作业名称。

- **`load_properties`**

    待导入的源数据信息。语法如下：

    ```SQL
    [COLUMNS TERMINATED BY '<column_separator>'],
    [ROWS TERMINATED BY '<row_separator>'],
    [COLUMNS ([<column_name> [, ...] ] [, column_assignment [, ...] ] )],
    [WHERE <expr>],
    [PARTITION ([ <partition_name> [, ...] ])]
    [TEMPORARY PARTITION (<temporary_partition1_name>[,<temporary_partition2_name>,...])]
    ```

    详细的参数介绍，请参见 [CREATE ROUTINE LOAD](./CREATE_ROUTINE_LOAD.md#load_properties)。

- **`job_properties`**

  导入作业的属性。语法如下：

  ```SQL
  PROPERTIES ("<key1>" = "<value1>"[, "<key2>" = "<value2>" ...])
  ```

  目前仅支持修改如下属性：

  - `desired_concurrent_number`

  - `max_error_number`

  - `max_batch_interval`

  - `max_batch_rows`

  - `max_batch_size`

  - `jsonpaths`

  - `json_root`

  - `strip_outer_array`

  - `strict_mode`

  - `timezone`

  详细的属性介绍，请参见 [CREATE ROUTINE LOAD](./CREATE_ROUTINE_LOAD.md#job_properties)。

- **`data_source`** 和 **`data_source_properties`**

  - **`data_source`**

    必填，指定数据源，目前仅支持取值为 `KAFKA`。

  - **`data_source_properties`**

    数据源的相关属性。目前支持修改：

    - `kafka_partitions` 和 `kafka_offsets`：需要注意的是，仅支持修改当前已经消费的 Kafka partition 的 offset，不支持新增 Kafka partition。
    - `property.*`： 自定义的数据源 Kafka 相关参数，例如 `property.kafka_default_offsets`。

## 示例

1. 将导入作业的属性 `desired_concurrent_number` 调大至 `5`，以提高导入任务的并行度。任务的并行度的详细说明，参见[如何提高导入性能](../../../faq/loading/Routine_load_faq.md#1-如何提高导入性能)。

    ```SQL
    ALTER ROUTINE LOAD FOR example_tbl_ordertest
    PROPERTIES
    (
        "desired_concurrent_number" = "5"
    );
    ```

2. 同时修改导入作业的属性和数据源信息。

    ```SQL
    ALTER ROUTINE LOAD FOR example_tbl_ordertest
    PROPERTIES
    (
        "desired_concurrent_number" = "5"
    )
    FROM KAFKA
    (
        "kafka_partitions" = "0, 1, 2",
        "kafka_offsets" = "100, 200, 100",
        "property.group.id" = "new_group"
    );
    ```

3. 同时修改过滤条件和导入的目标 StarRocks 分区。

    ```SQL
    ALTER ROUTINE LOAD FOR example_tbl_ordertest
    WHERE pay_dt < 2023-06-31
    PARTITION (p202306);
    ```
