---
keywords: ['xiugai'] 
displayed_sidebar: docs
description: "修改处于 PAUSED 状态的 Routine Load 导入作业。"
---

# ALTER ROUTINE LOAD

import RoutineLoadPrivNote from '../../../../_assets/commonMarkdown/RoutineLoadPrivNote.mdx'

## 功能

该语法用于修改 Routine Load 导入作业。注意，只能修改处于 `PAUSED` 状态的作业。您可以通过 [PAUSE ROUTINE LOAD](PAUSE_ROUTINE_LOAD.md) 暂停 Routine Load 导入作业。

修改成功后，您可以：

- 通过 [SHOW ROUTINE LOAD](SHOW_ROUTINE_LOAD.md) 检查修改后的作业详情。
- 通过 [RESUME ROUTINE LOAD](RESUME_ROUTINE_LOAD.md) 重启该导入作业。

<RoutineLoadPrivNote />

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

    详细的参数介绍，请参见 [CREATE ROUTINE LOAD](CREATE_ROUTINE_LOAD.md#load_properties)。

    当 ALTER 修改 `COLUMNS` 或 `WHERE` 时，该语句会使用创建作业时保存的 `sql_mode`（而不是当前会话的 `sql_mode`）重新解析，因为 Follower FE 和 FE 重启时都是这样读取它的。如果某个表达式在该模式下含义不同（例如 `a || b` 默认表示 `OR`，而在 `PIPES_AS_CONCAT` 下表示字符串拼接），ALTER 会被拒绝。请将会话的 `sql_mode` 设置为与作业相同的值，或改用含义明确的表达式（`concat(a, b)`）。如果作业的 `COLUMNS` 或 `WHERE` 定义无法写回持久化语句并解析为相同的表达式，ALTER 同样会被拒绝，错误信息中会给出该表达式。仅修改作业属性或数据源属性的 ALTER 不会改变现有的导入定义。

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

  详细的属性介绍，请参见 [CREATE ROUTINE LOAD](CREATE_ROUTINE_LOAD.md#job_properties)。

- **`data_source`** 和 **`data_source_properties`**

  - **`data_source`**

    必填，指定数据源，目前仅支持取值为 `KAFKA`。

  - **`data_source_properties`**

    数据源的相关属性。目前支持修改：

    - `kafka_partitions` 和 `kafka_offsets`：需要注意的是，默认仅支持修改当前已经消费的 Kafka partition 的 offset，不支持新增 Kafka partition。如果同一语句中将 `property.kafka_partition_discovery` 设置为 `true`，则所列分区改为按 Kafka 主题实际存在的分区进行校验，因此也可以为固定分区列表之外的分区指定 offset。
    - `property.*`： 自定义的数据源 Kafka 相关参数，例如 `property.kafka_default_offsets`。需要注意的是，`property.kafka_partition_discovery` 只能设置为 `true`，用于解除创建作业时固定的分区列表并重新开启分区自动发现。不支持改回 `false`，如需重新固定分区列表，请重建作业。

## 示例

1. 将导入作业的属性 `desired_concurrent_number` 调大至 `5`，以提高导入任务的并行度。任务的并行度的详细说明，参见[如何提高导入性能](../../../../faq/data_migration/loading/Routine_load_faq.md#1-如何提高导入性能)。

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
