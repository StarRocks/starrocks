---
displayed_sidebar: docs
---

# ALTER ROUTINE LOAD

import RoutineLoadPrivNote from '../../../../_assets/commonMarkdown/RoutineLoadPrivNote.md'

## Description

Alters a Routine Load job that is in the `PAUSED` state. You can execute PAUSE ROUTINE LOAD to pause a Routine Load job.

After successfully altering a Routine Load job, you can:

- Check the modifications made to the Routine Load job by using [SHOW ROUTINE LOAD](SHOW_ROUTINE_LOAD.md).
- Resume the Routine Load job by using [RESUME ROUTINE LOAD](RESUME_ROUTINE_LOAD.md).

<RoutineLoadPrivNote />

## Syntax

```SQL
ALTER ROUTINE LOAD FOR [<db_name>.]<job_name>
[load_properties]
[job_properties]
FROM data_source
[data_source_properties]
```

## Parameters

- **`[<db_name>.]<job_name>`**
  - **`db_name`**: Optional. The name of the StarRocks database.
  - **`job_name`:** Required. The name of the Routine Load job to be altered.
- **`load_properties`**

   The properties of the source data to be loaded. The syntax is as follows:

   ```SQL
   [COLUMNS TERMINATED BY '<column_separator>'],
   [ROWS TERMINATED BY '<row_separator>'],
   [COLUMNS ([<column_name> [, ...] ] [, column_assignment [, ...] ] )],
   [WHERE <expr>],
   [PARTITION ([ <partition_name> [, ...] ])]
   [TEMPORARY PARTITION (<temporary_partition1_name>[,<temporary_partition2_name>,...])]
   ```

   For detailed parameter descriptions, see [CREATE ROUTINE LOAD](CREATE_ROUTINE_LOAD.md#load_properties).

- **`job_properties`**

  The properties of the load job. The syntax is as follows:

  ```SQL
  PROPERTIES ("<key1>" = "<value1>"[, "<key2>" = "<value2>" ...])
  ```

  Only the following parameters can be altered:

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

  For detailed parameter descriptions, see [CREATE ROUTINE LOAD](CREATE_ROUTINE_LOAD.md#job_properties).

- **`data_source`** **and** **`data_source_properties`**

  - **`data_source`**

    Required. The source of the data you want to load. Valid value: `KAFKA`.

  - **`data_source_properties`**

    The properties of the data source. Currently, only the following properties can be altered:
    - `kafka_partitions` and `kafka_offsets`: Note that StarRocks only supports modifying the offset of Kafka partitions that have already been consumed but does not support adding new Kafka partitions.
    - `property.*`: Custom parameters for the data source Kafka, such as `property.kafka_default_offsets`.

## Examples

1. The following example increases the value of the property `desired_concurrent_number` of the load job to `5` in order to increase the parallelism of load tasks. For details on task parallelism, see [how to improve load performance](../../../../faq/loading/Routine_load_faq.md#how-can-i-improve-loading-performance).

   ```SQL
   ALTER ROUTINE LOAD FOR example_tbl_ordertest
   PROPERTIES
   (
   "desired_concurrent_number" = "5"
   );
   ```

2. The following example alters the load job's properties and data source information at the same time.

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

3. The following example alters the filtering condition and the StarRocks partitions that data is loaded into at the same time.

   ```SQL
   ALTER ROUTINE LOAD FOR example_tbl_ordertest
   WHERE pay_dt < 2023-06-31
   PARTITION (p202306);
   ```
