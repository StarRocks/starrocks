---
displayed_sidebar: docs
---

# ALTER ROUTINE LOAD

import RoutineLoadPrivNote from '../../../../_assets/commonMarkdown/RoutineLoadPrivNote.md'

## 説明

`PAUSED` 状態にある Routine Load ジョブを変更します。PAUSE ROUTINE LOAD を実行して Routine Load ジョブを一時停止できます。

Routine Load ジョブを正常に変更した後、以下の操作が可能です:

- [SHOW ROUTINE LOAD](SHOW_ROUTINE_LOAD.md) を使用して、Routine Load ジョブに行われた変更を確認します。
- [RESUME ROUTINE LOAD](RESUME_ROUTINE_LOAD.md) を使用して、Routine Load ジョブを再開します。

<RoutineLoadPrivNote />

## 構文

```SQL
ALTER ROUTINE LOAD FOR [<db_name>.]<job_name>
[load_properties]
[job_properties]
FROM data_source
[data_source_properties]
```

## パラメータ

- **`[<db_name>.]<job_name>`**
  - **`db_name`**: 任意。StarRocks データベースの名前。
  - **`job_name`:** 必須。変更する Routine Load ジョブの名前。
- **`load_properties`**

   ロードするソースデータのプロパティ。構文は以下の通りです:

   ```SQL
   [COLUMNS TERMINATED BY '<column_separator>'],
   [ROWS TERMINATED BY '<row_separator>'],
   [COLUMNS ([<column_name> [, ...] ] [, column_assignment [, ...] ] )],
   [WHERE <expr>],
   [PARTITION ([ <partition_name> [, ...] ])]
   [TEMPORARY PARTITION (<temporary_partition1_name>[,<temporary_partition2_name>,...])]
   ```

   詳細なパラメータの説明については、[CREATE ROUTINE LOAD](CREATE_ROUTINE_LOAD.md#load_properties) を参照してください。

- **`job_properties`**

  ロードジョブのプロパティ。構文は以下の通りです:

  ```SQL
  PROPERTIES ("<key1>" = "<value1>"[, "<key2>" = "<value2>" ...])
  ```

  以下のパラメータのみ変更可能です:

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

  詳細なパラメータの説明については、[CREATE ROUTINE LOAD](CREATE_ROUTINE_LOAD.md#job_properties) を参照してください。

- **`data_source`** **および** **`data_source_properties`**

  - **`data_source`**

    必須。ロードしたいデータのソース。有効な値: `KAFKA`。

  - **`data_source_properties`**

    データソースのプロパティ。現在、以下のプロパティのみ変更可能です:
    - `kafka_partitions` および `kafka_offsets`: StarRocks は既に消費された Kafka パーティションのオフセットの変更のみをサポートし、新しい Kafka パーティションの追加はサポートしていません。
    - `property.*`: Kafka のデータソース用のカスタムパラメータ、例: `property.kafka_default_offsets`。

## 例

1. 次の例では、ロードジョブのプロパティ `desired_concurrent_number` の値を `5` に増やし、ロードタスクの並行性を高めます。

   ```SQL
   ALTER ROUTINE LOAD FOR example_tbl_ordertest
   PROPERTIES
   (
   "desired_concurrent_number" = "5"
   );
   ```

2. 次の例では、ロードジョブのプロパティとデータソース情報を同時に変更します。

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

3. 次の例では、フィルタリング条件と StarRocks パーティションを同時に変更してデータをロードします。

   ```SQL
   ALTER ROUTINE LOAD FOR example_tbl_ordertest
   WHERE pay_dt < 2023-06-31
   PARTITION (p202306);
   ```