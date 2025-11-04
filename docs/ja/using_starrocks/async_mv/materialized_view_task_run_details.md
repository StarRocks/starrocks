# マテリアライズドビュータスク実行の理解

マテリアライズドビューのリフレッシュタスク実行の情報は、マテリアライズドビューのリフレッシュ動作を理解し、問題をトラブルシュートし、パフォーマンスを監視するのに役立ちます。

## 概要

システムがマテリアライズドビューをリフレッシュするとき、リフレッシュ操作に関する詳細情報を含むタスク実行が作成されます。この情報は `MVTaskRunExtraMessage` オブジェクトに格納され、システムビュー [`information_schema.task_runs`](../../sql-reference/information_schema/task_runs.md) の `EXTRA_MESSAGE` フィールドをクエリすることで取得できます。

## タスク実行の追加メッセージ

このセクションでは、`MVTaskRunExtraMessage` に提供されるフィールドについて説明します。

#### `forceRefresh`

- **Type**: Boolean
- **Description**: 通常のリフレッシュ条件をバイパスする強制リフレッシュかどうかを示します。`REFRESH MATERIALIZED VIEW ... FORCE` を実行して手動で強制フルリフレッシュをトリガーした場合、`true` が返されます。

#### `partitionStart`

- **Type**: String
- **Description**: このリフレッシュ操作の開始パーティション境界です。リフレッシュするパーティション範囲の下限を定義します。
- **Format**: パーティションキーの値（例: 日付ベースのパーティションの場合は `"2024-01-01"`）
- **Example**: 2024年1月以降のデータがリフレッシュされる場合、このフィールドは `"2024-01-01"` になります。

#### `partitionEnd`

- **Type**: String
- **Description**: このリフレッシュ操作の終了パーティション境界です。リフレッシュするパーティション範囲の上限を定義します。
- **Format**: パーティションキーの値（例: 日付ベースのパーティションの場合は `"2024-01-31"`）
- **Example**: 2024年1月までのデータがリフレッシュされる場合、このフィールドは `"2024-01-31"` になります。

#### `mvPartitionsToRefresh`

- **Type**: Set of Strings
- **Description**: タスク実行でリフレッシュされる予定のマテリアライズドビューパーティションのリストです。この項目は、どのマテリアライズドビューパーティションが更新されるかを追跡するのに役立ちます。
- **Size Limit**: 実際の数がこの値を超える場合、過剰なメタデータストレージを防ぐために、システムはパーティションの数を `max_mv_task_run_meta_message_values_length`（デフォルト: 100）に自動的に切り詰めます。
- **Example**: `["p20240101", "p20240102", "p20240103"]`
- **Note**: これはマテリアライズドビュー自身のパーティションを表し、ベーステーブルのパーティションではありません。

#### `refBasePartitionsToRefreshMap`

- **Type**: Map of String to Set of Strings
- **Description**: リフレッシュすべきパーティションのセットへの参照ベーステーブルのマッピングです。
- **Usage**: この値はマテリアライズドビュープランスケジューラ段階で設定されます。どのベーステーブルのパーティションをスキャンする必要があるかを追跡するのに使用できます。
- **Format**: `{tableName -> Set<partitionName>}`
- **Size Limit**: 実際の数がこの値を超える場合、過剰なメタデータストレージを防ぐために、システムはパーティションの数を `max_mv_task_run_meta_message_values_length`（デフォルト: 100）に自動的に切り詰めます。
- **Example**:

  ```json
  {
    "orders": ["p20240101", "p20240102"],
    "customers": ["p202401"]
  }
  ```

- **Note**: これは最適化前の**計画された**パーティションセットです。

#### `basePartitionsToRefreshMap`

- **Type**: Map of String to Set of Strings
- **Description**: 実行中にリフレッシュされたパーティションのセットへの参照ベーステーブルのマッピングです。
- **Usage**: この値はマテリアライズドビューバージョンマップがコミットされた後に設定されます。これはオプティマイザによって使用された実際のパーティションを反映します。
- **Format**: `{tableName -> Set<partitionName>}`
- **Size Limit**: 実際の数がこの値を超える場合、過剰なメタデータストレージを防ぐために、システムはパーティションの数を `max_mv_task_run_meta_message_values_length`（デフォルト: 100）に自動的に切り詰めます。
- **Example**: 

  ```json
  {
    "orders": ["p20240101", "p20240102"],
    "line_items": ["p20240101_batch1", "p20240101_batch2"]
  }
  ```

- **Note**: これはクエリ最適化と実行後の**実際の**パーティションセットです。

:::note

**refBasePartitionsToRefreshMap と basePartitionsToRefreshMap の違い:**
- `refBasePartitionsToRefreshMap`: 最適化前の計画されたパーティション（通常はメイン参照テーブル用）
- `basePartitionsToRefreshMap`: 最適化後の実際のパーティション（すべてのテーブルと最適化されたパーティションセットを含む）

:::

#### `nextPartitionStart`

- **Type**: String
- **Description**: 次のインクリメンタルリフレッシュの開始パーティション境界です。この値は、リソース制限や大規模データボリュームのためにリフレッシュ操作が複数のタスク実行に分割される場合、次のタスク実行でリフレッシュされるパーティション範囲の下限を定義します。
- **Example**: 現在のタスク実行が `"2024-01-15"` までのデータをリフレッシュする場合、このフィールドの値は `"2024-01-16"` になるかもしれません。

#### `nextPartitionEnd`

- **Type**: String
- **Description**: 次のインクリメンタルリフレッシュの終了パーティション境界です。この値は、リソース制限や大規模データボリュームのためにリフレッシュ操作が複数のタスク実行に分割される場合、次のタスク実行でリフレッシュされるパーティション範囲の上限を定義します。
- **Example**: 次の処理対象のパーティションバッチに対して `"2024-01-31"`。

#### `nextPartitionValues`

- **Type**: String
- **Description**: 次のリフレッシュ用にシリアル化されたパーティション値（リストパーティション化や複雑なパーティションスキームで使用）。単純な開始/終了範囲が不十分な場合に特定のパーティション値を格納します。
- **Example**: マルチカラムリストパーティションに対して `"('US', 'ACTIVE'), ('UK', 'ACTIVE')"`。

#### `processStartTime`

- **Type**: Integer (timestamp in milliseconds)
- **Description**: タスク実行が実際に処理を開始したタイムスタンプ（待機時間は除外）。`Processing Time = Finish Time - Process Start Time` の式を使用して実際の処理時間を計算するのに使用できます（キュー待機時間は除外）。
- **Example**: `1704067200000`（2024-01-01 00:00:00 UTC）

#### `executeOption`

- **Type**: ExecuteOption object
- **Description**: タスク実行の設定オプション。
- **Default**: `Priority` = `LOWEST`, `isMergeRedundant` = `false`
- **Fields**:
  - `priority`: タスク実行の優先度（`Constants.TaskRunPriority` の値）
    - `HIGHEST`: 0
    - `HIGH`: 32
    - `NORMAL`: 64
    - `LOW`: 96
    - `LOWEST`: 127
  - `isMergeRedundant`: 冗長なリフレッシュ操作をマージするかどうか。
  - `properties`: 追加の実行プロパティ（`Map<String, String>` の形式）。

#### `planBuilderMessage`

- **Type**: Map of String to String
- **Description**: クエリプランビルダーからの診断メッセージとメタデータ。クエリプランニング、最適化の決定、および潜在的な問題に関する情報を含みます。
- **Size Limit**: 実際の数がこの値を超える場合、過剰なメタデータストレージを防ぐために、システムはパーティションの数を `max_mv_task_run_meta_message_values_length`（デフォルト: 100）に自動的に切り詰めます。

#### `refreshMode`

- **Type**: String
- **Description**: このタスク実行のリフレッシュモード。マテリアライズドビューがどのようにリフレッシュされたかを示します。
- **Valid Values**:
  - `"COMPLETE"`: すべてのパーティションのフルリフレッシュ
  - `"PARTIAL"`: 特定のパーティションのインクリメンタルリフレッシュ
  - `"FORCE"`: 古さチェックをバイパスする強制リフレッシュ
  - `""` (empty): デフォルトまたは未指定

#### `adaptivePartitionRefreshNumber`

- **Type**: Integer
- **Description**: アダプティブパーティションリフレッシュが使用される場合に、各イテレーションでリフレッシュすべきパーティションの数。この値は、システムリソースとデータボリュームに基づいて自動的に決定され、リフレッシュパフォーマンスを最適化します。
- **Default**: `-1`（アダプティブリフレッシュが設定または使用されていないことを示します）
- **Example**: `10`（一度に10パーティションをリフレッシュすることを示します）

## タスク実行の詳細をクエリする

システムビュー `information_schema.task_runs` を通じてマテリアライズドビュタスク実行情報をクエリできます。

```sql
SELECT 
    TASK_NAME,
    CREATE_TIME,
    FINISH_TIME,
    STATE,
    EXTRA_MESSAGE
FROM information_schema.task_runs
WHERE TASK_NAME LIKE 'mv-%'
ORDER BY CREATE_TIME DESC
LIMIT 10;
```

`EXTRA_MESSAGE` 列には `MVTaskRunExtraMessage` の JSON 表現が含まれています。

JSON 文字列 `EXTRA_MESSAGE` をさらに解析して、読みやすくすることができます。

```sql
SELECT 
    TASK_NAME,
    CREATE_TIME,
    get_json_string(EXTRA_MESSAGE, '$.refreshMode') AS refresh_mode,
    get_json_string(EXTRA_MESSAGE, '$.forceRefresh') AS force_refresh,
    get_json_string(EXTRA_MESSAGE, '$.mvPartitionsToRefresh') AS mv_partitions,
    get_json_int(EXTRA_MESSAGE, '$.processStartTime') AS process_start_ms,
    get_json_int(EXTRA_MESSAGE, '$.adaptivePartitionRefreshNumber') AS adaptive_batch_size
FROM information_schema.task_runs
WHERE TASK_NAME = 'mv-12345'
ORDER BY CREATE_TIME DESC;
```

## リフレッシュパフォーマンスの理解

### 処理時間の計算

```sql
SELECT 
    TASK_NAME,
    FINISH_TIME,
    get_json_bigint(EXTRA_MESSAGE, '$.processStartTime') AS process_start_time,
    (unix_timestamp(FINISH_TIME) * 1000 - 
     get_json_bigint(EXTRA_MESSAGE, '$.processStartTime')) / 1000 AS processing_seconds
FROM information_schema.task_runs
WHERE TASK_NAME LIKE 'mv-%' AND STATE = 'SUCCESS';
```

### パーティションリフレッシュパターンの分析

```sql
SELECT 
    TASK_NAME,
    CREATE_TIME,
    get_json_string(EXTRA_MESSAGE, '$.partitionStart') AS start_partition,
    get_json_string(EXTRA_MESSAGE, '$.partitionEnd') AS end_partition,
    get_json_string(EXTRA_MESSAGE, '$.nextPartitionStart') AS next_start,
    get_json_string(EXTRA_MESSAGE, '$.nextPartitionEnd') AS next_end
FROM information_schema.task_runs
WHERE TASK_NAME = 'mv-12345'
ORDER BY CREATE_TIME DESC;
```

## 設定項目

### max_mv_task_run_meta_message_values_length

- **Type**: Integer
- **Default**: 100
- **Scope**: FE configuration
- **Description**: 過剰なメタデータの増加を防ぐために、セットまたは MAP フィールドに格納するアイテムの最大数。`mvPartitionsToRefresh`、`refBasePartitionsToRefreshMap`、`basePartitionsToRefreshMap`、および `planBuilderMessage` のサイズを制限します。

## ベストプラクティス

### リフレッシュパフォーマンスの監視

- `processStartTime` と実際の終了時間を比較して、キューイングの問題を特定します。
- `adaptivePartitionRefreshNumber` を使用してバッチサイズを最適化します。

### 失敗したリフレッシュのデバッグ

- `planBuilderMessage` を確認してオプティマイザの問題をチェックします。
- `refBasePartitionsToRefreshMap` と `basePartitionsToRefreshMap` を比較してパーティションプルーニングの問題を確認します。

### インクリメンタルリフレッシュの最適化

- `nextPartitionStart` と `nextPartitionEnd` を監視して、マルチイテレーションリフレッシュパターンを理解します。
- リフレッシュが頻繁に複数の実行にまたがる場合は、パーティショングラニュラリティを調整します。

### パーティションカバレッジの理解

- `mvPartitionsToRefresh` と `basePartitionsToRefreshMap` を比較して、マテリアライズドビューとベーステーブルのパーティションマッピングを確認します。
- ベーステーブルのパーティションが予想されるリフレッシュ範囲と一致しているかどうかを確認します。

## トラブルシューティング

### 問題: リフレッシュに時間がかかりすぎる

**チェック:**

1. `processStartTime` - 作成時間からの大きな差は、タスク実行がキューにあったことを示します。
2. `basePartitionsToRefreshMap` - 大きな値は、スキャンされるパーティションが多すぎることを示します。
3. `adaptivePartitionRefreshNumber` - ワークロードを調整する必要があるかもしれません。

### 問題: 予期しないパーティションがリフレッシュされた

**チェック:**

1. `forceRefresh` - `true` が返された場合、強制フルリフレッシュが実行されたことを示します。
2. `refBasePartitionsToRefreshMap` - 計画されたパーティションを示します。
3. `basePartitionsToRefreshMap` - 最適化後の実際のパーティションを示します。
4. 上記の2つのマップを比較して、オプティマイザがプランを変更したかどうかを確認します。

### 問題: リフレッシュが複数のイテレーションで停滞する

**チェック:**

1. `nextPartitionStart` と `nextPartitionEnd` - 未完了のリフレッシュ状態を示します。
2. `adaptivePartitionRefreshNumber` - ワークロードを調整する必要があるかもしれません。
3. バッチサイズを増やすか、パーティショングラニュラリティを減らすことを検討してください。

## 参照

- [CREATE MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md)
- [REFRESH MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/REFRESH_MATERIALIZED_VIEW.md)
- [Information Schema.task_runs](../../sql-reference/information_schema/task_runs.md)
- [Materialized View Management](./Materialized_view.md)