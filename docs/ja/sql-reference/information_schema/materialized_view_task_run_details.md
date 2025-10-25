# マテリアライズドビュータスク実行の詳細

このドキュメントでは、マテリアライズドビュー（MV）リフレッシュタスク実行の内部詳細について説明します。これは、MVリフレッシュの動作を理解し、問題をトラブルシューティングし、MVパフォーマンスを監視するのに役立ちます。

## 概要

StarRocksがマテリアライズドビューをリフレッシュすると、リフレッシュ操作に関する詳細情報を含むタスク実行が作成されます。この情報は `MVTaskRunExtraMessage` オブジェクトに保存され、システムビューを通じてクエリできます。

## タスク実行の追加メッセージフィールド

#### forceRefresh
- **タイプ**: Boolean
- **説明**: 通常のリフレッシュ条件をバイパスする強制リフレッシュかどうかを示します
- **使用方法**: `REFRESH MATERIALIZED VIEW ... FORCE` を使用して完全リフレッシュを手動でトリガーすると `true` に設定されます
- **デフォルト**: `false`

#### partitionStart
- **タイプ**: String
- **説明**: このリフレッシュ操作の開始パーティション境界
- **使用方法**: リフレッシュするパーティション範囲の下限を定義します
- **形式**: パーティションキー値（例：日付パーティションの場合は `"2024-01-01"`）
- **例**: 2024年1月以降のデータをリフレッシュする場合、これは `"2024-01-01"` になります

#### partitionEnd
- **タイプ**: String
- **説明**: このリフレッシュ操作の終了パーティション境界
- **使用方法**: リフレッシュするパーティション範囲の上限を定義します
- **形式**: パーティションキー値（例：日付パーティションの場合は `"2024-01-31"`）
- **例**: 2024年1月までのデータをリフレッシュする場合、これは `"2024-01-31"` になります

#### mvPartitionsToRefresh
- **タイプ**: 文字列のセット
- **説明**: このタスク実行でリフレッシュされる予定のマテリアライズドビューパーティションのセット
- **使用方法**: どのMVパーティションが更新されるかを追跡します
- **サイズ制限**: 過度なメタデータストレージを防ぐため、`max_mv_task_run_meta_message_values_length`（デフォルト：100）に自動的に切り捨てられます
- **例**: `["p20240101", "p20240102", "p20240103"]`
- **注意**: これは基本テーブルのパーティションではなく、MVの自己パーティションを表します

#### refBasePartitionsToRefreshMap
- **タイプ**: 文字列から文字列のセットへのマップ
- **説明**: 参照基本テーブル名を、リフレッシュする必要があるそのパーティションのセットにマップします
- **使用方法**: MVプランスケジューラステージ中に設定され、スキャンする必要がある基本テーブルパーティションを追跡します
- **形式**: `{tableName -> Set<partitionName>}`
- **サイズ制限**: `max_mv_task_run_meta_message_values_length` に自動的に切り捨てられます
- **例**: 
  ```json
  {
    "orders": ["p20240101", "p20240102"],
    "customers": ["p202401"]
  }
  ```
- **注意**: これは最適化前の**計画された**パーティションセットです

#### basePartitionsToRefreshMap
- **タイプ**: 文字列から文字列のセットへのマップ
- **説明**: すべての基本テーブル名を、実行中に実際にリフレッシュされたパーティションのセットにマップします
- **使用方法**: MVバージョンマップがコミットされた後に設定され、オプティマイザーが使用する実際のパーティションを反映します
- **形式**: `{tableName -> Set<partitionName>}`
- **サイズ制限**: `max_mv_task_run_meta_message_values_length` に自動的に切り捨てられます
- **例**: 
  ```json
  {
    "orders": ["p20240101", "p20240102"],
    "line_items": ["p20240101_batch1", "p20240101_batch2"]
  }
  ```
- **注意**: これはクエリ最適化と実行後の**実際の**パーティションセットです

**refBasePartitionsToRefreshMap と basePartitionsToRefreshMap の違い:**
- `refBasePartitionsToRefreshMap`: 最適化前の計画されたパーティション（通常はメイン参照テーブル用）
- `basePartitionsToRefreshMap`: 最適化後の実際のパーティション（すべてのテーブルと最適化されたパーティションセットを含む）

#### nextPartitionStart
- **タイプ**: String
- **説明**: 次の増分リフレッシュの開始パーティション境界
- **使用方法**: リソース制限または大量のデータボリュームのために、リフレッシュ操作が複数のタスク実行に分割される場合に使用されます
- **例**: 現在の実行が `"2024-01-15"` までリフレッシュした場合、これは `"2024-01-16"` になる可能性があります

#### nextPartitionEnd
- **タイプ**: String
- **説明**: 次の増分リフレッシュの終了パーティション境界
- **使用方法**: `nextPartitionStart` とペアで、次のリフレッシュイテレーションの範囲を定義します
- **例**: 処理する次のパーティションバッチの場合は `"2024-01-31"`

#### nextPartitionValues
- **タイプ**: String
- **説明**: 次のリフレッシュ用のシリアル化されたパーティション値（リストパーティショニングまたは複雑なパーティションスキームに使用）
- **使用方法**: 単純な開始/終了範囲では不十分な場合に、特定のパーティション値を保存します
- **例**: 複数列リストパーティションの場合は `"('US', 'ACTIVE'), ('UK', 'ACTIVE')"`

#### processStartTime
- **タイプ**: Long（ミリ秒単位のタイムスタンプ）
- **説明**: タスク実行が実際に処理を開始したタイムスタンプ（保留時間を除く）
- **使用方法**: 実際の処理時間を計算するために使用されます：`finishTime - processStartTime`
- **式**: `処理時間 = 終了時間 - 処理開始時間`（キュー待機時間を除く）
- **例**: `1704067200000`（2024-01-01 00:00:00 UTC）

#### executeOption
- **タイプ**: ExecuteOption オブジェクト
- **説明**: タスク実行の設定オプション
- **デフォルト**: Priority = LOWEST, isMergeRedundant = false
- **フィールド**:
  - `priority`: タスク実行優先度（`Constants.TaskRunPriority` からの値）
    - `HIGHEST`: 0
    - `HIGH`: 32
    - `NORMAL`: 64
    - `LOW`: 96
    - `LOWEST`: 127
  - `isMergeRedundant`: 冗長なリフレッシュ操作をマージするかどうか
  - `properties`: 追加の実行プロパティ（Map<String, String>）

#### planBuilderMessage
- **タイプ**: 文字列から文字列へのマップ
- **説明**: クエリプランビルダーからの診断メッセージとメタデータ
- **使用方法**: クエリプランニング、最適化決定、潜在的な問題に関する情報が含まれます
- **サイズ制限**: `max_mv_task_run_meta_message_values_length` に自動的に切り捨てられます

#### refreshMode
- **タイプ**: String
- **説明**: このタスク実行で使用されたリフレッシュモード
- **使用方法**: MVがどのようにリフレッシュされたかを示します
- **可能な値**:
  - `"COMPLETE"`: すべてのパーティションの完全リフレッシュ
  - `"PARTIAL"`: 特定のパーティションの増分リフレッシュ
  - `"FORCE"`: 古さチェックをバイパスする強制リフレッシュ
  - `""`（空）: デフォルトまたは未指定モード

#### adaptivePartitionRefreshNumber
- **タイプ**: Integer
- **説明**: アダプティブパーティションリフレッシュを使用する場合に、各イテレーションでリフレッシュする必要があるパーティションの数
- **使用方法**: システムリソースとデータボリュームに基づいて自動的に決定され、リフレッシュパフォーマンスを最適化します
- **デフォルト**: -1（設定されていないか、アダプティブリフレッシュを使用していない）
- **例**: `10` は一度に10個のパーティションをリフレッシュすることを意味します

## タスク実行詳細のクエリ

システムビューを通じてマテリアライズドビュータスク実行情報をクエリできます：

### information_schema.task_runs を使用

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

### 追加メッセージ JSON の解析

`EXTRA_MESSAGE` 列には `MVTaskRunExtraMessage` のJSON表現が含まれます：

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

## 設定パラメータ

### max_mv_task_run_meta_message_values_length

- **タイプ**: Integer
- **デフォルト**: 100
- **説明**: 過度なメタデータ増加を防ぐために、set/map フィールドに保存する項目の最大数
- **スコープ**: FE 設定パラメータ
- **使用方法**: 以下のサイズを制限します：
  - `mvPartitionsToRefresh`
  - `refBasePartitionsToRefreshMap`
  - `basePartitionsToRefreshMap`
  - `planBuilderMessage`

## ベストプラクティス

### 1. リフレッシュパフォーマンスの監視
- `processStartTime` と実際の終了時間を追跡して、キューイングの問題を特定します
- `adaptivePartitionRefreshNumber` を使用してバッチサイズを最適化します

### 2. 失敗したリフレッシュのデバッグ
- オプティマイザーの問題については `planBuilderMessage` を確認します
- パーティションプルーニングの問題については `refBasePartitionsToRefreshMap` と `basePartitionsToRefreshMap` を確認します

### 3. 増分リフレッシュの最適化
- `nextPartitionStart` と `nextPartitionEnd` を監視して、複数イテレーションのリフレッシュパターンを理解します
- リフレッシュが頻繁に複数の実行にまたがる場合は、パーティションの粒度を調整します

### 4. パーティションカバレッジの理解
- `mvPartitionsToRefresh` と `basePartitionsToRefreshMap` を比較して、MV-基本テーブルパーティションマッピングを確認します
- 基本テーブルパーティションが予想されるリフレッシュ範囲と一致することを確認します

## トラブルシューティング

### 問題: リフレッシュに時間がかかりすぎる

**確認事項:**
1. `processStartTime` - 作成時間との差が大きい場合はキューイングを示します
2. `basePartitionsToRefreshMap` - スキャンされているパーティションが多すぎます
3. `adaptivePartitionRefreshNumber` - ワークロードに合わせて調整が必要な場合があります

### 問題: 予期しないパーティションがリフレッシュされる

**確認事項:**
1. `forceRefresh` - 完全リフレッシュを引き起こす true に設定されている可能性があります
2. `refBasePartitionsToRefreshMap` - 計画されたパーティションを表示します
3. `basePartitionsToRefreshMap` - 最適化後の実際のパーティションを表示します
4. 2つのマップを比較して、オプティマイザーがプランを変更したかどうかを確認します

### 問題: リフレッシュが複数のイテレーションで停滞している

**確認事項:**
1. `nextPartitionStart` と `nextPartitionEnd` - 不完全なリフレッシュ状態を表示します
2. `adaptivePartitionRefreshNumber` - データボリュームに対して小さすぎる可能性があります
3. バッチサイズを増やすか、パーティションの粒度を減らすことを検討してください

## 関連項目

- [CREATE MATERIALIZED VIEW](../sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md)
- [REFRESH MATERIALIZED VIEW](../sql-statements/materialized_view/REFRESH_MATERIALIZED_VIEW.md)
- [Information Schema: task_runs](task_runs.md)
- [マテリアライズドビューの管理](../../using_starrocks/Materialized_view.md)

