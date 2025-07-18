---
displayed_sidebar: docs
sidebar_position: 80
keywords: ['profile', 'query']
---

# Query Profile Metrics

> **StarRocks Query Profile** によって生成される生のメトリクスの権威あるリファレンスで、オペレーターごとにグループ化されています。  
> 用語集として使用してください。トラブルシューティングのガイダンスについては、**query_profile_tuning_recipes.md** にジャンプしてください。

### Summary Metrics

クエリ実行に関する基本情報:

| Metric | Description |
|--------|-------------|
| Total | クエリによって消費された総時間。プランニング、実行、およびプロファイリングフェーズの期間を含みます。 |
| Query State | クエリの状態。可能な状態には、Finished、Error、Running があります。 |
| Query ID | クエリの一意の識別子。 |
| Start Time | クエリが開始されたタイムスタンプ。 |
| End Time | クエリが終了したタイムスタンプ。 |
| Total | クエリの総期間。 |
| Query Type | クエリのタイプ。 |
| Query State | クエリの現在の状態。 |
| StarRocks Version | 使用された StarRocks のバージョン。 |
| User | クエリを実行したユーザー。 |
| Default Db | クエリに使用されたデフォルトのデータベース。 |
| Sql Statement | 実行された SQL ステートメント。 |
| Variables | クエリに使用された重要な変数。 |
| NonDefaultSessionVariables | クエリに使用された非デフォルトのセッション変数。 |
| Collect Profile Time | プロファイルを収集するのにかかった時間。 |
| IsProfileAsync | プロファイル収集が非同期であったかどうかを示します。 |

### Planner Metrics

プランナーの包括的な概要を提供します。通常、プランナーに費やされる総時間が10ms未満であれば、問題はありません。

特定のシナリオでは、プランナーがより多くの時間を必要とする場合があります:
1. 複雑なクエリは、最適な実行プランを確保するために、解析と最適化に追加の時間を必要とする場合があります。
2. 多数のマテリアライズドビューが存在する場合、クエリの書き換えに必要な時間が増加する可能性があります。
3. 複数の同時クエリがシステムリソースを使い果たし、クエリキューが使用される場合、`Pending` 時間が延長される可能性があります。
4. 外部テーブルを含むクエリは、外部メタデータサーバーとの通信に追加の時間を要する可能性があります。

Example:
```
     - -- Parser[1] 0
     - -- Total[1] 3ms
     -     -- Analyzer[1] 0
     -         -- Lock[1] 0
     -         -- AnalyzeDatabase[1] 0
     -         -- AnalyzeTemporaryTable[1] 0
     -         -- AnalyzeTable[1] 0
     -     -- Transformer[1] 0
     -     -- Optimizer[1] 1ms
     -         -- MVPreprocess[1] 0
     -         -- MVTextRewrite[1] 0
     -         -- RuleBaseOptimize[1] 0
     -         -- CostBaseOptimize[1] 0
     -         -- PhysicalRewrite[1] 0
     -         -- DynamicRewrite[1] 0
     -         -- PlanValidate[1] 0
     -             -- InputDependenciesChecker[1] 0
     -             -- TypeChecker[1] 0
     -             -- CTEUniqueChecker[1] 0
     -             -- ColumnReuseChecker[1] 0
     -     -- ExecPlanBuild[1] 0
     - -- Pending[1] 0
     - -- Prepare[1] 0
     - -- Deploy[1] 2ms
     -     -- DeployLockInternalTime[1] 2ms
     -         -- DeploySerializeConcurrencyTime[2] 0
     -         -- DeployStageByStageTime[6] 0
     -         -- DeployWaitTime[6] 1ms
     -             -- DeployAsyncSendTime[2] 0
     - DeployDataSize: 10916
    Reason:
```

### Execution Overview Metrics

高レベルの実行統計:

| Metric | Description | Rule of Thumb |
|--------|-------------|---------------|
| FrontendProfileMergeTime | FE 側のプロファイル処理時間 | < 10ms 正常 |
| QueryAllocatedMemoryUsage | ノード全体で割り当てられたメモリの合計 | |
| QueryDeallocatedMemoryUsage | ノード全体で解放されたメモリの合計 | |
| QueryPeakMemoryUsagePerNode | ノードごとの最大ピークメモリ | < 80% 容量正常 |
| QuerySumMemoryUsage | ノード全体でのピークメモリの合計 | |
| QueryExecutionWallTime | 実行のウォールタイム | |
| QueryCumulativeCpuTime | ノード全体での CPU 時間の合計 | `walltime * totalCpuCores` と比較 |
| QueryCumulativeOperatorTime | オペレーター実行時間の合計 | オペレーター時間の割合の分母 |
| QueryCumulativeNetworkTime | Exchange ノードのネットワーク時間の合計 | |
| QueryCumulativeScanTime | Scan ノードの IO 時間の合計 | |
| QueryPeakScheduleTime | 最大パイプラインスケジュール時間 | 単純なクエリでは < 1s 正常 |
| QuerySpillBytes | ディスクにスピルされたデータ | < 1GB 正常 |

### Fragment Metrics

フラグメントレベルの実行詳細:

| Metric | Description |
|--------|-------------|
| InstanceNum | FragmentInstances の数 |
| InstanceIds | すべての FragmentInstances の ID |
| BackendNum | 参加している BEs の数 |
| BackendAddresses | BE のアドレス |
| FragmentInstancePrepareTime | フラグメント準備フェーズの期間 |
| InstanceAllocatedMemoryUsage | インスタンスに割り当てられたメモリの合計 |
| InstanceDeallocatedMemoryUsage | インスタンスで解放されたメモリの合計 |
| InstancePeakMemoryUsage | インスタンス全体でのピークメモリ |

### Pipeline Metrics

パイプラインの実行詳細と関係:

![profile_pipeline_time_relationship](../../_assets/Profile/profile_pipeline_time_relationship.jpeg)

主要な関係:
- DriverTotalTime = ActiveTime + PendingTime + ScheduleTime
- ActiveTime = ∑ OperatorTotalTime + OverheadTime
- PendingTime = InputEmptyTime + OutputFullTime + PreconditionBlockTime + PendingFinishTime
- InputEmptyTime = FirstInputEmptyTime + FollowupInputEmptyTime

| Metric | Description |
|--------|-------------|
| DegreeOfParallelism | パイプライン実行の並行性の度合い。 |
| TotalDegreeOfParallelism | 並行性の度合いの合計。同じパイプラインが複数のマシンで実行される可能性があるため、この項目はすべての値を集計します。 |
| DriverPrepareTime | 準備フェーズにかかった時間。このメトリクスは DriverTotalTime に含まれません。 |
| DriverTotalTime | 準備フェーズに費やされた時間を除くパイプラインの総実行時間。 |
| ActiveTime | 各オペレーターの実行時間や、has_output、need_input などのメソッドを呼び出す際のフレームワーク全体のオーバーヘッドを含むパイプラインの実行時間。 |
| PendingTime | さまざまな理由でスケジュールされることを妨げられたパイプラインの時間。 |
| InputEmptyTime | 入力キューが空であるためにブロックされたパイプラインの時間。 |
| FirstInputEmptyTime | 入力キューが空であるために最初にブロックされたパイプラインの時間。最初のブロック時間は、主にパイプラインの依存関係によって引き起こされるため、別途計算されます。 |
| FollowupInputEmptyTime | 入力キューが空であるためにその後ブロックされたパイプラインの時間。 |
| OutputFullTime | 出力キューが満杯であるためにブロックされたパイプラインの時間。 |
| PreconditionBlockTime | 依存関係が満たされていないためにブロックされたパイプラインの時間。 |
| PendingFinishTime | 非同期タスクの終了を待つためにブロックされたパイプラインの時間。 |
| ScheduleTime | 準備キューに入ってから実行のためにスケジュールされるまでのパイプラインのスケジュール時間。 |
| BlockByInputEmpty | InputEmpty のためにパイプラインがブロックされた回数。 |
| BlockByOutputFull | OutputFull のためにパイプラインがブロックされた回数。 |
| BlockByPrecondition | 未満の前提条件のためにパイプラインがブロックされた回数。 |

### Operator Metrics

| Metric | Description |
|--------|-------------|
| PrepareTime | 準備に費やされた時間。 |
| OperatorTotalTime | オペレーターによって消費された総時間。次の式を満たします: OperatorTotalTime = PullTotalTime + PushTotalTime + SetFinishingTime + SetFinishedTime + CloseTime。準備に費やされた時間は含まれません。 |
| PullTotalTime | オペレーターが push_chunk を実行するのに費やす総時間。 |
| PushTotalTime | オペレーターが pull_chunk を実行するのに費やす総時間。 |
| SetFinishingTime | オペレーターが set_finishing を実行するのに費やす総時間。 |
| SetFinishedTime | オペレーターが set_finished を実行するのに費やす総時間。 |
| PushRowNum | オペレーターの入力行の累積数。 |
| PullRowNum | オペレーターの出力行の累積数。 |
| JoinRuntimeFilterEvaluate | ジョインランタイムフィルターが評価された回数。 |
| JoinRuntimeFilterHashTime | ジョインランタイムフィルターのハッシュを計算するのに費やされた時間。 |
| JoinRuntimeFilterInputRows | ジョインランタイムフィルターの入力行数。 |
| JoinRuntimeFilterOutputRows | ジョインランタイムフィルターの出力行数。 |
| JoinRuntimeFilterTime | ジョインランタイムフィルターに費やされた時間。 |

### Scan Operator

#### OLAP Scan Operator

OLAP_SCAN オペレーターは、StarRocks 内部テーブルからデータを読み取る役割を担っています。

| Metric | Description |
|--------|-------------|
| Table | テーブル名。 |
| Rollup | マテリアライズドビュー名。マテリアライズドビューがヒットしない場合、テーブル名と同じです。 |
| SharedScan | enable_shared_scan セッション変数が有効かどうか。 |
| TabletCount | タブレットの数。 |
| MorselsCount | モーセルの数。これは基本的な IO 実行単位です。 |
| PushdownPredicates | プッシュダウン述語の数。 |
| Predicates | 述語式。 |
| BytesRead | 読み取られたデータのサイズ。 |
| CompressedBytesRead | ディスクから読み取られた圧縮データのサイズ。 |
| UncompressedBytesRead | ディスクから読み取られた非圧縮データのサイズ。 |
| RowsRead | 読み取られた行数（述語フィルタリング後）。 |
| RawRowsRead | 読み取られた生の行数（述語フィルタリング前）。 |
| ReadPagesNum | 読み取られたページ数。 |
| CachedPagesNum | キャッシュされたページ数。 |
| ChunkBufferCapacity | チャンクバッファの容量。 |
| DefaultChunkBufferCapacity | チャンクバッファのデフォルト容量。 |
| PeakChunkBufferMemoryUsage | チャンクバッファのピークメモリ使用量。 |
| PeakChunkBufferSize | チャンクバッファのピークサイズ。 |
| PrepareChunkSourceTime | チャンクソースの準備に費やされた時間。 |
| ScanTime | 累積スキャン時間。スキャン操作は非同期 I/O スレッドプールで完了します。 |
| IOTaskExecTime | IO タスクの実行時間。 |
| IOTaskWaitTime | IO タスクのスケジュール実行までの成功した提出からの待機時間。 |
| SubmitTaskCount | IO タスクが提出された回数。 |
| SubmitTaskTime | タスク提出に費やされた時間。 |
| PeakIOTasks | IO タスクのピーク数。 |
| PeakScanTaskQueueSize | IO タスクキューのピークサイズ。 |

#### Connector Scan Operator

OLAP_SCAN オペレーターに似ていますが、Iceberg/Hive/Hudi/Detal などの外部テーブルをスキャンするために使用されます。

| Metric | Description |
|--------|-------------|
| DataSourceType | データソースタイプ。HiveDataSource、ESDataSource などが含まれます。 |
| Table | テーブル名。 |
| TabletCount | タブレットの数。 |
| MorselsCount | モーセルの数。 |
| Predicates | 述語式。 |
| PredicatesPartition | パーティションに適用される述語式。 |
| SharedScan | `enable_shared_scan` セッション変数が有効かどうか。 |
| ChunkBufferCapacity | チャンクバッファの容量。 |
| DefaultChunkBufferCapacity | チャンクバッファのデフォルト容量。 |
| PeakChunkBufferMemoryUsage | チャンクバッファのピークメモリ使用量。 |
| PeakChunkBufferSize | チャンクバッファのピークサイズ。 |
| PrepareChunkSourceTime | チャンクソースの準備にかかった時間。 |
| ScanTime | スキャンの累積時間。スキャン操作は非同期 I/O スレッドプールで完了します。 |
| IOTaskExecTime | I/O タスクの実行時間。 |
| IOTaskWaitTime | IO タスクのスケジュール実行までの成功した提出からの待機時間。 |
| SubmitTaskCount | IO タスクが提出された回数。 |
| SubmitTaskTime | タスク提出にかかった時間。 |
| PeakIOTasks | IO タスクのピーク数。 |
| PeakScanTaskQueueSize | IO タスクキューのピークサイズ。 |

### Exchange Operator

Exchange Operator は BE ノード間でデータを送信する役割を担っています。いくつかの種類の交換操作があります: GATHER/BROADCAST/SHUFFLE。

Exchange Operator がクエリのボトルネックになる可能性のある典型的なシナリオ:
1. Broadcast Join: これは小さなテーブルに適した方法です。しかし、例外的な場合にオプティマイザーが最適でないクエリプランを選択すると、ネットワーク帯域幅が大幅に増加する可能性があります。
2. Shuffle Aggregation/Join: 大きなテーブルをシャッフルすると、ネットワーク帯域幅が大幅に増加する可能性があります。

#### Exchange Sink Operator

| Metric | Description |
|--------|-------------|
| ChannelNum | チャネルの数。通常、チャネルの数は受信者の数と等しいです。 |
| DestFragments | 目的地の FragmentInstance ID のリスト。 |
| DestID | 目的地のノード ID。 |
| PartType | データ分配モード。UNPARTITIONED、RANDOM、HASH_PARTITIONED、BUCKET_SHUFFLE_HASH_PARTITIONED などが含まれます。 |
| SerializeChunkTime | チャンクをシリアライズするのにかかった時間。 |
| SerializedBytes | シリアライズされたデータのサイズ。 |
| ShuffleChunkAppendCounter | PartType が HASH_PARTITIONED または BUCKET_SHUFFLE_HASH_PARTITIONED の場合のチャンク追加操作の回数。 |
| ShuffleChunkAppendTime | PartType が HASH_PARTITIONED または BUCKET_SHUFFLE_HASH_PARTITIONED の場合のチャンク追加操作にかかった時間。 |
| ShuffleHashTime | PartType が HASH_PARTITIONED または BUCKET_SHUFFLE_HASH_PARTITIONED の場合のハッシュ計算にかかった時間。 |
| RequestSent | 送信されたデータパケットの数。 |
| RequestUnsent | 送信されなかったデータパケットの数。このメトリクスはショートサーキットロジックがある場合にゼロ以外になります。それ以外の場合はゼロです。 |
| BytesSent | 送信されたデータのサイズ。 |
| BytesUnsent | 送信されなかったデータのサイズ。このメトリクスはショートサーキットロジックがある場合にゼロ以外になります。それ以外の場合はゼロです。 |
| BytesPassThrough | 目的地のノードが現在のノードである場合、データはネットワークを介して送信されず、パススルーデータと呼ばれます。このメトリクスはそのようなパススルーデータのサイズを示します。パススルーは `enable_exchange_pass_through` によって制御されます。 |
| PassThroughBufferPeakMemoryUsage | パススルーバッファのピークメモリ使用量。 |
| CompressTime | 圧縮時間。 |
| CompressedBytes | 圧縮されたデータのサイズ。 |
| OverallThroughput | スループットレート。 |
| NetworkTime | データパケットの送信にかかった時間（受信後の処理時間を除く）。 |
| NetworkBandwidth | 推定ネットワーク帯域幅。 |
| WaitTime | 送信者キューが満杯のための待機時間。 |
| OverallTime | 送信プロセス全体の総時間、つまり最初のデータパケットの送信から最後のデータパケットの正しい受信の確認まで。 |
| RpcAvgTime | RPC の平均時間。 |
| RpcCount | RPC の総数。 |

#### Exchange Source Operator

| Metric | Description |
|--------|-------------|
| RequestReceived | 受信したデータパケットのサイズ。 |
| BytesReceived | 受信したデータのサイズ。 |
| DecompressChunkTime | チャンクを解凍するのにかかった時間。 |
| DeserializeChunkTime | チャンクをデシリアライズするのにかかった時間。 |
| ClosureBlockCount | ブロックされたクロージャの数。 |
| ClosureBlockTime | クロージャのブロックされた時間。 |
| ReceiverProcessTotalTime | 受信側の処理にかかった総時間。 |
| WaitLockTime | ロック待機時間。 |

### Aggregate Operator

**Metrics List**

| Metric | Description |
|--------|-------------|
| `GroupingKeys` | `GROUP BY` カラム。 |
| `AggregateFunctions` | 集計関数の計算にかかった時間。 |
| `AggComputeTime` | AggregateFunctions + Group By の時間。 |
| `ChunkBufferPeakMem` | チャンクバッファのピークメモリ使用量。 |
| `ChunkBufferPeakSize` | チャンクバッファのピークサイズ。 |
| `ExprComputeTime` | 式の計算にかかった時間。 |
| `ExprReleaseTime` | 式の解放にかかった時間。 |
| `GetResultsTime` | 集計結果を抽出するのにかかった時間。 |
| `HashTableSize` | ハッシュテーブルのサイズ。 |
| `HashTableMemoryUsage` | ハッシュテーブルのメモリサイズ。 |
| `InputRowCount` | 入力行数。 |
| `PassThroughRowCount` | 自動モードで、低集計後にストリーミングモードに劣化した場合のストリーミングモードで処理されたデータ行数。 |
| `ResultAggAppendTime` | 集計結果カラムを追加するのにかかった時間。 |
| `ResultGroupByAppendTime` | Group By カラムを追加するのにかかった時間。 |
| `ResultIteratorTime` | ハッシュテーブルを反復するのにかかった時間。 |
| `StreamingTime` | ストリーミングモードでの処理時間。 |

### Join Operator

**Metrics List**

| Metric | Description |
|--------|-------------|
| `DistributionMode` | 分配タイプ。BROADCAST、PARTITIONED、COLOCATE などが含まれます。 |
| `JoinPredicates` | ジョイン述語。 |
| `JoinType` | ジョインタイプ。 |
| `BuildBuckets` | ハッシュテーブルのバケット数。 |
| `BuildKeysPerBucket` | ハッシュテーブルのバケットごとのキー数。 |
| `BuildConjunctEvaluateTime` | ビルドフェーズ中の結合評価にかかった時間。 |
| `BuildHashTableTime` | ハッシュテーブルの構築にかかった時間。 |
| `ProbeConjunctEvaluateTime` | プローブフェーズ中の結合評価にかかった時間。 |
| `SearchHashTableTimer` | ハッシュテーブルを検索するのにかかった時間。 |
| `CopyRightTableChunkTime` | 右テーブルからチャンクをコピーするのにかかった時間。 |
| `OutputBuildColumnTime` | ビルド側のカラムを出力するのにかかった時間。 |
| `OutputProbeColumnTime` | プローブ側のカラムを出力するのにかかった時間。 |
| `HashTableMemoryUsage` | ハッシュテーブルのメモリ使用量。 |
| `RuntimeFilterBuildTime` | ランタイムフィルターの構築にかかった時間。 |
| `RuntimeFilterNum` | ランタイムフィルターの数。 |

### Window Function Operator

| Metric | Description |
|--------|-------------|
| `ProcessMode` | 実行モード。Materializing と Streaming の2つの部分を含みます。さらに、Cumulative、RemovableCumulative、ByDefinition も含まれます。 |
| `ComputeTime` | ウィンドウ関数の計算にかかった時間。 |
| `PartitionKeys` | パーティションカラム。 |
| `AggregateFunctions` | 集計関数。 |
| `ColumnResizeTime` | カラムのリサイズにかかった時間。 |
| `PartitionSearchTime` | パーティション境界を検索するのにかかった時間。 |
| `PeerGroupSearchTime` | ピアグループ境界を検索するのにかかった時間。ウィンドウタイプが `RANGE` の場合にのみ意味があります。 |
| `PeakBufferedRows` | バッファ内のピーク行数。 |
| `RemoveUnusedRowsCount` | 未使用のバッファが削除された回数。 |
| `RemoveUnusedTotalRows` | 未使用のバッファから削除された行の総数。 |

### Sort Operator

| Metric | Description |
|--------|-------------|
| `SortKeys` | ソートキー。 |
| `SortType` | クエリ結果のソート方法: フルソートまたはトップ N 結果のソート。 |
| `MaxBufferedBytes` | バッファされたデータのピークサイズ。 |
| `MaxBufferedRows` | バッファされた行のピーク数。 |
| `NumSortedRuns` | ソートされたランの数。 |
| `BuildingTime` | ソート中に内部データ構造を維持するのにかかった時間。 |
| `MergingTime` | ソート中にソートされたランをマージするのにかかった時間。 |
| `SortingTime` | ソートにかかった時間。 |
| `OutputTime` | 出力ソートシーケンスを構築するのにかかった時間。 |

### Merge Operator

| Metric | Description | Level |
|--------|-------------|-------|
| `Limit` | リミット。 | Primary |
| `Offset` | オフセット。 | Primary |
| `StreamingBatchSize` | ストリーミングモードでマージが実行される場合のマージ操作ごとに処理されるデータのサイズ | Primary |
| `LateMaterializationMaxBufferChunkNum` | 後期実体化が有効な場合のバッファ内の最大チャンク数。 | Primary |
| `OverallStageCount` | すべてのステージの総実行回数。 | Primary |
| `OverallStageTime` | 各ステージの総実行時間。 | Primary |
| `1-InitStageCount` | Init ステージの実行回数。 | Secondary |
| `2-PrepareStageCount` | Prepare ステージの実行回数。 | Secondary |
| `3-ProcessStageCount` | Process ステージの実行回数。 | Secondary |
| `4-SplitChunkStageCount` | SplitChunk ステージの実行回数。 | Secondary |
| `5-FetchChunkStageCount` | FetchChunk ステージの実行回数。 | Secondary |
| `6-PendingStageCount` | Pending ステージの実行回数。 | Secondary |
| `7-FinishedStageCount` | Finished ステージの実行回数。 | Secondary |
| `1-InitStageTime` | Init ステージの実行時間。 | Secondary |
| `2-PrepareStageTime` | Prepare ステージの実行時間。 | Secondary |
| `3-ProcessStageTime` | Process ステージの実行時間。 | Secondary |
| `4-SplitChunkStageTime` | Split ステージにかかった時間。 | Secondary |
| `5-FetchChunkStageTime` | Fetch ステージにかかった時間。 | Secondary |
| `6-PendingStageTime` | Pending ステージにかかった時間。 | Secondary |
| `7-FinishedStageTime` | Finished ステージにかかった時間。 | Secondary |
| `LateMaterializationGenerateOrdinalTime` | 後期実体化中に序数カラムを生成するのにかかった時間。 | Tertiary |
| `SortedRunProviderTime` | Process ステージ中にプロバイダーからデータを取得するのにかかった時間。 | Tertiary |

### TableFunction Operator

| Metric | Description |
|--------|-------------|
| `TableFunctionExecTime` | テーブル関数の計算時間。 |
| `TableFunctionExecCount` | テーブル関数の実行回数。 |

### Project Operator

Project Operator は `SELECT <expr>` を実行する役割を担っています。クエリに高価な式が含まれている場合、このオペレーターはかなりの時間を要することがあります。

| Metric | Description |
|--------|-------------|
| `ExprComputeTime` | 式の計算時間。 |
| `CommonSubExprComputeTime` | 共通部分式の計算時間。 |

### LocalExchange Operator

| Metric | Description |
|--------|-------------|
| Type | Local Exchange のタイプ。`Passthrough`、`Partition`、`Broadcast` が含まれます。 |
| `ShuffleNum` | シャッフルの数。このメトリクスは `Type` が `Partition` の場合にのみ有効です。 |
| `LocalExchangePeakMemoryUsage` | ピークメモリ使用量。 |
| `LocalExchangePeakBufferSize` | バッファのピークサイズ。 |
| `LocalExchangePeakBufferMemoryUsage` | バッファのピークメモリ使用量。 |
| `LocalExchangePeakBufferChunkNum` | バッファ内のピークチャンク数。 |
| `LocalExchangePeakBufferRowNum` | バッファ内のピーク行数。 |
| `LocalExchangePeakBufferBytes` | バッファ内のデータのピークサイズ。 |
| `LocalExchangePeakBufferChunkSize` | バッファ内のチャンクのピークサイズ。 |
| `LocalExchangePeakBufferChunkRowNum` | バッファ内のチャンクごとのピーク行数。 |
| `LocalExchangePeakBufferChunkBytes` | バッファ内のチャンクごとのデータのピークサイズ。 |

### OlapTableSink Operator

OlapTableSink Operator は `INSERT INTO <table>` 操作を実行する役割を担っています。

:::tip
- `OlapTableSink` の `PushChunkNum` メトリクスの最大値と最小値の差が大きい場合、上流オペレーターでのデータスキューを示しており、ロードパフォーマンスのボトルネックになる可能性があります。
- `RpcClientSideTime` は `RpcServerSideTime` にネットワーク伝送時間と RPC フレームワーク処理時間を加えたものです。`RpcClientSideTime` と `RpcServerSideTime` の間に大きな差がある場合、圧縮を有効にして伝送時間を短縮することを検討してください。
:::

| Metric | Description |
|--------|-------------|
| `IndexNum` | 目的地のテーブルに作成された同期マテリアライズドビューの数。 |
| `ReplicatedStorage` | シングルリーダーレプリケーションが有効かどうか。 |
| `TxnID` | ロードトランザクションの ID。 |
| `RowsRead` | 上流オペレーターから読み取られた行数。 |
| `RowsFiltered` | データ品質が不十分なためにフィルタリングされた行数。 |
| `RowsReturned` | 目的地のテーブルに書き込まれた行数。 |
| `RpcClientSideTime` | クライアント側で記録されたロードの総 RPC 時間消費。 |
| `RpcServerSideTime` | サーバー側で記録されたロードの総 RPC 時間消費。 |
| `PrepareDataTime` | データ準備フェーズの総時間消費。データ形式の変換とデータ品質チェックを含みます。 |
| `SendDataTime` | データ送信のローカル時間消費。データのシリアライズと圧縮、および送信者キューへのタスクの提出時間を含みます。 |