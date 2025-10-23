---
displayed_sidebar: docs
sidebar_label: Troubleshooting
---

# データロードのトラブルシューティング

このガイドは、DBA や運用エンジニアが外部の監視システムに頼らずに SQL インターフェースを通じてデータロードジョブのステータスを監視するのを支援するために設計されています。また、ロード操作中のパフォーマンスボトルネックの特定や異常のトラブルシューティングに関するガイダンスも提供します。

## 用語

**Load Job:** **Routine Load Job** や **Pipe Job** のような継続的なデータロードプロセス。

**Load Task:** 通常、単一のロードトランザクションに対応する一度きりのデータロードプロセス。例として、**Broker Load**、**Stream Load**、**Spark Load**、および **INSERT INTO** があります。Routine Load ジョブと Pipe ジョブは、データ取り込みを行うためにタスクを継続的に生成します。

## ロードジョブの観察

ロードジョブを観察する方法は2つあります：

- SQL ステートメント **[SHOW ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD.md)** および **[SHOW PIPES](../../sql-reference/sql-statements/loading_unloading/pipe/SHOW_PIPES.md)** を使用する。
- システムビュー **[information_schema.routine_load_jobs](../../sql-reference/information_schema/routine_load_jobs.md)** および **[information_schema.pipes](../../sql-reference/information_schema/pipes.md)** を使用する。

## ロードタスクの観察

ロードタスクも2つの方法で監視できます：

- SQL ステートメント **[SHOW LOAD](../../sql-reference/sql-statements/loading_unloading/SHOW_LOAD.md)** および **[SHOW ROUTINE LOAD TASK](../../sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD_TASK.md)** を使用する。
- システムビュー **[information_schema.loads](../../sql-reference/information_schema/loads.md)** および **statistics.loads_history** を使用する。

### SQL ステートメント

**SHOW** ステートメントは、現在のデータベースの進行中および最近完了したロードタスクを表示し、タスクのステータスを迅速に把握できます。取得される情報は、**statistics.loads_history** システムビューのサブセットです。

SHOW LOAD ステートメントは Broker Load、Insert Into、Spark Load タスクの情報を返し、SHOW ROUTINE LOAD TASK ステートメントは Routine Load タスクの情報を返します。

### システムビュー

#### information_schema.loads

**information_schema.loads** システムビューは、最近のロードタスクに関する情報を保存し、アクティブなものと最近完了したものを含みます。StarRocks は定期的にデータを **statistics.loads_history** システムテーブルに同期し、永続的に保存します。

**information_schema.loads** は以下のフィールドを提供します：

| フィールド                | 説明                                                  |
| -------------------- | ------------------------------------------------------------ |
| ID                   | グローバルに一意の識別子。                                  |
| LABEL                | ロードジョブのラベル。                                       |
| PROFILE_ID           | `ANALYZE PROFILE` を通じて分析できるプロファイルの ID。 |
| DB_NAME              | 対象テーブルが属するデータベース。              |
| TABLE_NAME           | 対象テーブル。                                            |
| USER                 | ロードジョブを開始したユーザー。                         |
| WAREHOUSE            | ロードジョブが属するウェアハウス。                 |
| STATE                | ロードジョブの状態。 有効な値:<ul><li>`PENDING`/`BEGIN`: ロードジョブが作成された。</li><li>`QUEUEING`/`BEFORE_LOAD`: ロードジョブがスケジュール待ちのキューにある。</li><li>`LOADING`: ロードジョブが実行中。</li><li>`PREPARING`: トランザクションが事前コミットされている。</li><li>`PREPARED`: トランザクションが事前コミットされた。</li><li>`COMMITED`: トランザクションがコミットされた。</li><li>`FINISHED`: ロードジョブが成功した。</li><li>`CANCELLED`: ロードジョブが失敗した。</li></ul> |
| PROGRESS             | ロードジョブの ETL ステージと LOADING ステージの進捗。 |
| TYPE                 | ロードジョブのタイプ。 Broker Load の場合、返される値は `BROKER`。INSERT の場合、返される値は `INSERT`。Stream Load の場合、返される値は `STREAM`。Routine Load の場合、返される値は `ROUTINE`。 |
| PRIORITY             | ロードジョブの優先度。 有効な値: `HIGHEST`, `HIGH`, `NORMAL`, `LOW`, `LOWEST`。 |
| SCAN_ROWS            | スキャンされたデータ行の数。                    |
| SCAN_BYTES           | スキャンされたバイト数。                        |
| FILTERED_ROWS        | データ品質が不十分なためにフィルタリングされたデータ行の数。 |
| UNSELECTED_ROWS      | WHERE 句で指定された条件によりフィルタリングされたデータ行の数。 |
| SINK_ROWS            | ロードされたデータ行の数。                     |
| RUNTIME_DETAILS      | ロードの実行時メタデータ。詳細は [RUNTIME_DETAILS](#runtime_details) を参照。 |
| CREATE_TIME          | ロードジョブが作成された時間。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| LOAD_START_TIME      | ロードジョブの LOADING ステージの開始時間。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| LOAD_COMMIT_TIME     | ロADING トランザクションがコミットされた時間。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| LOAD_FINISH_TIME     | ロードジョブの LOADING ステージの終了時間。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| PROPERTIES           | ロードジョブの静的プロパティ。詳細は [PROPERTIES](#properties) を参照。 |
| ERROR_MSG            | ロードジョブのエラーメッセージ。エラーが発生しなかった場合、`NULL` が返されます。 |
| TRACKING_SQL         | ロードジョブの追跡ログをクエリするために使用できる SQL ステートメント。ロードジョブが不適格なデータ行を含む場合にのみ SQL ステートメントが返されます。不適格なデータ行を含まない場合、`NULL` が返されます。 |
| REJECTED_RECORD_PATH | ロードジョブでフィルタリングされたすべての不適格なデータ行にアクセスできるパス。ログに記録される不適格なデータ行の数は、ロードジョブで設定された `log_rejected_record_num` パラメータによって決まります。このパスにアクセスするには `wget` コマンドを使用できます。不適格なデータ行を含まない場合、`NULL` が返されます。 |

##### RUNTIME_DETAILS

- 共通メトリクス:

| メトリック               | 説明                                                  |
| -------------------- | ------------------------------------------------------------ |
| load_id              | ロード実行計画のグローバルに一意の ID。               |
| txn_id               | ロードトランザクション ID。                                         |

- Broker Load、INSERT INTO、Spark Load の特定メトリクス:

| メトリック               | 説明                                                  |
| -------------------- | ------------------------------------------------------------ |
| etl_info             | ETL 詳細。このフィールドは Spark Load ジョブにのみ有効です。他のタイプのロードジョブでは、値は空になります。 |
| etl_start_time       | ロードジョブの ETL ステージの開始時間。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| etl_start_time       | ロードジョブの ETL ステージの終了時間。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| unfinished_backends  | 実行が完了していない BEs のリスト。                      |
| backends             | 実行に参加している BEs のリスト。                      |
| file_num             | 読み取られたファイルの数。                                        |
| file_size            | 読み取られたファイルの合計サイズ。                                    |
| task_num             | サブタスクの数。                                          |

- Routine Load の特定メトリクス:

| メトリック               | 説明                                                  |
| -------------------- | ------------------------------------------------------------ |
| schedule_interval    | Routine Load がスケジュールされる間隔。               |
| wait_slot_time       | Routine Load タスクが実行スロットを待機している間に経過した時間。 |
| check_offset_time    | Routine Load タスクのスケジューリング中にオフセット情報を確認する際に消費される時間。 |
| consume_time         | Routine Load タスクが上流データを読み取るのに消費する時間。 |
| plan_time            | 実行計画を生成する時間。                      |
| commit_publish_time  | COMMIT RPC を実行するのに消費される時間。                     |

- Stream Load の特定メトリクス:

| メトリック                 | 説明                                                |
| ---------------------- | ---------------------------------------------------------- |
| timeout                | ロードタスクのタイムアウト。                                    |
| begin_txn_ms           | トランザクションを開始するのに消費される時間。                    |
| plan_time_ms           | 実行計画を生成する時間。                    |
| receive_data_time_ms   | データを受信する時間。                                   |
| commit_publish_time_ms | COMMIT RPC を実行するのに消費される時間。                   |
| client_ip              | クライアントの IP アドレス。                                         |

##### PROPERTIES

- Broker Load、INSERT INTO、Spark Load の特定プロパティ:

| プロパティ               | 説明                                                |
| ---------------------- | ---------------------------------------------------------- |
| timeout                | ロードタスクのタイムアウト。                                    |
| max_filter_ratio       | データ品質が不十分なためにフィルタリングされるデータ行の最大比率。 |

- Routine Load の特定プロパティ:

| プロパティ               | 説明                                                |
| ---------------------- | ---------------------------------------------------------- |
| job_name               | Routine Load ジョブ名。                                     |
| task_num               | 実際に並行して実行されるサブタスクの数。          |
| timeout                | ロードタスクのタイムアウト。                                    |

#### statistics.loads_history

**statistics.loads_history** システムビューは、デフォルトで過去3か月間のロード記録を保存します。DBA はビューの `partition_ttl` を変更して保持期間を調整できます。**statistics.loads_history** は **information_schema.loads** と一貫したスキーマを持っています。

## Load Profiles でロードパフォーマンスの問題を特定する

**Load Profile** は、データロードに関与するすべてのワーカーノードの実行詳細を記録します。これにより、StarRocks クラスター内のパフォーマンスボトルネックを迅速に特定できます。

### Load Profiles を有効にする

StarRocks は、ロードの種類に応じて Load Profiles を有効にする複数の方法を提供します：

#### Broker Load と INSERT INTO の場合

Broker Load と INSERT INTO の Load Profiles をセッションレベルで有効にします：

```sql
SET enable_profile = true;
```

デフォルトでは、長時間実行されるジョブ（300 秒以上）に対してプロファイルが自動的に有効になります。このしきい値をカスタマイズするには：

```sql
SET big_query_profile_threshold = 60s;
```

:::note
`big_query_profile_threshold` がデフォルト値 `0` に設定されている場合、デフォルトの動作はクエリのプロファイリングを無効にすることです。ただし、ロードタスクの場合、実行時間が 300 秒を超えるタスクには自動的にプロファイルが記録されます。
:::

StarRocks は **Runtime Profiles** もサポートしており、長時間実行されるロードジョブの実行メトリクスを定期的に（30 秒ごとに）報告します。報告間隔をカスタマイズするには：

```sql
SET runtime_profile_report_interval = 60;
```

:::note
`runtime_profile_report_interval` はロードタスクの最小報告間隔のみを指定します。実際の報告間隔は動的に調整され、この値を超える場合があります。
:::

#### Stream Load と Routine Load の場合

Stream Load と Routine Load の Load Profiles をテーブルレベルで有効にします：

```sql
ALTER TABLE <table_name> SET ("enable_load_profile" = "true");
```

Stream Load は通常、高い QPS を持つため、StarRocks は広範なプロファイリングによるパフォーマンス低下を避けるために Load Profile 収集のサンプリングを許可しています。収集間隔を調整するには、FE パラメータ `load_profile_collect_interval_second` を設定します。この設定は、テーブルプロパティを介して有効にされた Load Profiles にのみ適用されます。デフォルト値は `0` です。

```SQL
ADMIN SET FRONTEND CONFIG ("load_profile_collect_interval_second"="30");
```

StarRocks はまた、特定の時間しきい値を超えるロードジョブからのみプロファイルを収集することを許可しています。このしきい値を調整するには、FE パラメータ `stream_load_profile_collect_threshold_second` を設定します。デフォルト値は `0` です。

```SQL
ADMIN SET FRONTEND CONFIG ("stream_load_profile_collect_threshold_second"="10");
```

### Load Profiles を分析する

Load Profiles の構造は Query Profiles と同一です。詳細な手順については、[Query Tuning Recipes](../../best_practices/query_tuning/query_profile_tuning_recipes.md) を参照してください。

Load Profiles を分析するには、[ANALYZE PROFILE](../../sql-reference/sql-statements/cluster-management/plan_profile/ANALYZE_PROFILE.md) を実行します。詳細な手順については、[Analyze text-based Profiles](../../best_practices/query_tuning/query_profile_text_based_analysis.md) を参照してください。

プロファイルは詳細なオペレーターメトリクスを提供します。主要なコンポーネントには `OlapTableSink` オペレーターと `LoadChannel` オペレーターが含まれます。

#### OlapTableSink オペレーター

| メトリック            | 説明                                                  |
| ----------------- | ------------------------------------------------------------ |
| IndexNum          | 対象テーブルの同期マテリアライズドビューの数。 |
| ReplicatedStorage | シングルリーダーレプリケーションが有効かどうか。                |
| TxnID             | ロードトランザクション ID。                                         |
| RowsRead          | 上流オペレーターから読み取られたデータ行の数。         |
| RowsFiltered      | データ品質が不十分なためにフィルタリングされたデータ行の数。 |
| RowsReturned      | ロードされたデータ行の数。                     |
| RpcClientSideTime | クライアント側の統計からのデータ書き込み RPC に消費される合計時間。 |
| RpcServerSideTime | サーバー側の統計からのデータ書き込み RPC に消費される合計時間。 |
| PrepareDataTime   | データフォーマット変換とデータ品質チェックに消費される時間。 |
| SendDataTime      | データ送信に消費されるローカル時間。データのシリアル化、圧縮、および送信キューへの書き込みを含む。 |

:::tip
- `OLAP_TABLE_SINK` の `PushChunkNum` の最大値と最小値の間の大きな差異は、上流オペレーターでのデータスキューを示しており、書き込みパフォーマンスのボトルネックを引き起こす可能性があります。
- `RpcClientSideTime` は `RpcServerSideTime`、ネットワーク伝送時間、および RPC フレームワーク処理時間の合計に等しいです。`RpcClientSideTime` と `RpcServerSideTime` の差が大きい場合、データ圧縮を有効にして伝送時間を短縮することを検討してください。
- `RpcServerSideTime` が時間の大部分を占める場合、さらなる分析は `LoadChannel` プロファイルを使用して実施できます。
:::

#### LoadChannel オペレーター

| メトリック              | 説明                                                  |
| ------------------- | ------------------------------------------------------------ |
| Address             | BE ノードの IP アドレスまたは FQDN。                           |
| LoadMemoryLimit     | ロードのメモリ制限。                                    |
| PeakMemoryUsage     | ロードのピークメモリ使用量。                               |
| OpenCount           | チャネルが開かれた回数。シンクの総並列度を反映します。 |
| OpenTime            | チャネルを開くのに消費される合計時間。                        |
| AddChunkCount       | ロードチャンクの数、つまり `TabletsChannel::add_chunk` の呼び出し回数。 |
| AddRowNum           | ロードされたデータ行の数。                     |
| AddChunkTime        | ロードチャンクに消費される合計時間、つまり `TabletsChannel::add_chunk` の総実行時間。    |
| WaitFlushTime       | `TabletsChannel::add_chunk` が MemTable フラッシュを待機するのに費やした合計時間。 |
| WaitWriterTime      | `TabletsChannel::add_chunk` が非同期デルタライターの実行を待機するのに費やした合計時間。 |
| WaitReplicaTime     | `TabletsChannel::add_chunk` がレプリカからの同期を待機するのに費やした合計時間。 |
| PrimaryTabletsNum   | プライマリタブレットの数。                                   |
| SecondaryTabletsNum | セカンダリタブレットの数。                                 |

:::tip
`WaitFlushTime` が長時間かかる場合、フラッシュスレッドのリソースが不足している可能性があります。BE の設定 `flush_thread_num_per_store` を調整することを検討してください。
:::

## ベストプラクティス

### Broker Load のパフォーマンスボトルネックを診断する

1. Broker Load を使用してデータをロードします：

    ```SQL
    LOAD LABEL click_bench.hits_1713874468 
    (
        DATA INFILE ("s3://test-data/benchmark_data/query_data/click_bench/hits.tbl*") 
        INTO TABLE hits COLUMNS TERMINATED BY "\t" (WatchID,JavaEnable,Title,GoodEvent,EventTime,EventDate,CounterID,ClientIP,RegionID,UserID,CounterClass,OS,UserAgent,URL,Referer,IsRefresh,RefererCategoryID,RefererRegionID,URLCategoryID,URLRegionID,ResolutionWidth,ResolutionHeight,ResolutionDepth,FlashMajor,FlashMinor,FlashMinor2,NetMajor,NetMinor,UserAgentMajor,UserAgentMinor,CookieEnable,JavascriptEnable,IsMobile,MobilePhone,MobilePhoneModel,Params,IPNetworkID,TraficSourceID,SearchEngineID,SearchPhrase,AdvEngineID,IsArtifical,WindowClientWidth,WindowClientHeight,ClientTimeZone,ClientEventTime,SilverlightVersion1,SilverlightVersion2,SilverlightVersion3,SilverlightVersion4,PageCharset,CodeVersion,IsLink,IsDownload,IsNotBounce,FUniqID,OriginalURL,HID,IsOldCounter,IsEvent,IsParameter,DontCountHits,WithHash,HitColor,LocalEventTime,Age,Sex,Income,Interests,Robotness,RemoteIP,WindowName,OpenerName,HistoryLength,BrowserLanguage,BrowserCountry,SocialNetwork,SocialAction,HTTPError,SendTiming,DNSTiming,ConnectTiming,ResponseStartTiming,ResponseEndTiming,FetchTiming,SocialSourceNetworkID,SocialSourcePage,ParamPrice,ParamOrderID,ParamCurrency,ParamCurrencyID,OpenstatServiceName,OpenstatCampaignID,OpenstatAdID,OpenstatSourceID,UTMSource,UTMMedium,UTMCampaign,UTMContent,UTMTerm,FromTag,HasGCLID,RefererHash,URLHash,CLID)
    ) 
    WITH BROKER 
    (
                "aws.s3.access_key" = "<iam_user_access_key>",
                "aws.s3.secret_key" = "<iam_user_secret_key>",
                "aws.s3.region" = "<aws_s3_region>"
    )
    ```

2. **SHOW PROFILELIST** を使用して、ランタイムプロファイルのリストを取得します。

    ```SQL
    MySQL [click_bench]> SHOW PROFILELIST;
    +--------------------------------------+---------------------+----------+---------+----------------------------------------------------------------------------------------------------------------------------------+
    | QueryId                              | StartTime           | Time     | State   | Statement                                                                                                                        |
    +--------------------------------------+---------------------+----------+---------+----------------------------------------------------------------------------------------------------------------------------------+
    | 3df61627-f82b-4776-b16a-6810279a79a3 | 2024-04-23 20:28:26 | 11s850ms | Running | LOAD LABEL click_bench.hits_1713875306 (DATA INFILE ("s3://test-data/benchmark_data/query_data/click_bench/hits.tbl*" ... |
    +--------------------------------------+---------------------+----------+---------+----------------------------------------------------------------------------------------------------------------------------------+
    1 row in set (0.00 sec)
    ```

3. **ANALYZE PROFILE** を使用して、ランタイムプロファイルを表示します。

    ```SQL
    MySQL [click_bench]> ANALYZE PROFILE FROM '3df61627-f82b-4776-b16a-6810279a79a3';
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | Explain String                                                                                                                                              |
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | Summary                                                                                                                                             |
    |     Attention: The transaction of the statement will be aborted, and no data will be actually inserted!!!                           |
    |     Attention: Profile is not identical!!!                                                                                          |
    |     QueryId: 3df61627-f82b-4776-b16a-6810279a79a3                                                                                                   |
    |     Version: default_profile-70fe819                                                                                                                |
    |     State: Running                                                                                                                                  |
    |     Legend: ⏳ for blocked; 🚀 for running; ✅ for finished                                                                                             |
    |     TotalTime: 31s832ms                                                                                                                             |
    |         ExecutionTime: 30s1ms [Scan: 28s885ms (96.28%), Network: 0ns (0.00%), ResultDeliverTime: 7s613ms (25.38%), ScheduleTime: 145.701ms (0.49%)] |
    |         FrontendProfileMergeTime: 3.838ms                                                                                                           |
    |     QueryPeakMemoryUsage: 141.367 MB, QueryAllocatedMemoryUsage: 82.422 GB                                                                          |
    |     Top Most Time-consuming Nodes:                                                                                                                  |
    |         1. FILE_SCAN (id=0)  🚀 : 28s902ms (85.43%)                                                                                              |
    |         2. OLAP_TABLE_SINK 🚀 : 4s930ms (14.57%)                                                                                                      |
    |     Top Most Memory-consuming Nodes:                                                                                                                |
    |     Progress (finished operator/all operator): 0.00%                                                                                                |
    |     NonDefaultVariables:                                                                                                                            |
    |         big_query_profile_threshold: 0s -> 60s                                                                                                      |
    |         enable_adaptive_sink_dop: false -> true                                                                                                     |
    |         enable_profile: false -> true                                                                                                               |
    |         sql_mode_v2: 32 -> 34                                                                                                                       |
    |         use_compute_nodes: -1 -> 0                                                                                                                  |
    | Fragment 0                                                                                                                                          |
    | │   BackendNum: 3                                                                                                                                   |
    | │   InstancePeakMemoryUsage: 128.541 MB, InstanceAllocatedMemoryUsage: 82.422 GB                                                                    |
    | │   PrepareTime: 2.304ms                                                                                                                            |
    | └──OLAP_TABLE_SINK                                                                                                                                  |
    |    │   TotalTime: 4s930ms (14.57%) [CPUTime: 4s930ms]                                                                                               |
    |    │   OutputRows: 14.823M (14823424)                                                                                                               |
    |    │   PartitionType: RANDOM                                                                                                                        |
    |    │   Table: hits                                                                                                                                  |
    |    └──FILE_SCAN (id=0)  🚀                                                                                                                       |
    |           Estimates: [row: ?, cpu: ?, memory: ?, network: ?, cost: ?]                                                                          |
    |           TotalTime: 28s902ms (85.43%) [CPUTime: 17.038ms, ScanTime: 28s885ms]                                                                 |
    |           OutputRows: 14.823M (14823424)                                                                                                       |
    |           Progress (processed rows/total rows): ?                                                                                              |
    |           Detail Timers: [ScanTime = IOTaskExecTime + IOTaskWaitTime]                                                                          |
    |               IOTaskExecTime: 25s612ms [min=19s376ms, max=28s804ms]                                                                            |
    |               IOTaskWaitTime: 63.192ms [min=20.946ms, max=91.668ms]                                                                            |
    |                                                                                                                                                         |
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------+
    40 rows in set (0.04 sec)
    ```

プロファイルは、`FILE_SCAN` セクションが約 29 秒かかり、合計 32 秒の約 90% を占めていることを示しています。これは、オブジェクトストレージからデータを読み取ることが現在のロードプロセスのボトルネックであることを示しています。

### Stream Load のパフォーマンスを診断する

1. 対象テーブルの Load Profile を有効にします。

    ```SQL
    mysql> ALTER TABLE duplicate_200_column_sCH SET('enable_load_profile'='true');
    Query OK, 0 rows affected (0.00 sec)
    ```

2. **SHOW PROFILELIST** を使用して、プロファイルのリストを取得します。

    ```SQL
    mysql> SHOW PROFILELIST;
    +--------------------------------------+---------------------+----------+----------+-----------+
    | QueryId                              | StartTime           | Time     | State    | Statement |
    +--------------------------------------+---------------------+----------+----------+-----------+
    | 90481df8-afaf-c0fd-8e91-a7889c1746b6 | 2024-09-19 10:43:38 | 9s571ms  | Finished |           |
    | 9c41a13f-4d7b-2c18-4eaf-cdeea3facba5 | 2024-09-19 10:43:37 | 10s664ms | Finished |           |
    | 5641cf37-0af4-f116-46c6-ca7cce149886 | 2024-09-19 10:43:20 | 13s88ms  | Finished |           |
    | 4446c8b3-4dc5-9faa-dccb-e1a71ab3519e | 2024-09-19 10:43:20 | 13s64ms  | Finished |           |
    | 48469b66-3866-1cd9-9f3b-17d786bb4fa7 | 2024-09-19 10:43:20 | 13s85ms  | Finished |           |
    | bc441907-e779-bc5a-be8e-992757e4d992 | 2024-09-19 10:43:19 | 845ms    | Finished |           |
    +--------------------------------------+---------------------+----------+----------+-----------+
    ```

3. **ANALYZE PROFILE** を使用して、プロファイルを表示します。

    ```SQL
    mysql> ANALYZE PROFILE FROM '90481df8-afaf-c0fd-8e91-a7889c1746b6';
    +-----------------------------------------------------------+
    | Explain String                                            |
    +-----------------------------------------------------------+
    | Load:                                                     |
    |   Summary:                                                |
    |      - Query ID: 90481df8-afaf-c0fd-8e91-a7889c1746b6     |
    |      - Start Time: 2024-09-19 10:43:38                    |
    |      - End Time: 2024-09-19 10:43:48                      |
    |      - Query Type: Load                                   |
    |      - Load Type: STREAM_LOAD                             |
    |      - Query State: Finished                              |
    |      - StarRocks Version: main-d49cb08                    |
    |      - Sql Statement                                      |
    |      - Default Db: ingestion_db                           |
    |      - NumLoadBytesTotal: 799008                          |
    |      - NumRowsAbnormal: 0                                 |
    |      - NumRowsNormal: 280                                 |
    |      - Total: 9s571ms                                     |
    |      - numRowsUnselected: 0                               |
    |   Execution:                                              |
    |     Fragment 0:                                           |
    |        - Address: 172.26.93.218:59498                     |
    |        - InstanceId: 90481df8-afaf-c0fd-8e91-a7889c1746b7 |
    |        - TxnID: 1367                                      |
    |        - ReplicatedStorage: true                          |
    |        - AutomaticPartition: false                        |
    |        - InstanceAllocatedMemoryUsage: 12.478 MB          |
    |        - InstanceDeallocatedMemoryUsage: 10.745 MB        |
    |        - InstancePeakMemoryUsage: 9.422 MB                |
    |        - MemoryLimit: -1.000 B                            |
    |        - RowsProduced: 280                                |
    |          - AllocAutoIncrementTime: 348ns                  |
    |          - AutomaticBucketSize: 0                         |
    |          - BytesRead: 0.000 B                             |
    |          - CloseWaitTime: 9s504ms                         |
    |          - IOTaskExecTime: 0ns                            |
    |          - IOTaskWaitTime: 0ns                            |
    |          - IndexNum: 1                                    |
    |          - NumDiskAccess: 0                               |
    |          - OpenTime: 15.639ms                             |
    |          - PeakMemoryUsage: 0.000 B                       |
    |          - PrepareDataTime: 583.480us                     |
    |            - ConvertChunkTime: 44.670us                   |
    |            - ValidateDataTime: 109.333us                  |
    |          - RowsFiltered: 0                                |
    |          - RowsRead: 0                                    |
    |          - RowsReturned: 280                              |
    |          - RowsReturnedRate: 12.049K (12049) /sec         |
    |          - RpcClientSideTime: 28s396ms                    |
    |          - RpcServerSideTime: 28s385ms                    |
    |          - RpcServerWaitFlushTime: 0ns                    |
    |          - ScanTime: 9.841ms                              |
    |          - ScannerQueueCounter: 1                         |
    |          - ScannerQueueTime: 3.272us                      |
    |          - ScannerThreadsInvoluntaryContextSwitches: 0    |
    |          - ScannerThreadsTotalWallClockTime: 0ns          |
    |            - MaterializeTupleTime(*): 0ns                 |
    |            - ScannerThreadsSysTime: 0ns                   |
    |            - ScannerThreadsUserTime: 0ns                  |
    |          - ScannerThreadsVoluntaryContextSwitches: 0      |
    |          - SendDataTime: 2.452ms                          |
    |            - PackChunkTime: 1.475ms                       |
    |            - SendRpcTime: 1.617ms                         |
    |              - CompressTime: 0ns                          |
    |              - SerializeChunkTime: 880.424us              |
    |            - WaitResponseTime: 0ns                        |
    |          - TotalRawReadTime(*): 0ns                       |
    |          - TotalReadThroughput: 0.000 B/sec               |
    |       DataSource:                                         |
    |          - DataSourceType: FileDataSource                 |
    |          - FileScanner:                                   |
    |            - CastChunkTime: 0ns                           |
    |            - CreateChunkTime: 227.100us                   |
    |            - FileReadCount: 3                             |
    |            - FileReadTime: 253.765us                      |
    |            - FillTime: 6.892ms                            |
    |            - MaterializeTime: 133.637us                   |
    |            - ReadTime: 0ns                                |
    |          - ScannerTotalTime: 9.292ms                      |
    +-----------------------------------------------------------+
    76 rows in set (0.00 sec)
    ```

## 付録

### 運用に役立つ SQL

#### 分単位のスループットをクエリする

```SQL
-- 全体
select date_trunc('minute', load_finish_time) as t,count(*) as tpm,sum(SCAN_BYTES) as scan_bytes,sum(sink_rows) as sink_rows from _statistics_.loads_history group by t order by t desc limit 10;

-- テーブル
select date_trunc('minute', load_finish_time) as t,count(*) as tpm,sum(SCAN_BYTES) as scan_bytes,sum(sink_rows) as sink_rows from _statistics_.loads_history where table_name = 't' group by t order by t desc limit 10;
```

#### テーブルの RowsetNum と SegmentNum をクエリする

```SQL
-- 全体
select * from information_schema.be_tablets t, information_schema.tables_config c where t.table_id = c.table_id order by num_segment desc limit 5;
select * from information_schema.be_tablets t, information_schema.tables_config c where t.table_id = c.table_id order by num_rowset desc limit 5;

-- テーブル
select * from information_schema.be_tablets t, information_schema.tables_config c where t.table_id = c.table_id and table_name = 't' order by num_segment desc limit 5;
select * from information_schema.be_tablets t, information_schema.tables_config c where t.table_id = c.table_id and table_name = 't' order by num_rowset desc limit 5;
```

- 高い RowsetNum (>100) はロードが頻繁すぎることを示します。頻度を減らすか、Compaction スレッドを増やすことを検討してください。
- 高い SegmentNum (>100) はロードごとのセグメントが多すぎることを示します。Compaction スレッドを増やすか、テーブルにランダム分散戦略を採用することを検討してください。

#### データスキューを確認する

##### ノード間のデータスキュー

```SQL
-- 全体
SELECT tbt.be_id, sum(tbt.DATA_SIZE) FROM information_schema.tables_config tb JOIN information_schema.be_tablets tbt ON tb.TABLE_ID = tbt.TABLE_ID group by be_id;

-- テーブル
SELECT tbt.be_id, sum(tbt.DATA_SIZE) FROM information_schema.tables_config tb JOIN information_schema.be_tablets tbt ON tb.TABLE_ID = tbt.TABLE_ID WHERE tb.table_name = 't' group by be_id;
```

ノードレベルのスキューが検出された場合、分布キーとしてより高いカーディナリティの列を使用するか、テーブルにランダム分散戦略を採用することを検討してください。

##### タブレット間のデータスキュー

```SQL
select tablet_id,t.data_size,num_row,visible_version,num_version,num_rowset,num_segment,PARTITION_NAME from information_schema.partitions_meta m, information_schema.be_tablets t where t.partition_id = m.partition_id and m.partition_name = 'att' and m.table_name='att' order by t.data_size desc;
```

### ロードの一般的な監視メトリクス

#### BE Load

これらのメトリクスは Grafana の **BE Load** カテゴリで利用可能です。このカテゴリが見つからない場合は、[最新の Grafana ダッシュボードテンプレート](../../administration/management/monitoring/Monitor_and_Alert.md#125-ダッシュボードの設定) を使用していることを確認してください。

##### ThreadPool

これらのメトリクスは、スレッドプールの状態を分析するのに役立ちます。例えば、タスクがバックログされているか、どのくらいの時間待機しているかを確認できます。現在、4つのスレッドプールが監視されています：

- `async_delta_writer`
- `memtable_flush`
- `segment_replicate_sync`
- `segment_flush`

各スレッドプールには以下のメトリクスが含まれます：

| 名前        | 説明                                                                                               |
| ----------- | --------------------------------------------------------------------------------------------------------- |
| **rate**    | タスク処理率。                                                                                     |
| **pending** | タスクがキューで待機する時間。                                                                    |
| **execute** | タスクの実行時間。                                                                                      |
| **total**   | プールで利用可能なスレッドの最大数。                                                          |
| **util**    | 指定された期間のプール利用率。サンプリングの不正確さにより、負荷が高いときに 100% を超えることがあります。 |
| **count**   | キュー内のタスクの瞬間的な数。                                                               |

:::note
- バックログの信頼できる指標は、**pending duration** が増加し続けるかどうかです。**workers util** と **queue count** は必要ですが十分ではない指標です。
- バックログが発生した場合、**rate** と **execute duration** を使用して、負荷の増加によるものか処理の遅さによるものかを判断します。
- **workers util** はプールがどれだけ忙しいかを評価するのに役立ち、チューニングの指針となります。
:::

##### LoadChannel::add_chunks

これらのメトリクスは、`BRPC tablet_writer_add_chunks` リクエストを受信した後の `LoadChannel::add_chunks` の動作を分析するのに役立ちます。

| 名前              | 説明                                                                             |
| ----------------- | --------------------------------------------------------------------------------------- |
| **rate**          | `add_chunks` リクエストの処理率。                                               |
| **execute**       | `add_chunks` の平均実行時間。                                                 |
| **wait_memtable** | プライマリレプリカの MemTable フラッシュを待機する平均時間。                             |
| **wait_writer**   | プライマリレプリカの非同期デルタライターが書き込み/コミットを実行するのを待機する平均時間。 |
| **wait_replica**  | セカンダリレプリカがセグメントフラッシュを完了するのを待機する平均時間。                     |

:::note
- **latency** メトリクスは `wait_memtable`、`wait_writer`、`wait_replica` の合計に等しいです。
- 高い待機率は下流のボトルネックを示しており、さらなる分析が必要です。
:::

##### Async Delta Writer

これらのメトリクスは、**async delta writer** の動作を分析するのに役立ちます。

| 名前              | 説明                                       |
| ----------------- | ------------------------------------------------- |
| **rate**          | 書き込み/コミットタスクの処理率。            |
| **pending**       | スレッドプールキューで待機する時間。      |
| **execute**       | 単一タスクを処理する平均時間。            |
| **wait_memtable** | MemTable フラッシュを待機する平均時間。          |
| **wait_replica**  | セグメント同期を待機する平均時間。 |

:::note
- タスクごとの合計時間（上流の観点から）は **pending** と **execute** の合計に等しいです。
- **execute** はさらに **wait_memtable** と **wait_replica** を含みます。
- 高い **pending** 時間は **execute** が遅いかスレッドプールが小さいことを示す可能性があります。 
- **wait** が **execute** の大部分を占める場合、ボトルネックは下流のステージにあり、そうでない場合、ボトルネックはライターのロジック内にある可能性が高いです。
:::

##### MemTable Flush

これらのメトリクスは **MemTable flush** のパフォーマンスを分析します。

| 名前            | 説明                                  |
| --------------- | -------------------------------------------- |
| **rate**        | MemTables のフラッシュ率。                     |
| **memory-size** | 秒あたりにフラッシュされるメモリ内データの量。 |
| **disk-size**   | 秒あたりに書き込まれるディスクデータの量。      |
| **execute**     | タスクの実行時間。                         |
| **io**          | フラッシュタスクの I/O 時間。                  |

:::note
- **rate** と **size** を比較することで、ワークロードが変化しているか、大量のインポートが行われているかを判断できます。例えば、小さな **rate** で大きな **size** は大量のインポートを示します。
- 圧縮率は `memory-size / disk-size` を使用して推定できます。
- **io** 時間が **execute** の中でどの程度の割合を占めているかを確認することで、I/O がボトルネックかどうかを評価できます。
:::

##### Segment Replicate Sync

| 名前        | 説明                                  |
| ----------- | -------------------------------------------- |
| **rate**    | セグメント同期の率。             |
| **execute** | 単一のタブレットレプリカを同期する時間。 |

##### Segment Flush

これらのメトリクスは **segment flush** のパフォーマンスを分析します。

| 名前        | 説明                             |
| ----------- | --------------------------------------- |
| **rate**    | セグメントフラッシュの率。                     |
| **size**    | 秒あたりにフラッシュされるディスクデータの量。 |
| **execute** | タスクの実行時間。                    |
| **io**      | フラッシュタスクの I/O 時間。             |

:::note
- **rate** と **size** を比較することで、ワークロードが変化しているか、大量のインポートが行われているかを判断できます。例えば、小さな **rate** で大きな **size** は大量のインポートを示します。
- **io** 時間が **execute** の中でどの程度の割合を占めているかを確認することで、I/O がボトルネックかどうかを評価できます。
:::
