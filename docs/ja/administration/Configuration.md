---
displayed_sidebar: docs
---

# パラメーター設定

このトピックでは、FE、BE、およびシステムパラメーターについて説明します。また、これらのパラメーターを設定および調整するための提案も提供します。

## FE設定項目

FEパラメーターは動的パラメーターと静的パラメーターに分類されます。

- 動的パラメーターはSQLコマンドを実行することで設定および調整でき、非常に便利です。ただし、FEを再起動すると設定が無効になります。そのため、変更の損失を防ぐために、`fe.conf`ファイルの設定項目も変更することをお勧めします。

- 静的パラメーターはFE設定ファイル**fe.conf**でのみ設定および調整できます。**このファイルを変更した後、変更を有効にするにはFEを再起動する必要があります。**

パラメーターが動的パラメーターであるかどうかは、[ADMIN SHOW CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SHOW_CONFIG.md)の出力の`IsMutable`列で示されます。`TRUE`は動的パラメーターを示します。

動的および静的FEパラメーターの両方が**fe.conf**ファイルで設定できることに注意してください。

### FE設定項目の表示

FEが起動した後、MySQLクライアントでADMIN SHOW FRONTEND CONFIGコマンドを実行してパラメーター設定を確認できます。特定のパラメーターの設定を確認したい場合は、次のコマンドを実行します。

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

返されるフィールドの詳細な説明については、[ADMIN SHOW CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SHOW_CONFIG.md)を参照してください。

> **注意**
>
> クラスター管理関連のコマンドを実行するには、管理者権限が必要です。

### FE動的パラメーターの設定

[ADMIN SET FRONTEND CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SET_CONFIG.md)を使用して、FE動的パラメーターの設定を変更できます。

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

> **注意**
>
> FEを再起動すると、設定は`fe.conf`ファイルのデフォルト値に戻ります。そのため、変更の損失を防ぐために、`fe.conf`の設定項目も変更することをお勧めします。

#### ロギング

| パラメーター      | 単位 | デフォルト | 説明                                                  |
| -------------- | ---- | ------- | ------------------------------------------------------------ |
| qe_slow_log_ms | ms   | 5000    | クエリが遅いクエリであるかどうかを判断するために使用されるしきい値。クエリの応答時間がこのしきい値を超える場合、`fe.audit.log`に遅いクエリとして記録されます。 |

#### メタデータとクラスター管理

| パラメーター                        | 単位 | デフォルト | 説明                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| -------------------------------- | ---- | ------- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| catalog_try_lock_timeout_ms      | ms   | 5000    | グローバルロックを取得するためのタイムアウト時間。                                                                                                                                                                                                                                                                                                                                                                                                               |
| edit_log_roll_num                | -    | 50000   | ログエントリが書き込まれる前に作成されるメタデータログエントリの最大数。<br/>このパラメーターはログファイルのサイズを制御するために使用されます。新しいログファイルはBDBJEデータベースに書き込まれます。                                                                                                                                                                                                                            |
| ignore_unknown_log_id            | -    | FALSE   | 不明なログIDを無視するかどうか。FEがロールバックされると、以前のバージョンのBEsは一部のログIDを認識できない場合があります。<br/>値が`TRUE`の場合、FEは不明なログIDを無視します。値が`FALSE`の場合、FEは終了します。                                                                                                                                                                                                                     |
| ignore_materialized_view_error   | -    | FALSE   | FEがマテリアライズドビューエラーによって引き起こされるメタデータ例外を無視するかどうか。マテリアライズドビューエラーによって引き起こされるメタデータ例外のためにFEが起動に失敗した場合、このパラメーターを`true`に設定してFEが例外を無視できるようにします。このパラメーターはv2.5.10以降でサポートされています。                                                                                                                                                                                                                                                   |
| ignore_meta_check                | -    | FALSE   | 非リーダーFEがリーダーFEからのメタデータギャップを無視するかどうか。値がTRUEの場合、非リーダーFEはリーダーFEからのメタデータギャップを無視し、データ読み取りサービスを提供し続けます。<br/>このパラメーターは、リーダーFEを長時間停止してもデータ読み取りサービスを継続的に提供することを保証します。<br/>値がFALSEの場合、非リーダーFEはリーダーFEからのメタデータギャップを無視せず、データ読み取りサービスを停止します。 |
|meta_delay_toleration_second      | s    | 300     | フォロワーおよびオブザーバーFEのメタデータがリーダーFEのメタデータに遅れることが許容される最大期間。単位：秒。<br/>この期間を超えると、非リーダーFEはサービスを提供しなくなります。                                                                                                                                                                                                                                                |
| drop_backend_after_decommission  | -    | TRUE    | BEが退役後に削除されるかどうか。`TRUE`はBEが退役後すぐに削除されることを示します。<br/>`FALSE`はBEが退役後に削除されないことを示します。                                                                                                                                                                                                                                            |
| enable_collect_query_detail_info | -    | FALSE   | クエリのプロファイルを表示するかどうか。このパラメーターが`TRUE`に設定されている場合、システムはクエリのプロファイルを収集します。<br/>このパラメーターが`FALSE`に設定されている場合、システムはクエリのプロファイルを収集しません。                                                                                                                                                                                                                                       |
| enable_background_refresh_connector_metadata | -    | `true` in v3.0<br />`false` in v2.5  | 定期的なHiveメタデータキャッシュの更新を有効にするかどうか。有効にすると、StarRocksはHiveクラスターのメタストア（Hive MetastoreまたはAWS Glue）をポーリングし、頻繁にアクセスされるHiveカタログのキャッシュされたメタデータを更新してデータの変更を検知します。`true`はHiveメタデータキャッシュの更新を有効にすることを示し、`false`は無効にすることを示します。このパラメーターはv2.5.5以降でサポートされています。                                      |
| background_refresh_metadata_interval_millis         | ms   | 600000 | 2回の連続したHiveメタデータキャッシュ更新の間隔。このパラメーターはv2.5.5以降でサポートされています。                                                                                                                                                                                                                                                                                                                                          |
| background_refresh_metadata_time_secs_since_last_access_secs | s    | 86400  | Hiveメタデータキャッシュ更新タスクの有効期限。アクセスされたHiveカタログについて、指定された時間以上アクセスされていない場合、StarRocksはそのキャッシュされたメタデータの更新を停止します。アクセスされていないHiveカタログについては、StarRocksはそのキャッシュされたメタデータを更新しません。このパラメーターはv2.5.5以降でサポートされています。                                                                                       |

#### クエリエンジン

| パラメーター                                | 単位 | デフォルト      | 説明                                                  |
| ---------------------------------------- | ---- | ------------ | ------------------------------------------------------------ |
| max_allowed_in_element_num_of_delete     | -    | 10000        | DELETE文のIN述語に許可される要素の最大数。 |
| enable_materialized_view                 | -    | TRUE         | マテリアライズドビューの作成を有効にするかどうか。        |
| enable_decimal_v3                        | -    | TRUE         | DECIMAL V3データ型をサポートするかどうか。                 |
|  expr_children_limit                     | -    | 10000        | 式に許可される子式の最大数。 |
| enable_sql_blacklist                     | -    | FALSE        | SQLクエリのブラックリストチェックを有効にするかどうか。この機能が有効な場合、ブラックリストにあるクエリは実行できません。 |
| dynamic_partition_check_interval_seconds | s    | 600          | 新しいデータがチェックされる間隔。新しいデータが検出されると、StarRocksは自動的にデータのパーティションを作成します。 |
| dynamic_partition_enable                 | -    | TRUE         | 動的パーティション化機能を有効にするかどうか。この機能が有効な場合、StarRocksは新しいデータのパーティションを動的に作成し、期限切れのパーティションを自動的に削除してデータの新鮮さを確保します。 |
| max_partitions_in_one_batch              | -    | 4096         | パーティションを一括作成する際に作成できる最大パーティション数。 |
| max_query_retry_time                     | -    | 2            | FEでのクエリの最大再試行回数。                |
| max_create_table_timeout_second          | s    | 600          | テーブル作成の最大タイムアウト期間（秒単位）。 |
| create_table_max_serial_replicas         | -    | 128          | 直列に作成されるレプリカの最大数。実際のレプリカ数がこれを超える場合、レプリカは並行して作成されます。テーブル作成に時間がかかる場合は、この設定を減らすことをお勧めします。 |
| max_running_rollup_job_num_per_table     | -    | 1            | テーブルに対して並行して実行できるロールアップジョブの最大数。 |
| max_planner_scalar_rewrite_num           | -    | 100000       | オプティマイザがスカラ演算子をリライトできる最大回数。 |
| enable_statistic_collect                 | -    | TRUE         | CBOのために統計を収集するかどうか。この機能はデフォルトで有効です。 |
| enable_collect_full_statistic            | -    | TRUE         | 自動フル統計収集を有効にするかどうか。この機能はデフォルトで有効です。 |
| statistic_auto_collect_ratio             | -    | 0.8          | 自動収集のための統計が健全かどうかを判断するためのしきい値。統計の健全性がこのしきい値を下回る場合、自動収集がトリガーされます。 |
| statistic_max_full_collect_data_size     | GB   | 100          | 自動収集のためにデータを収集する最大パーティションサイズ。単位：GB。パーティションがこの値を超える場合、フル収集は破棄され、サンプル収集が行われます。 |
| statistic_collect_interval_sec           | s    | 300          | 自動収集中にデータ更新をチェックする間隔。単位：秒。 |
| statistic_auto_analyze_start_time | STRING      | 00:00:00   | 自動収集の開始時間。値の範囲：`00:00:00` - `23:59:59`。 |
| statistic_auto_analyze_end_time | STRING      | 23:59:59  | 自動収集の終了時間。値の範囲：`00:00:00` - `23:59:59`。 |
| statistic_sample_collect_rows            | -    | 200000       | サンプル収集のために収集する最小行数。パラメーター値がテーブルの実際の行数を超える場合、フル収集が行われます。 |
| histogram_buckets_size                   | -    | 64           | ヒストグラムのデフォルトバケット数。                   |
| histogram_mcv_size                       | -    | 100          | ヒストグラムの最も一般的な値（MCV）の数。      |
| histogram_sample_ratio                   | -    | 0.1          | ヒストグラムのサンプリング比率。                          |
| histogram_max_sample_row_count           | -    | 10000000     | ヒストグラムのために収集する最大行数。       |
| statistics_manager_sleep_time_sec        | s    | 60           | メタデータがスケジュールされる間隔。単位：秒。システムはこの間隔に基づいて次の操作を実行します：<ul><li>統計を保存するためのテーブルを作成します。</li><li>削除された統計を削除します。</li><li>期限切れの統計を削除します。</li></ul>|
| statistic_update_interval_sec            | s    | 24 \* 60 \* 60 | 統計情報のキャッシュが更新される間隔。単位：秒。 |
| statistic_analyze_status_keep_second     | s    | 259200       | 収集タスクの履歴を保持する期間。デフォルト値は3日です。単位：秒。 |
|statistic_collect_concurrency             | -    |  3  | 並行して実行できる手動収集タスクの最大数。デフォルト値は3で、最大3つの手動収集タスクを並行して実行できます。<br/>この値を超えると、受信タスクはPENDING状態になり、スケジュールを待ちます。|
| enable_local_replica_selection           | -    | FALSE        | クエリのためにローカルレプリカを選択するかどうか。ローカルレプリカはネットワーク伝送コストを削減します。このパラメーターがTRUEに設定されている場合、CBOは現在のFEと同じIPアドレスを持つBE上のtabletレプリカを優先的に選択します。このパラメーターがFALSEに設定されている場合、ローカルレプリカと非ローカルレプリカの両方が選択される可能性があります。デフォルト値はFALSEです。 |
| max_distribution_pruner_recursion_depth  | -    | 100          | パーティションプルーナーが許可する最大再帰深度。再帰深度を増やすと、より多くの要素をプルーニングできますが、CPU消費も増加します。 |
|enable_udf                                |  -   |    FALSE     | UDFを有効にするかどうか。                            |

#### ロードとアンロード

| パラメーター                               | 単位 | デフォルト                                         | 説明                                                  |
| --------------------------------------- | ---- | ----------------------------------------------- | ------------------------------------------------------------ |
| max_broker_load_job_concurrency         | -    | 5                                               | StarRocksクラスター内で許可されるBroker Loadジョブの最大同時実行数。このパラメーターはBroker Loadにのみ有効です。このパラメーターの値は`max_running_txn_num_per_db`の値より小さくなければなりません。v2.5以降、このパラメーターのデフォルト値は`10`から`5`に変更されました。このパラメーターの別名は`async_load_task_pool_size`です。|
| load_straggler_wait_second              | s    | 300                                             | BEレプリカによって許容される最大ロード遅延。この値を超えると、他のレプリカからデータをクローンするためにクローンが実行されます。単位：秒。 |
| desired_max_waiting_jobs                | -    | 1024                                            | FE内の保留中ジョブの最大数。この数はテーブル作成、ロード、スキーマ変更ジョブなどすべてのジョブを指します。FE内の保留中ジョブの数がこの値に達すると、FEは新しいロード要求を拒否します。このパラメーターは非同期ロードにのみ有効です。v2.5以降、このパラメーターのデフォルト値は100から1024に変更されました。|
| max_load_timeout_second                 | s    | 259200                                          | ロードジョブに許可される最大タイムアウト期間。この制限を超えると、ロードジョブは失敗します。この制限はすべてのタイプのロードジョブに適用されます。単位：秒。 |
| min_load_timeout_second                 | s    | 1                                               | ロードジョブに許可される最小タイムアウト期間。この制限はすべてのタイプのロードジョブに適用されます。単位：秒。 |
| max_running_txn_num_per_db              | -    | 100                                             | StarRocksクラスター内の各データベースで実行可能なロードトランザクションの最大数。デフォルト値は`100`です。<br/>データベースで実行中のロードトランザクションの実際の数がこのパラメーターの値を超えると、新しいロード要求は処理されません。同期ロードジョブの新しい要求は拒否され、非同期ロードジョブの新しい要求はキューに入れられます。このパラメーターの値を増やすことはお勧めしません。システムの負荷が増加するためです。 |
| load_parallel_instance_num              | -    | 1                                               | BE上の各ロードジョブの最大同時ロードインスタンス数。 |
| disable_load_job                        | -    | FALSE                                           | クラスターがエラーに遭遇したときにロードを無効にするかどうか。これにより、クラスターエラーによる損失を防ぎます。デフォルト値は`FALSE`で、ロードが無効になっていないことを示します。`TRUE`はロードが無効になり、クラスターが読み取り専用状態になることを示します。|
| history_job_keep_max_second             | s    | 604800                                          | スキーマ変更ジョブなどの履歴ジョブを保持できる最大期間（秒単位）。 |
| label_keep_max_num                      | -    | 1000                                            | 一定期間内に保持できるロードジョブの最大数。この数を超えると、履歴ジョブの情報が削除されます。 |
| label_keep_max_second                   | s    | 259200                                          | 完了したロードジョブのラベルがStarRocksシステム内で保持される最大期間。このパラメーターはすべてのタイプのロードジョブに適用されます。デフォルト値は3日です。この期間が経過すると、ラベルは削除されます。単位：秒。値が大きすぎると、多くのメモリを消費します。 |
| max_routine_load_job_num                | -    | 100                                             | StarRocksクラスター内のRoutine Loadジョブの最大数。 |
| max_routine_load_task_concurrent_num    | -    | 5                                               | 各Routine Loadジョブの最大同時タスク数。 |
| max_routine_load_task_num_per_be        | -    | 5                                               | 各BEで実行できる最大同時Routine Loadタスク数。この値はBE設定項目`routine_load_thread_pool_size`の値以下でなければなりません。 |
| max_routine_load_batch_size             | Byte | 4294967296                                      | Routine Loadタスクによってロードできるデータの最大量（バイト単位）。 |
| routine_load_task_consume_second        | s    | 15                                              | 各Routine Loadタスクがデータを消費できる最大期間（秒単位）。 |
| routine_load_task_timeout_second        | s    | 60                                              | 各Routine Loadタスクのタイムアウト期間（秒単位）。 |
| routine_load_unstable_threshold_second | s     | 3600  | Routine Loadジョブ内のタスクが遅延した場合、Routine LoadジョブはUNSTABLE状態に設定されます。具体的には、消費されているメッセージのタイムスタンプと現在の時間の差がこのしきい値を超え、データソースに未消費のメッセージが存在する場合です。| 
| max_tolerable_backend_down_num          | -    | 0                                               | 許容される故障BEノードの最大数。この数を超えると、Routine Loadジョブは自動的に回復できません。 |
| period_of_auto_resume_min               | Min  | 5                                               | Routine Loadジョブが自動的に回復される間隔（分単位）。 |
| spark_load_default_timeout_second       | s    | 86400                                           | 各Spark Loadジョブのタイムアウト期間（秒単位）。    |
| spark_home_default_dir                  | -    | StarRocksFE.STARROCKS_HOME_DIR + "/lib/spark2x" | Sparkクライアントのルートディレクトリ。                        |
| stream_load_default_timeout_second      | s    | 600                                             | 各Stream Loadジョブのデフォルトタイムアウト期間（秒単位）。 |
| max_stream_load_timeout_second          | s    | 259200                                          | Stream Loadジョブに許可される最大タイムアウト期間（秒単位）。 |
| insert_load_default_timeout_second      | s    | 3600                                            | データをロードするために使用されるINSERT INTO文のタイムアウト期間（秒単位）。 |
| broker_load_default_timeout_second      | s    | 14400                                           | Broker Loadジョブのタイムアウト期間（秒単位）。      |
| min_bytes_per_broker_scanner            | Byte | 67108864                                        | Broker Loadインスタンスが処理できるデータの最小量（バイト単位）。 |
| max_broker_concurrency                  | -    | 100                                             | Broker Loadタスクの最大同時インスタンス数。 |
| export_max_bytes_per_be_per_task        | Byte | 268435456                                       | 単一のBEから単一のデータアンロードタスクによってエクスポートできるデータの最大量（バイト単位）。 |
| export_running_job_num_limit            | -    | 5                                               | 並行して実行できるデータエクスポートタスクの最大数。 |
| export_task_default_timeout_second      | s    | 7200                                            | データエクスポートタスクのタイムアウト期間（秒単位）。  |
| empty_load_as_error                     | -    | TRUE                                            | データがロードされていない場合にエラーメッセージ「すべてのパーティションにロードデータがありません」を返すかどうか。値:<br/> - TRUE: データがロードされていない場合、システムは失敗メッセージを表示し、「すべてのパーティションにロードデータがありません」というエラーを返します。<br/> - FALSE: データがロードされていない場合、システムは成功メッセージを表示し、エラーではなくOKを返します。 |
| external_table_commit_timeout_ms        | ms    | 10000                                          | StarRocks外部テーブルへの書き込みトランザクションをコミット（公開）するためのタイムアウト期間。デフォルト値`10000`は10秒のタイムアウト期間を示します。 |

#### ストレージ

| パラメーター                                     | 単位 | デフォルト                | 説明                                                  |
| --------------------------------------------- | ---- | ---------------------- | ------------------------------------------------------------ |
| enable_strict_storage_medium_check            | -    | FALSE                  | ユーザーがテーブルを作成する際に、FEがBEの記憶媒体を厳密にチェックするかどうか。このパラメーターが`TRUE`に設定されている場合、ユーザーがテーブルを作成する際にFEはBEの記憶媒体をチェックし、CREATE TABLE文で指定された`storage_medium`パラメーターと異なる場合はエラーを返します。たとえば、CREATE TABLE文で指定された記憶媒体がSSDであるが、実際のBEの記憶媒体がHDDである場合、テーブル作成は失敗します。このパラメーターが`FALSE`の場合、ユーザーがテーブルを作成する際にFEはBEの記憶媒体をチェックしません。 |
| enable_auto_tablet_distribution               | -    | TRUE                   | バケット数を自動的に設定するかどうか。<ul><li>このパラメーターが`TRUE`に設定されている場合、テーブルを作成する際やパーティションを追加する際にバケット数を指定する必要はありません。StarRocksが自動的にバケット数を決定します。バケット数を自動的に設定する戦略については、[バケット数の決定](../table_design/Data_distribution.md#determine-the-number-of-buckets)を参照してください。</li><li>このパラメーターが`FALSE`に設定されている場合、テーブルを作成する際やパーティションを追加する際にバケット数を手動で指定する必要があります。テーブルに新しいパーティションを追加する際にバケット数を指定しない場合、新しいパーティションはテーブル作成時に設定されたバケット数を継承します。ただし、新しいパーティションのバケット数を手動で指定することもできます。</li></ul>バージョン2.5.7以降、StarRocksはこのパラメーターの設定をサポートしています。|
| storage_usage_soft_limit_percent              | %    | 90                     | BEディレクトリ内のストレージ使用率のソフトリミット。BEストレージディレクトリのストレージ使用率（パーセンテージ）がこの値を超え、残りのストレージスペースが`storage_usage_soft_limit_reserve_bytes`未満の場合、tabletはこのディレクトリにクローンできません。 |
| storage_usage_soft_limit_reserve_bytes        | Byte | 200 \* 1024 \* 1024 \* 1024 | BEディレクトリ内の残りストレージスペースのソフトリミット。BEストレージディレクトリの残りストレージスペースがこの値未満で、ストレージ使用率（パーセンテージ）が`storage_usage_soft_limit_percent`を超える場合、tabletはこのディレクトリにクローンできません。 |
| storage_usage_hard_limit_percent              | %    | 95                     | BEディレクトリ内のストレージ使用率のハードリミット。BEストレージディレクトリのストレージ使用率（パーセンテージ）がこの値を超え、残りのストレージスペースが`storage_usage_hard_limit_reserve_bytes`未満の場合、LoadおよびRestoreジョブは拒否されます。この項目をBE設定項目`storage_flood_stage_usage_percent`と一緒に設定する必要があります。 |
| storage_usage_hard_limit_reserve_bytes        | Byte | 100 \* 1024 \* 1024 \* 1024 | BEディレクトリ内の残りストレージスペースのハードリミット。BEストレージディレクトリの残りストレージスペースがこの値未満で、ストレージ使用率（パーセンテージ）が`storage_usage_hard_limit_percent`を超える場合、LoadおよびRestoreジョブは拒否されます。この項目をBE設定項目`storage_flood_stage_left_capacity_bytes`と一緒に設定する必要があります。 |
| catalog_trash_expire_second                   | s    | 86400                  | テーブルまたはデータベースが削除された後、メタデータが保持される最長期間。この期間が経過すると、データは削除され、回復できなくなります。単位：秒。 |
| alter_table_timeout_second                    | s    | 86400                  | スキーマ変更操作（ALTER TABLE）のタイムアウト期間。単位：秒。 |
| recover_with_empty_tablet                     | -    | FALSE                  | 失われたまたは破損したtabletレプリカを空のものと置き換えるかどうか。tabletレプリカが失われたり破損したりすると、このtabletまたは他の健康なtabletでのデータクエリが失敗する可能性があります。失われたまたは破損したtabletレプリカを空のtabletと置き換えることで、クエリを実行し続けることができます。ただし、データが失われているため、結果が正しくない可能性があります。デフォルト値は`FALSE`で、失われたまたは破損したtabletレプリカは空のものと置き換えられず、クエリは失敗します。 |
| tablet_create_timeout_second                  | s    | 10                      | tabletを作成するためのタイムアウト期間（秒単位）。       |
| tablet_delete_timeout_second                  | s    | 2                      | tabletを削除するためのタイムアウト期間（秒単位）。      |
| check_consistency_default_timeout_second      | s    | 600                    | レプリカの一貫性チェックのタイムアウト期間。tabletのサイズに基づいてこのパラメーターを設定できます。 |
| tablet_sched_slot_num_per_path                | -    | 8                      | BEストレージディレクトリで同時に実行できるtablet関連タスクの最大数。別名は`schedule_slot_num_per_path`です。v2.5以降、このパラメーターのデフォルト値は`4`から`8`に変更されました。|
| tablet_sched_max_scheduling_tablets           | -    | 10000                  | 同時にスケジュールできるtabletの最大数。この値を超えると、tabletのバランス調整と修復チェックがスキップされます。 |
| tablet_sched_disable_balance                  | -    | FALSE                  | tabletのバランス調整を無効にするかどうか。`TRUE`はtabletのバランス調整が無効であることを示します。`FALSE`はtabletのバランス調整が有効であることを示します。別名は`disable_balance`です。 |
| tablet_sched_disable_colocate_balance         | -    | FALSE                  | Colocate Tableのレプリカバランス調整を無効にするかどうか。`TRUE`はレプリカバランス調整が無効であることを示します。`FALSE`はレプリカバランス調整が有効であることを示します。別名は`disable_colocate_balance`です。 |
| tablet_sched_max_balancing_tablets            | -    | 500                    | 同時にバランス調整できるtabletの最大数。この値を超えると、tabletの再バランス調整がスキップされます。別名は`max_balancing_tablets`です。 |
| tablet_sched_balance_load_disk_safe_threshold | -    | 0.5                    | BEディスク使用率がバランスされているかどうかを判断するためのしきい値。このパラメーターは`tablet_sched_balancer_strategy`が`disk_and_tablet`に設定されている場合にのみ有効です。すべてのBEのディスク使用率が50％未満の場合、ディスク使用率はバランスされていると見なされます。`disk_and_tablet`ポリシーでは、最高と最低のBEディスク使用率の差が10％を超える場合、ディスク使用率はバランスされていないと見なされ、tabletの再バランス調整がトリガーされます。別名は`balance_load_disk_safe_threshold`です。 |
| tablet_sched_balance_load_score_threshold     | -    | 0.1                    | BE負荷がバランスされているかどうかを判断するためのしきい値。このパラメーターは`tablet_sched_balancer_strategy`が`be_load_score`に設定されている場合にのみ有効です。負荷が平均負荷より10％低いBEは低負荷状態にあり、負荷が平均負荷より10％高いBEは高負荷状態にあります。別名は`balance_load_score_threshold`です。 |
| tablet_sched_repair_delay_factor_second       | s    | 60                     | レプリカが修復される間隔（秒単位）。別名は`tablet_repair_delay_factor_second`です。 |
| tablet_sched_min_clone_task_timeout_sec       | s    | 3 \* 60                | tabletをクローンするための最小タイムアウト期間（秒単位）。 |
| tablet_sched_max_clone_task_timeout_sec       | s    | 2 \* 60 \* 60          | tabletをクローンするための最大タイムアウト期間（秒単位）。別名は`max_clone_task_timeout_sec`です。 |
| tablet_sched_max_not_being_scheduled_interval_ms | ms   | 15 \* 60 \* 100 | tabletクローンタスクがスケジュールされている場合、このパラメーターで指定された時間の間にtabletがスケジュールされていない場合、StarRocksはそれに優先順位を与え、できるだけ早くスケジュールします。 |
| tablet_sched_be_down_tolerate_time_s | s   | 900 | スケジューラがBEノードを非アクティブ状態にしておくことを許容する最大期間。この時間のしきい値に達すると、そのBEノード上のtabletは他のアクティブなBEノードに移動されます。 |

#### その他のFE動的パラメーター

| パラメーター                                | 単位 | デフォルト     | 説明                                                  |
| ---------------------------------------- | ---- | ----------- | ------------------------------------------------------------ |
| plugin_enable                            | -    | TRUE        | プラグインがFEにインストールできるかどうか。プラグインはLeader FEにのみインストールまたはアンインストールできます。 |
| max_small_file_number                    | -    | 100         | FEディレクトリに保存できる小さなファイルの最大数。 |
| max_small_file_size_bytes                | Byte | 1024 * 1024 | 小さなファイルの最大サイズ（バイト単位）。                  |
| agent_task_resend_wait_time_ms           | ms   | 5000        | エージェントタスクを再送信する前にFEが待機する期間。エージェントタスクは、タスク作成時間と現在の時間の差がこのパラメーターの値を超えた場合にのみ再送信されます。このパラメーターはエージェントタスクの繰り返し送信を防ぐために使用されます。単位：ms。 |
| backup_job_default_timeout_ms            | ms   | 86400*1000  | バックアップジョブのタイムアウト期間（ms単位）。この値を超えると、バックアップジョブは失敗します。 |
| report_queue_size                        | -    | 100         | レポートキューで待機できるジョブの最大数。<br/>レポートはBEのディスク、タスク、およびtablet情報に関するものです。キューにレポートジョブが多すぎると、OOMが発生します。 |
| enable_experimental_mv                   | -    | TRUE       | 非同期マテリアライズドビュー機能を有効にするかどうか。`TRUE`はこの機能が有効であることを示します。v2.5.2以降、この機能はデフォルトで有効になっています。v2.5.2以前のバージョンでは、この機能はデフォルトで無効です。 |
| authentication_ldap_simple_bind_base_dn  | -  | 空文字列 | LDAPサーバーがユーザーの認証情報を検索し始める基点であるベースDN。|
| authentication_ldap_simple_bind_root_dn  |  -  | 空文字列 | ユーザーの認証情報を検索するために使用される管理者DN。|
| authentication_ldap_simple_bind_root_pwd |  -  | 空文字列 | ユーザーの認証情報を検索するために使用される管理者のパスワード。|
| authentication_ldap_simple_server_host   |  -  | 空文字列 |  LDAPサーバーが実行されているホスト。                               |
| authentication_ldap_simple_server_port   |  -  | 389     | LDAPサーバーのポート。                                       |
| authentication_ldap_simple_user_search_attr|  -  | uid     | LDAPオブジェクト内のユーザーを識別する属性の名前。|
|max_upload_task_per_be                    |  -  | 0       | 各BACKUP操作で、StarRocksがBEノードに割り当てるアップロードタスクの最大数。この項目が0以下に設定されている場合、タスク数に制限はありません。この項目はv2.5.7以降でサポートされています。     |
|max_download_task_per_be                  |  -  | 0       | 各RESTORE操作で、StarRocksがBEノードに割り当てるダウンロードタスクの最大数。この項目が0以下に設定されている場合、タスク数に制限はありません。この項目はv2.5.7以降でサポートされています。     |
|allow_system_reserved_names               | FALSE | ユーザーが`__op`および`__row`で始まる名前の列を作成できるかどうか。この機能を有効にするには、このパラメーターを`TRUE`に設定します。これらの名前形式はStarRocksで特別な目的のために予約されているため、そのような列を作成すると未定義の動作が発生する可能性があります。したがって、この機能はデフォルトで無効です。この項目はv2.5.15以降でサポートされています。|
|default_mv_refresh_immediate              | TRUE  | 非同期マテリアライズドビューを作成後すぐに更新するかどうか。この項目が`true`に設定されている場合、新しく作成されたマテリアライズドビューはすぐに更新されます。この項目はv2.5.18以降でサポートされています。|

### FE静的パラメーターの設定

このセクションでは、FE設定ファイル**fe.conf**で設定できる静的パラメーターの概要を説明します。これらのパラメーターをFEに再設定した後、変更を有効にするにはFEを再起動する必要があります。

#### ロギング

| パラメーター               | デフォルト                                 | 説明                                                  |
| ----------------------- | --------------------------------------- | ------------------------------------------------------------ |
| log_roll_size_mb        | 1024                                    | ログファイルごとのサイズ。単位：MB。デフォルト値`1024`は、ログファイルごとのサイズを1GBとして指定します。 |
| sys_log_dir             | StarRocksFE.STARROCKS_HOME_DIR + "/log" | システムログファイルを保存するディレクトリ。                  |
| sys_log_level           | INFO                                    | システムログエントリが分類される重大度レベル。 有効な値：`INFO`、`WARN`、`ERROR`、および`FATAL`。 |
| sys_log_verbose_modules | 空文字列                            | StarRocksがシステムログを生成するモジュール。このパラメーターが`org.apache.starrocks.catalog`に設定されている場合、StarRocksはカタログモジュールのシステムログのみを生成します。 |
| sys_log_roll_interval   | DAY                                     | StarRocksがシステムログエントリをローテーションする時間間隔。 有効な値：`DAY`および`HOUR`。<ul><li>このパラメーターが`DAY`に設定されている場合、システムログファイルの名前に`yyyyMMdd`形式のサフィックスが追加されます。</li><li>このパラメーターが`HOUR`に設定されている場合、システムログファイルの名前に`yyyyMMddHH`形式のサフィックスが追加されます。</li></ul> |
| sys_log_delete_age      | 7d                                      | システムログファイルの保持期間。デフォルト値`7d`は、各システムログファイルが7日間保持されることを指定します。StarRocksは各システムログファイルをチェックし、7日前に生成されたものを削除します。 |
| sys_log_roll_num        | 10                                      | `sys_log_roll_interval`パラメーターで指定された各保持期間内に保持できるシステムログファイルの最大数。 |
| audit_log_dir           | StarRocksFE.STARROCKS_HOME_DIR + "/log" | 監査ログファイルを保存するディレクトリ。                   |
| audit_log_roll_num      | 90                                      | `audit_log_roll_interval`パラメーターで指定された各保持期間内に保持できる監査ログファイルの最大数。 |
| audit_log_modules       | slow_query, query                       | StarRocksが監査ログエントリを生成するモジュール。デフォルトでは、StarRocksはslow_queryモジュールとqueryモジュールの監査ログを生成します。モジュール名はカンマ（,）とスペースで区切ります。 |
| audit_log_roll_interval | DAY                                     | StarRocksが監査ログエントリをローテーションする時間間隔。 有効な値：`DAY`および`HOUR`。<ul><li>このパラメーターが`DAY`に設定されている場合、監査ログファイルの名前に`yyyyMMdd`形式のサフィックスが追加されます。</li><li>このパラメーターが`HOUR`に設定されている場合、監査ログファイルの名前に`yyyyMMddHH`形式のサフィックスが追加されます。</li></ul> |
| audit_log_delete_age    | 30d                                     | 監査ログファイルの保持期間。デフォルト値`30d`は、各監査ログファイルが30日間保持されることを指定します。StarRocksは各監査ログファイルをチェックし、30日前に生成されたものを削除します。 |
| dump_log_dir            | StarRocksFE.STARROCKS_HOME_DIR + "/log" | ダンプログファイルを保存するディレクトリ。                    |
| dump_log_modules        | query                                   | StarRocksがダンプログエントリを生成するモジュール。デフォルトでは、StarRocksはqueryモジュールのダンプログを生成します。モジュール名はカンマ（,）とスペースで区切ります。 |
| dump_log_roll_interval  | DAY                                     | StarRocksがダンプログエントリをローテーションする時間間隔。 有効な値：`DAY`および`HOUR`。<ul><li>このパラメーターが`DAY`に設定されている場合、ダンプログファイルの名前に`yyyyMMdd`形式のサフィックスが追加されます。</li><li>このパラメーターが`HOUR`に設定されている場合、ダンプログファイルの名前に`yyyyMMddHH`形式のサフィックスが追加されます。</li></ul> |
| dump_log_roll_num       | 10                                      | `dump_log_roll_interval`パラメーターで指定された各保持期間内に保持できるダンプログファイルの最大数。 |
| dump_log_delete_age     | 7d                                      | ダンプログファイルの保持期間。デフォルト値`7d`は、各ダンプログファイルが7日間保持されることを指定します。StarRocksは各ダンプログファイルをチェックし、7日前に生成されたものを削除します。 |

#### サーバー

| パラメーター                            | デフォルト           | 説明                                                                                                                                                                                                                                                                                                                                                                                               |
|--------------------------------------|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| frontend_address                     | 0.0.0.0           | FEノードのIPアドレス。                                                                                                                                                                                                                                                                                                                                                                            |
| priority_networks                    | 空文字列      | 複数のIPアドレスを持つサーバーの選択戦略を宣言します。このパラメーターで指定されたリストと一致するIPアドレスは最大で1つでなければなりません。このパラメーターの値は、セミコロン（;）で区切られたCIDR表記のエントリからなるリストです。たとえば、10.10.10.0/24です。このリストのエントリと一致するIPアドレスがない場合、IPアドレスはランダムに選択されます。 |
| http_port                            | 8030              | FEノード内のHTTPサーバーがリッスンするポート。                                                                                                                                                                                                                                                                                                                                                 |
| http_worker_threads_num              | 0                 | HTTPサーバーがHTTPリクエストを処理するためのワーカースレッドの数。負の値または0の場合、スレッド数はCPUコア数の2倍になります。導入されたバージョン: 2.5.18，3.0.10，3.1.7，3.2.2.                                                                                 |
| http_backlog_num                     | 1024              | FEノード内のHTTPサーバーが保持するバックログキューの長さ。                                                                                                                                                                                                                                                                                                                                   |
| cluster_name                         | StarRocks Cluster | FEが属するStarRocksクラスターの名前。クラスター名はWebページの`Title`に表示されます。                                                                                                                                                                                                                                                                                     |
| rpc_port                             | 9020              | FEノード内のThriftサーバーがリッスンするポート。                                                                                                                                                                                                                                                                                                                                               |
| thrift_backlog_num                   | 1024              | FEノード内のThriftサーバーが保持するバックログキューの長さ。                                                                                                                                                                                                                                                                                                                                 |
| thrift_server_max_worker_threads     | 4096              | FEノード内のThriftサーバーがサポートするワーカースレッドの最大数。                                                                                                                                                                                                                                                                                                              |
| thrift_client_timeout_ms             | 5000              | アイドルクライアント接続がタイムアウトするまでの時間。単位：ms。                                                                                                                                                                                                                                                                                                                                |
| thrift_server_queue_size             | 4096              | リクエストが保留中のキューの長さ。Thriftサーバーで処理中のスレッド数が`thrift_server_max_worker_threads`で指定された値を超える場合、新しいリクエストは保留キューに追加されます。                                                                                                                                                                    |
| brpc_idle_wait_max_time              | 10000             | bRPCクライアントがアイドル状態で待機する最大時間。単位：ms。                                                                                                                                                                                                                                                                                                                    |
| query_port                           | 9030              | FEノード内のMySQLサーバーがリッスンするポート。                                                                                                                                                                                                                                                                                                                                                |
| mysql_service_nio_enabled            | TRUE              | FEノードの非同期I/Oが有効かどうかを指定します。                                                                                                                                                                                                                                                                                                                                            |
| mysql_service_io_threads_num         | 4                 | FEノード内のMySQLサーバーがI/Oイベントを処理するために実行できる最大スレッド数。                                                                                                                                                                                                                                                                                                   |
| mysql_nio_backlog_num                | 1024              | FEノード内のMySQLサーバーが保持するバックログキューの長さ。                                                                                                                                                                                                                                                                                                                                  |
| max_mysql_service_task_threads_num   | 4096              | FEノード内のMySQLサーバーがタスクを処理するために実行できる最大スレッド数。                                                                                                                                                                                                                                                                                                        |
| mysql_server_version                 | 5.1.0             | クライアントに返されるMySQLサーバーバージョン。このパラメーターを変更すると、次の状況でバージョン情報に影響します： 1. `select version();` 2. ハンドシェイクパケットバージョン 3. グローバル変数`version`の値（`show variables like 'version';`）                                                                                                                                                                                      |
| max_connection_scheduler_threads_num | 4096              | 接続スケジューラがサポートする最大スレッド数。                                                                                                                                                                                                                                                                                                                             |
| qe_max_connection                    | 1024              | すべてのユーザーがFEノードに確立できる最大接続数。                                                                                                                                                                                                                                                                                                                    |
| check_java_version                   | TRUE              | 実行されたJavaプログラムとコンパイルされたJavaプログラムのバージョン互換性をチェックするかどうかを指定します。バージョンが互換性がない場合、StarRocksはエラーを報告し、Javaプログラムの起動を中止します。                                                                                                                                                                                                     |

#### メタデータとクラスター管理

| パラメーター                         | デフォルト                                  | 説明                                                  |
| --------------------------------- | ---------------------------------------- | ------------------------------------------------------------ |
| meta_dir                          | StarRocksFE.STARROCKS_HOME_DIR + "/meta" | メタデータを保存するディレクトリ。                          |
| heartbeat_mgr_threads_num         | 8                                        | ハートビートタスクを実行するためにハートビートマネージャーが実行できるスレッド数。 |
| heartbeat_mgr_blocking_queue_size | 1024                                     | ハートビートマネージャーが実行するハートビートタスクを保存するブロッキングキューのサイズ。 |
| metadata_failure_recovery         | FALSE                                    | FEのメタデータを強制的にリセットするかどうかを指定します。このパラメーターを設定する際には注意が必要です。 |
| edit_log_port                     | 9010                                     | StarRocksクラスター内のリーダー、フォロワー、およびオブザーバーFE間の通信に使用されるポート。 |
| edit_log_type                     | BDB                                      | 生成できる編集ログのタイプ。値を`BDB`に設定します。 |
| bdbje_heartbeat_timeout_second    | 30                                       | StarRocksクラスター内のリーダー、フォロワー、およびオブザーバーFE間のハートビートがタイムアウトするまでの時間。単位：秒。 |
| bdbje_lock_timeout_second         | 1                                        | BDB JEベースのFEでロックがタイムアウトするまでの時間。単位：秒。 |
| max_bdbje_clock_delta_ms          | 5000                                     | StarRocksクラスター内のリーダーFEとフォロワーまたはオブザーバーFE間で許容される最大クロックオフセット。単位：ms。 |
| txn_rollback_limit                | 100                                      | ロールバックできるトランザクションの最大数。  |
| bdbje_replica_ack_timeout_second  | 10                                       | メタデータがリーダーFEからフォロワーFEに書き込まれるときに、リーダーFEが指定された数のフォロワーFEからACKメッセージを待つ最大時間。単位：秒。大量のメタデータが書き込まれている場合、フォロワーFEはACKメッセージをリーダーFEに返すまでに長い時間がかかり、ACKタイムアウトが発生します。この状況では、メタデータの書き込みが失敗し、FEプロセスが終了します。この状況を防ぐために、このパラメーターの値を増やすことをお勧めします。 |
| master_sync_policy                | SYNC                                     | リーダーFEがログをディスクにフラッシュするポリシー。このパラメーターは、現在のFEがリーダーFEである場合にのみ有効です。有効な値：<ul><li>`SYNC`: トランザクションがコミットされると、ログエントリが生成され、同時にディスクにフラッシュされます。</li><li>`NO_SYNC`: トランザクションがコミットされるときに、ログエントリの生成とフラッシュは同時に行われません。</li><li>`WRITE_NO_SYNC`: トランザクションがコミットされると、ログエントリが同時に生成されますが、ディスクにはフラッシュされません。</li></ul>フォロワーFEを1つだけデプロイしている場合、このパラメーターを`SYNC`に設定することをお勧めします。フォロワーFEを3つ以上デプロイしている場合、このパラメーターと`replica_sync_policy`の両方を`WRITE_NO_SYNC`に設定することをお勧めします。 |
| replica_sync_policy               | SYNC                                     | フォロワーFEがログをディスクにフラッシュするポリシー。このパラメーターは、現在のFEがフォロワーFEである場合にのみ有効です。有効な値：<ul><li>`SYNC`: トランザクションがコミットされると、ログエントリが生成され、同時にディスクにフラッシュされます。</li><li>`NO_SYNC`: トランザクションがコミットされるときに、ログエントリの生成とフラッシュは同時に行われません。</li><li>`WRITE_NO_SYNC`: トランザクションがコミットされると、ログエントリが同時に生成されますが、ディスクにはフラッシュされません。</li></ul> |
| replica_ack_policy                | SIMPLE_MAJORITY                          | ログエントリが有効と見なされるポリシー。デフォルト値`SIMPLE_MAJORITY`は、フォロワーFEの過半数がACKメッセージを返すと、ログエントリが有効と見なされることを指定します。 |
| cluster_id                        | -1                                       | FEが属するStarRocksクラスターのID。同じクラスターIDを持つFEまたはBEは、同じStarRocksクラスターに属します。有効な値：任意の正の整数。デフォルト値`-1`は、クラスターのリーダーFEが初めて起動されたときに、StarRocksがStarRocksクラスターのランダムなクラスターIDを生成することを指定します。 |

#### クエリエンジン

| パラメーター                   | デフォルト | 説明                                                  |
| --------------------------- |---------| ------------------------------------------------------------ |
| publish_version_interval_ms | 10      | リリース検証タスクが発行される時間間隔。単位：ms。 |
| statistic_cache_columns     | 100000  | 統計テーブルにキャッシュできる行数。 |
| statistic_cache_thread_pool_size     | 10      | 統計キャッシュを更新するために使用されるスレッドプールのサイズ。 |

#### ロードとアンロード

| パラメーター                         | デフォルト                                                      | 説明                                                  |
| --------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| async_load_task_pool_size         | 2                                                            | ロードタスクスレッドプールのサイズ。このパラメーターはBroker Loadにのみ有効です。この値は`max_running_txn_num_per_db`より小さくなければなりません。v2.5以降、このパラメーターはFE動的パラメーターである`max_broker_load_job_concurrency`に名前が変更され、デフォルト値は`10`から`2`に変更されました。   |
| load_checker_interval_second      | 5                                                            | ロードジョブがローリングベースで処理される時間間隔。単位：秒。 |
| transaction_clean_interval_second | 30                                                           | 完了したトランザクションがクリーンアップされる時間間隔。単位：秒。完了したトランザクションがタイムリーにクリーンアップされるように、短い時間間隔を指定することをお勧めします。 |
| label_clean_interval_second       | 14400                                                        | ラベルがクリーンアップされる時間間隔。単位：秒。履歴ラベルがタイムリーにクリーンアップされるように、短い時間間隔を指定することをお勧めします。 |
| spark_dpp_version                 | 1.0.0                                                        | 使用されるSpark Dynamic Partition Pruning（DPP）のバージョン。   |
| spark_resource_path               | 空文字列                                                 | Spark依存パッケージのルートディレクトリ。          |
| spark_launcher_log_dir            | sys_log_dir + "/spark_launcher_log"                          | Sparkログファイルを保存するディレクトリ。                   |
| yarn_client_path                  | StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-client/hadoop/bin/yarn" | Yarnクライアントパッケージのルートディレクトリ。               |
| yarn_config_dir                   | StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-config"          | Yarn設定ファイルを保存するディレクトリ。       |
| export_checker_interval_second    | 5                                                            | ロードジョブがスケジュールされる時間間隔。          |
| export_task_pool_size             | 5                                                            | アンロードタスクスレッドプールのサイズ。                     |

#### ストレージ

| パラメーター                            | デフォルト         | 説明                                                  |
| ------------------------------------ | --------------- | ------------------------------------------------------------ |
| default_storage_medium               | HDD             | テーブルまたはパーティションの作成時に指定されていない場合に使用されるデフォルトの記憶媒体。有効な値：`HDD`および`SSD`。テーブルまたはパーティションを作成する際に、テーブルまたはパーティションの記憶媒体タイプを指定しない場合、このパラメーターで指定されたデフォルトの記憶媒体が使用されます。 |
| tablet_sched_balancer_strategy       | disk_and_tablet | tablet間でロードバランシングが実装されるポリシー。このパラメーターの別名は`tablet_balancer_strategy`です。有効な値：`disk_and_tablet`および`be_load_score`。 |
| tablet_sched_storage_cooldown_second | -1              | テーブル作成時からの自動冷却の遅延。このパラメーターの別名は`storage_cooldown_second`です。単位：秒。デフォルト値`-1`は自動冷却が無効であることを指定します。自動冷却を有効にしたい場合、このパラメーターを`-1`より大きい値に設定します。 |
| tablet_stat_update_interval_second   | 300             | FEが各BEからtablet統計を取得する時間間隔。単位：秒。 |

#### その他のFE静的パラメーター

| パラメーター                          | デフォルト                                         | 説明                                                  |
| ---------------------------------- | ----------------------------------------------- | ------------------------------------------------------------ |
| plugin_dir                         | STARROCKS_HOME_DIR/plugins                      | プラグインインストールパッケージを保存するディレクトリ。      |
| small_file_dir                     | StarRocksFE.STARROCKS_HOME_DIR + "/small_files" | 小さなファイルのルートディレクトリ。                           |
| max_agent_task_threads_num         | 4096                                            | エージェントタスクスレッドプールで許可される最大スレッド数。 |
| auth_token                         | 空文字列                                    | FEが属するStarRocksクラスター内でのID認証に使用されるトークン。このパラメーターが指定されていない場合、StarRocksはクラスターのリーダーFEが初めて起動されたときにクラスターのランダムなトークンを生成します。 |
| tmp_dir                            | StarRocksFE.STARROCKS_HOME_DIR + "/temp_dir"    | バックアップおよびリストア手順中に生成されたファイルなどの一時ファイルを保存するディレクトリ。これらの手順が終了すると、生成された一時ファイルは削除されます。 |
| locale                             | zh_CN.UTF-8                                     | FEが使用する文字セット。                    |
| hive_meta_load_concurrency         | 4                                               | Hiveメタデータに対してサポートされる最大同時スレッド数。 |
| hive_meta_cache_refresh_interval_s | 7200                                            | Hive外部テーブルのキャッシュされたメタデータが更新される時間間隔。単位：秒。 |
| hive_meta_cache_ttl_s              | 86400                                           | Hive外部テーブルのキャッシュされたメタデータが期限切れになるまでの時間。単位：秒。 |
| hive_meta_store_timeout_s          | 10                                              | Hiveメタストアへの接続がタイムアウトするまでの時間。単位：秒。 |
| es_state_sync_interval_second      | 10                                              | FEがElasticsearchインデックスを取得し、StarRocks外部テーブルのメタデータを同期する時間間隔。単位：秒。 |
| enable_auth_check                  | TRUE                                            | 認証チェック機能を有効にするかどうかを指定します。有効な値：`TRUE`および`FALSE`。`TRUE`はこの機能を有効にすることを指定し、`FALSE`はこの機能を無効にすることを指定します。 |
| enable_metric_calculator           | TRUE                                            | 定期的にメトリックを収集する機能を有効にするかどうかを指定します。有効な値：`TRUE`および`FALSE`。`TRUE`はこの機能を有効にすることを指定し、`FALSE`はこの機能を無効にすることを指定します。 |

## BE設定項目

一部のBE設定項目は動的パラメーターであり、BEノードがオンラインのままのときにコマンドで設定できます。それ以外は静的パラメーターです。BEノードの静的パラメーターは、対応する設定ファイル**be.conf**で変更し、BEノードを再起動して変更を有効にすることしかできません。

### BE設定項目の表示

次のコマンドを使用してBE設定項目を表示できます。

```shell
curl http://<BE_IP>:<BE_HTTP_PORT>/varz
```

### BE動的パラメーターの設定

`curl`コマンドを使用してBEノードの動的パラメーターを設定できます。

```Shell
curl -XPOST http://be_host:http_port/api/update_config?<configuration_item>=<value>
```

BE動的パラメーターは以下の通りです。

| 設定項目 | デフォルト | 単位 | 説明 |
| ------------------ | ------- | ---- | ----------- |
| enable_stream_load_verbose_log | false | N/A | Stream LoadジョブのHTTPリクエストとレスポンスをログに記録するかどうかを指定します。このパラメーターは2.5.17、3.0.9、3.1.6、および3.2.1で導入されました。 |
| report_task_interval_seconds | 10 | 秒 | タスクの状態を報告する時間間隔。タスクはテーブルの作成、テーブルの削除、データのロード、またはテーブルスキーマの変更である可能性があります。 |
| report_disk_state_interval_seconds | 60 | 秒 | ストレージボリュームの状態を報告する時間間隔。これには、ボリューム内のデータのサイズが含まれます。 |
| report_tablet_interval_seconds | 60 | 秒 | すべてのtabletの最新バージョンを報告する時間間隔。 |
| report_workgroup_interval_seconds | 5 | 秒 | すべてのワークグループの最新バージョンを報告する時間間隔。 |
| max_download_speed_kbps | 50000 | KB/s | 各HTTPリクエストの最大ダウンロード速度。この値は、BEノード間のデータレプリカ同期のパフォーマンスに影響します。 |
| download_low_speed_limit_kbps | 50 | KB/s | 各HTTPリクエストのダウンロード速度の下限。HTTPリクエストがこの値より低い速度で指定された時間（download_low_speed_timeで指定）内で継続的に実行されると、リクエストは中止されます。 |
| download_low_speed_time | 300 | 秒 | ダウンロード速度が制限を下回る状態でHTTPリクエストが実行できる最大時間。HTTPリクエストがこの設定項目で指定された時間内にdownload_low_speed_limit_kbpsの値より低い速度で継続的に実行されると、リクエストは中止されます。 |
| status_report_interval | 5 | 秒 | クエリがそのプロファイルを報告する時間間隔。これは、FEによるクエリ統計の収集に使用できます。 |
| scanner_thread_pool_thread_num | 48 | N/A | ストレージエンジンが並行ストレージボリュームスキャンに使用するスレッド数。すべてのスレッドはスレッドプールで管理されます。 |
| thrift_client_retry_interval_ms | 100 | ms | Thriftクライアントが再試行する時間間隔。 |
| scanner_thread_pool_queue_size | 102400 | N/A | ストレージエンジンがサポートするスキャンタスクの数。 |
| scanner_row_num | 16384 | N/A | 各スキャンスレッドがスキャンで返す最大行数。 |
| max_scan_key_num | 1024 | N/A | 各クエリによってセグメント化されるスキャンキーの最大数。 |
| max_pushdown_conditions_per_column | 1024 | N/A | 各列でプッシュダウンを許可する条件の最大数。この制限を超えると、述語はストレージ層にプッシュダウンされません。 |
| exchg_node_buffer_size_bytes | 10485760 | バイト | 各クエリの交換ノードの受信側の最大バッファサイズ。この設定項目はソフトリミットです。データが過剰な速度で受信側に送信されると、バックプレッシャーがトリガーされます。 |
| memory_limitation_per_thread_for_schema_change | 2 | GB | 各スキーマ変更タスクに許可される最大メモリサイズ。 |
| update_cache_expire_sec | 360 | 秒 | Update Cacheの有効期限。 |
| file_descriptor_cache_clean_interval | 3600 | 秒 | 一定期間使用されていないファイルディスクリプタをクリーンアップする時間間隔。 |
| disk_stat_monitor_interval | 5 | 秒 | ディスクの健康状態を監視する時間間隔。 |
| unused_rowset_monitor_interval | 30 | 秒 | 期限切れのrowsetをクリーンアップする時間間隔。 |
| max_percentage_of_error_disk | 0 | % | 対応するBEノードが終了する前にストレージボリュームで許容されるエラーの最大割合。 |
| default_num_rows_per_column_file_block | 1024 | N/A | 各行ブロックに保存できる最大行数。 |
| pending_data_expire_time_sec | 1800 | 秒 | ストレージエンジン内の保留データの有効期限。 |
| inc_rowset_expired_sec | 1800 | 秒 | 受信データの有効期限。この設定項目はインクリメンタルクローンで使用されます。 |
| tablet_rowset_stale_sweep_time_sec | 1800 | 秒 | tablet内の古いrowsetをスイープする時間間隔。短い間隔は、ロード中のメタデータ使用量を減らすことができます。 |
| snapshot_expire_time_sec | 172800 | 秒 | スナップショットファイルの有効期限。 |
| trash_file_expire_time_sec | 86,400 | 秒 | ゴミファイルをクリーンアップする時間間隔。デフォルト値はv2.5.17、v3.0.9、およびv3.1.6以降、259,200から86,400に変更されました。 |
| base_compaction_check_interval_seconds | 60 | 秒 | ベースコンパクションのスレッドポーリングの時間間隔。 |
| min_base_compaction_num_singleton_deltas | 5 | N/A | ベースコンパクションをトリガーする最小セグメント数。 |
| max_base_compaction_num_singleton_deltas | 100 | N/A | 各ベースコンパクションでコンパクト化できる最大セグメント数。 |
| base_compaction_interval_seconds_since_last_operation | 86400 | 秒 | 最後のベースコンパクション以来の時間間隔。この設定項目はベースコンパクションをトリガーする条件の1つです。 |
| cumulative_compaction_check_interval_seconds | 1 | 秒 | 累積コンパクションのスレッドポーリングの時間間隔。 |
| update_compaction_check_interval_seconds | 60 | 秒 | 主キーテーブルのUpdate Compactionをチェックする時間間隔。 |
| min_compaction_failure_interval_sec | 120 | 秒 | 最後のコンパクション失敗以来、Tablet Compactionがスケジュールされるまでの最小時間間隔。 |
| max_compaction_concurrency | -1 | N/A | コンパクション（ベースコンパクションと累積コンパクションの両方）の最大同時実行数。値-1は、同時実行数に制限がないことを示します。 |
| periodic_counter_update_period_ms | 500 | ms | カウンター統計を収集する時間間隔。 |
| pindex_major_compaction_limit_per_disk | 1 | N/A | ディスク上のコンパクションの最大同時実行数。これは、コンパクションによるディスク間の不均一なI/Oの問題に対処します。この問題は、特定のディスクのI/Oが過度に高くなる原因となる可能性があります。導入されたバージョン: 3.0.9 |
| load_error_log_reserve_hours | 48 | 時間 | データロードログが保持される時間。 |
| streaming_load_max_mb | 10240 | MB | StarRocksにストリーミングできるファイルの最大サイズ。 |
| streaming_load_max_batch_size_mb | 100 | MB | StarRocksにストリーミングできるJSONファイルの最大サイズ。 |
| memory_maintenance_sleep_time_s | 10 | 秒 | ColumnPool GCがトリガーされる時間間隔。StarRocksは定期的にGCを実行し、解放されたメモリをオペレーティングシステムに返します。 |
| write_buffer_size | 104857600 | バイト | メモリ内のMemTableのバッファサイズ。この設定項目はフラッシュをトリガーするしきい値です。 |
| tablet_stat_cache_update_interval_second | 300 | 秒 | Tablet Stat Cacheを更新する時間間隔。 |
| result_buffer_cancelled_interval_time | 300 | 秒 | BufferControlBlockがデータを解放する前の待機時間。 |
| thrift_rpc_timeout_ms | 5000 | ms | Thrift RPCのタイムアウト。 |
| txn_commit_rpc_timeout_ms | 20000 | ms | トランザクションコミットRPCのタイムアウト。 |
| max_consumer_num_per_group | 3 | N/A | Routine Loadのコンシューマーグループ内の最大コンシューマー数。 |
| max_memory_sink_batch_count | 20 | N/A | スキャンキャッシュバッチの最大数。 |
| scan_context_gc_interval_min | 5 | 分 | スキャンコンテキストをクリーンアップする時間間隔。 |
| path_gc_check_step | 1000 | N/A | 各回で連続してスキャンできる最大ファイル数。 |
| path_gc_check_step_interval_ms | 10 | ms | ファイルスキャン間の時間間隔。 |
| path_scan_interval_second | 86400 | 秒 | GCが期限切れデータをクリーンアップする時間間隔。 |
| storage_flood_stage_usage_percent | 95 | % | すべてのBEディレクトリのストレージ使用率のハードリミット。BEストレージディレクトリのストレージ使用率（パーセンテージ）がこの値を超え、残りのストレージスペースが`storage_flood_stage_left_capacity_bytes`未満の場合、LoadおよびRestoreジョブは拒否されます。この項目をFE設定項目`storage_usage_hard_limit_percent`と一緒に設定する必要があります。 |
| storage_flood_stage_left_capacity_bytes | 107374182400 | バイト | すべてのBEディレクトリの残りストレージスペースのハードリミット。BEストレージディレクトリの残りストレージスペースがこの値未満で、ストレージ使用率（パーセンテージ）が`storage_flood_stage_usage_percent`を超える場合、LoadおよびRestoreジョブは拒否されます。この項目をFE設定項目`storage_usage_hard_limit_reserve_bytes`と一緒に設定する必要があります。 |
| tablet_meta_checkpoint_min_new_rowsets_num | 10 | N/A | 最後のTabletMeta Checkpoint以来作成された最小rowset数。 |
| tablet_meta_checkpoint_min_interval_secs | 600 | 秒 | TabletMeta Checkpointのスレッドポーリングの時間間隔。 |
| max_runnings_transactions_per_txn_map | 100 | N/A | 各パーティションで同時に実行できるトランザクションの最大数。 |
| tablet_max_pending_versions | 1000 | N/A | 主キーテーブルで許容される最大保留バージョン数。保留バージョンは、コミットされているがまだ適用されていないバージョンを指します。 |
| tablet_max_versions | 1000 | N/A | tabletで許可される最大バージョン数。この値を超えると、新しい書き込み要求は失敗します。 |
| max_hdfs_file_handle | 1000 | N/A | 開くことができるHDFSファイルディスクリプタの最大数。 |
| parquet_buffer_stream_reserve_size | 1048576 | バイト | データを読み取る際にParquetリーダーが各列に予約するバッファサイズ。 |
| be_exit_after_disk_write_hang_second | 60 | 秒 | ディスクがハングした後にBEが終了するまでの待機時間。 |
| min_cumulative_compaction_failure_interval_sec | 30 | 秒 | 累積コンパクションが失敗時に再試行する最小時間間隔。 |
| size_tiered_level_num | 7 | N/A | サイズ階層型コンパクション戦略のレベル数。各レベルには最大1つのrowsetが予約されます。したがって、安定した状態では、この設定項目で指定されたレベル数と同じ数のrowsetが存在します。 |
| size_tiered_level_multiple | 5 | N/A | サイズ階層型コンパクション戦略における2つの連続したレベル間のデータサイズの倍数。 |
| size_tiered_min_level_size | 131072 | バイト | サイズ階層型コンパクション戦略における最小レベルのデータサイズ。この値より小さいrowsetは即座にデータコンパクションをトリガーします。 |
| storage_page_cache_limit | 20% | N/A | PageCacheサイズ。STRING。サイズとして指定できます。たとえば、`20G`、`20480M`、`20971520K`、または`21474836480B`です。また、メモリサイズに対する比率（パーセンテージ）として指定することもできます。たとえば、`20%`です。`disable_storage_page_cache`が`false`に設定されている場合にのみ有効です。 |

### BE静的パラメーターの設定

BEの静的パラメーターは、対応する設定ファイル**be.conf**で変更し、BEを再起動して変更を有効にすることでのみ設定できます。

BE静的パラメーターは以下の通りです。

#### be_port

- **デフォルト**: 9060
- **単位**: N/A
- **説明**: BE Thriftサーバーポートで、FEからのリクエストを受信するために使用されます。

#### brpc_port

- **デフォルト**: 8060
- **単位**: N/A
- **説明**: BE bRPCポートで、bRPCのネットワーク統計を表示するために使用されます。

#### brpc_num_threads

- **デフォルト**: -1
- **単位**: N/A
- **説明**: bRPCのbthreadsの数。値-1は、CPUスレッドと同じ数を示します。

#### priority_networks

- **デフォルト**: 空文字列
- **単位**: N/A
- **説明**: BEノードをホストするマシンに複数のIPアドレスがある場合に、BEノードの優先IPアドレスを指定するために使用されるCIDR形式のIPアドレス。

#### heartbeat_service_port

- **デフォルト**: 9050
- **単位**: N/A
- **説明**: BEハートビートサービスポートで、FEからのハートビートを受信するために使用されます。

#### heartbeat_service_thread_count

- **デフォルト**: 1
- **単位**: N/A
- **説明**: BEハートビートサービスのスレッド数。

#### create_tablet_worker_count

- **デフォルト**: 3
- **単位**: N/A
- **説明**: tabletを作成するために使用されるスレッド数。

#### drop_tablet_worker_count

- **デフォルト**: 3
- **単位**: N/A
- **説明**: tabletを削除するために使用されるスレッド数。

#### push_worker_count_normal_priority

- **デフォルト**: 3
- **単位**: N/A
- **説明**: NORMAL優先度のロードタスクを処理するために使用されるスレッド数。

#### push_worker_count_high_priority

- **デフォルト**: 3
- **単位**: N/A
- **説明**: HIGH優先度のロードタスクを処理するために使用されるスレッド数。

#### transaction_publish_version_worker_count

- **デフォルト**: 0
- **単位**: N/A
- **説明**: バージョンを公開するために使用される最大スレッド数。この値が0以下に設定されている場合、システムはCPUコア数の半分を値として使用し、インポートの同時実行数が高いが固定スレッド数しか使用されない場合にスレッドリソースが不足しないようにします。v2.5以降、デフォルト値は8から0に変更されました。

#### clear_transaction_task_worker_count

- **デフォルト**: 1
- **単位**: N/A
- **説明**: トランザクションをクリアするために使用されるスレッド数。

#### alter_tablet_worker_count

- **デフォルト**: 3
- **単位**: N/A
- **説明**: スキーマ変更のために使用されるスレッド数。

#### clone_worker_count

- **デフォルト**: 3
- **単位**: N/A
- **説明**: クローンのために使用されるスレッド数。

#### storage_medium_migrate_count

- **デフォルト**: 1
- **単位**: N/A
- **説明**: 記憶媒体の移行（SATAからSSDへの移行）に使用されるスレッド数。

#### check_consistency_worker_count

- **デフォルト**: 1
- **単位**: N/A
- **説明**: tabletの一貫性をチェックするために使用されるスレッド数。

#### sys_log_dir

- **デフォルト**: `${STARROCKS_HOME}/log`
- **単位**: N/A
- **説明**: システムログ（INFO、WARNING、ERROR、FATALを含む）を保存するディレクトリ。

#### user_function_dir

- **デフォルト**: `${STARROCKS_HOME}/lib/udf`
- **単位**: N/A
- **説明**: ユーザー定義関数（UDF）を保存するために使用されるディレクトリ。

#### small_file_dir

- **デフォルト**: `${STARROCKS_HOME}/lib/small_file`
- **単位**: N/A
- **説明**: ファイルマネージャーによってダウンロードされたファイルを保存するために使用されるディレクトリ。

#### sys_log_level

- **デフォルト**: INFO
- **単位**: N/A
- **説明**: システムログエントリが分類される重大度レベル。有効な値：INFO、WARN、ERROR、FATAL。

#### sys_log_roll_mode

- **デフォルト**: SIZE-MB-1024
- **単位**: N/A
- **説明**: システムログがログロールに分割されるモード。有効な値には`TIME-DAY`、`TIME-HOUR`、`SIZE-MB-size`が含まれます。デフォルト値は、各ログロールが1GBであることを示します。

#### sys_log_roll_num

- **デフォルト**: 10
- **単位**: N/A
- **説明**: 保留するログロールの数。

#### sys_log_verbose_modules

- **デフォルト**: 空文字列
- **単位**: N/A
- **説明**: 印刷されるログのモジュール。たとえば、この設定項目をOLAPに設定すると、StarRocksはOLAPモジュールのログのみを印刷します。有効な値は、`starrocks`、`starrocks::vectorized`、`pipeline`を含むBEの名前空間です。

#### sys_log_verbose_level

- **デフォルト**: 10
- **単位**: N/A
- **説明**: 印刷されるログのレベル。この設定項目は、コード内でVLOGで開始されるログの出力を制御するために使用されます。

#### log_buffer_level

- **デフォルト**: 空文字列
- **単位**: N/A
- **説明**: ログをフラッシュするための戦略。デフォルト値は、ログがメモリにバッファされることを示します。有効な値は`-1`と`0`です。`-1`は、ログがメモリにバッファされないことを示します。

#### num_threads_per_core

- **デフォルト**: 3
- **単位**: N/A
- **説明**: 各CPUコアで開始されるスレッドの数。

#### compress_rowbatches

- **デフォルト**: TRUE
- **単位**: N/A
- **説明**: BEs間のRPCで行バッチを圧縮するかどうかを制御するブール値。TRUEは行バッチを圧縮することを示し、FALSEは圧縮しないことを示します。

#### serialize_batch

- **デフォルト**: FALSE
- **単位**: N/A
- **説明**: BEs間のRPCで行バッチをシリアライズするかどうかを制御するブール値。TRUEは行バッチをシリアライズすることを示し、FALSEはシリアライズしないことを示します。

#### storage_root_path

- **デフォルト**: `${STARROCKS_HOME}/storage`
- **単位**: N/A
- **説明**: ストレージボリュームのディレクトリと媒体。
  - 複数のボリュームはセミコロン（`;`）で区切られます。
  - 記憶媒体がSSDの場合、ディレクトリの末尾に`medium:ssd`を追加します。
  - 記憶媒体がHDDの場合、ディレクトリの末尾に`medium:hdd`を追加します。

#### max_tablet_num_per_shard

- **デフォルト**: 1024
- **単位**: N/A
- **説明**: 各シャード内の最大tablet数。この設定項目は、各ストレージディレクトリの下にあるtablet子ディレクトリの数を制限するために使用されます。

#### max_garbage_sweep_interval

- **デフォルト**: 3600
- **単位**: 秒
- **説明**: ストレージボリュームでのガーベジコレクションの最大時間間隔。

#### min_garbage_sweep_interval

- **デフォルト**: 180
- **単位**: 秒
- **説明**: ストレージボリュームでのガーベジコレクションの最小時間間隔。

#### row_nums_check

- **デフォルト**: True
- **単位**: N/A
- **説明**: コンパクションの前後で行数をチェックするかどうかを制御するブール値。値がtrueの場合、行数チェックを有効にします。値がfalseの場合、行数チェックを無効にします。

#### file_descriptor_cache_capacity

- **デフォルト**: 16384
- **単位**: N/A
- **説明**: キャッシュできるファイルディスクリプタの数。

#### min_file_descriptor_number

- **デフォルト**: 60000
- **単位**: N/A
- **説明**: BEプロセス内のファイルディスクリプタの最小数。

#### index_stream_cache_capacity

- **デフォルト**: 10737418240
- **単位**: バイト
- **説明**: BloomFilter、Min、およびMaxの統計情報のキャッシュ容量。

#### disable_storage_page_cache

- **デフォルト**: FALSE
- **単位**: N/A
- **説明**: PageCacheを無効にするかどうかを制御するブール値。
  - PageCacheが有効な場合、StarRocksは最近スキャンされたデータをキャッシュします。
  - PageCacheは、同様のクエリが頻繁に繰り返される場合にクエリパフォーマンスを大幅に向上させることができます。
  - TRUEはPageCacheを無効にすることを示します。
  - この項目のデフォルト値はStarRocks v2.4以降、TRUEからFALSEに変更されました。

#### base_compaction_num_threads_per_disk

- **デフォルト**: 1
- **単位**: N/A
- **説明**: 各ストレージボリュームでのベースコンパクションに使用されるスレッド数。

#### base_cumulative_delta_ratio

- **デフォルト**: 0.3
- **単位**: N/A
- **説明**: 累積ファイルサイズとベースファイルサイズの比率。この比率がこの値に達することは、ベースコンパクションをトリガーする条件の1つです。

#### compaction_trace_threshold

- **デフォルト**: 60
- **単位**: 秒
- **説明**: 各コンパクションの時間しきい値。コンパクションが時間しきい値を超えて時間がかかる場合、StarRocksは対応するトレースを印刷します。

#### webserver_port

- **デフォルト**: 8040
- **単位**: N/A
- **説明**: HTTPサーバーポート。

#### webserver_num_workers

- **デフォルト**: 48
- **単位**: N/A
- **説明**: HTTPサーバーが使用するスレッド数。

#### load_data_reserve_hours

- **デフォルト**: 4
- **単位**: 時間
- **説明**: 小規模ロードによって生成されたファイルの保持時間。

#### number_tablet_writer_threads

- **デフォルト**: 16
- **単位**: N/A
- **説明**: Stream Loadに使用されるスレッド数。

#### streaming_load_rpc_max_alive_time_sec

- **デフォルト**: 1200
- **単位**: 秒
- **説明**: Stream LoadのRPCタイムアウト。

#### fragment_pool_thread_num_min

- **デフォルト**: 64
- **単位**: N/A
- **説明**: クエリに使用される最小スレッド数。

#### fragment_pool_thread_num_max

- **デフォルト**: 4096
- **単位**: N/A
- **説明**: クエリに使用される最大スレッド数。

#### fragment_pool_queue_size

- **デフォルト**: 2048
- **単位**: N/A
- **説明**: 各BEノードで処理できるクエリの上限数。

#### enable_partitioned_aggregation

- **デフォルト**: TRUE
- **単位**: N/A
- **説明**: パーティション集計を有効にするかどうかを制御するブール値。値がtrueの場合、パーティション集計を有効にします。値がfalseの場合、パーティション集計を無効にします。

#### enable_token_check

- **デフォルト**: TRUE
- **単位**: N/A
- **説明**: トークンチェックを有効にするかどうかを制御するブール値。TRUEはトークンチェックを有効にすることを示し、FALSEは無効にすることを示します。

#### enable_prefetch

- **デフォルト**: TRUE
- **単位**: N/A
- **説明**: クエリのプリフェッチを有効にするかどうかを制御するブール値。TRUEはプリフェッチを有効にすることを示し、FALSEは無効にすることを示します。

#### load_process_max_memory_limit_bytes

- **デフォルト**: 107374182400
- **単位**: バイト
- **説明**: BEノード上のすべてのロードプロセスが占有できるメモリリソースの最大サイズ制限。

#### load_process_max_memory_limit_percent

- **デフォルト**: 30
- **単位**: %
- **説明**: BEノード上のすべてのロードプロセスが占有できるメモリリソースの最大パーセンテージ制限。

#### sync_tablet_meta

- **デフォルト**: FALSE
- **単位**: N/A
- **説明**: tabletメタデータの同期を有効にするかどうかを制御するブール値。TRUEは同期を有効にすることを示し、FALSEは無効にすることを示します。

#### routine_load_thread_pool_size

- **デフォルト**: 10
- **単位**: N/A
- **説明**: 各BEでのRoutine Loadのスレッドプールサイズ。

#### internal_service_async_thread_num

- **デフォルト:** 10 (スレッド数)
- **説明:** Kafkaとの対話のために各BEで許可されるスレッドプールサイズ。現在、Routine Loadリクエストを処理するFEは、Kafkaとの対話にBEに依存しており、StarRocksの各BEはKafkaとの対話のための独自のスレッドプールを持っています。大量のRoutine LoadタスクがBEに分散されると、Kafkaとの対話のためのBEのスレッドプールが忙しすぎて、すべてのタスクをタイムリーに処理できない場合があります。この状況では、このパラメーターの値を調整してニーズに合わせることができます。

#### brpc_max_body_size

- **デフォルト**: 2147483648
- **単位**: バイト
- **説明**: bRPCの最大ボディサイズ。

#### tablet_map_shard_size

- **デフォルト**: 32
- **単位**: N/A
- **説明**: tabletマップシャードサイズ。値は2のべき乗でなければなりません。

#### enable_bitmap_union_disk_format_with_set

- **デフォルト**: FALSE
- **単位**: N/A
- **説明**: BITMAPタイプの新しいストレージ形式を有効にするかどうかを制御するブール値。TRUEは新しいストレージ形式を有効にすることを示し、FALSEは無効にすることを示します。

#### mem_limit

- **デフォルト**: 90%
- **単位**: N/A
- **説明**: BEプロセスメモリの上限。パーセンテージ（"80%"）または物理制限（"100G"）として設定できます。デフォルトのハードリミットはサーバーのメモリサイズの90％であり、ソフトリミットは80％です。StarRocksを他のメモリ集約型サービスと同じサーバーにデプロイしたい場合、このパラメーターを設定する必要があります。

#### flush_thread_num_per_store

- **デフォルト**: 2
- **単位**: N/A
- **説明**: 各ストアでMemTableをフラッシュするために使用されるスレッド数。

#### block_cache_enable

- **デフォルト**: false
- **単位**: N/A
- **説明**: Data Cacheを有効にするかどうか。TRUEはData Cacheが有効であることを示し、FALSEは無効であることを示します。

#### block_cache_disk_path

- **デフォルト**: N/A
- **単位**: N/A
- **説明**: ディスクのパス。このパラメーターに設定するパスの数は、BEマシン上のディスクの数と同じであることをお勧めします。複数のパスはセミコロン（;）で区切る必要があります。このパラメーターを追加すると、StarRocksは自動的にcachelib_dataという名前のファイルを作成してブロックをキャッシュします。

#### block_cache_meta_path

- **デフォルト**: N/A
- **単位**: N/A
- **説明**: ブロックメタデータの保存パス。保存パスをカスタマイズできます。メタデータを`$STARROCKS_HOME`パスの下に保存することをお勧めします。

#### block_cache_mem_size

- **デフォルト**: 2147483648
- **単位**: バイト
- **説明**: メモリにキャッシュできるデータの最大量。単位：バイト。デフォルト値は2147483648で、2GBです。このパラメーターの値を少なくとも20GBに設定することをお勧めします。Data Cacheが有効になった後、StarRocksがディスクから大量のデータを読み取る場合、値を増やすことを検討してください。

#### block_cache_disk_size

- **デフォルト**: 0
- **単位**: バイト
- **説明**: 単一のディスクにキャッシュできるデータの最大量。たとえば、block_cache_disk_pathパラメーターに2つのディスクパスを設定し、block_cache_disk_sizeパラメーターの値を21474836480（20GB）に設定した場合、これらの2つのディスクに最大40GBのデータをキャッシュできます。デフォルト値は0で、メモリのみがデータをキャッシュするために使用されることを示します。単位：バイト。

#### jdbc_connection_pool_size

- **デフォルト**: 8
- **単位**: N/A
- **説明**: JDBC接続プールサイズ。各BEノードで、同じjdbc_urlを持つ外部テーブルにアクセスするクエリは同じ接続プールを共有します。

#### jdbc_minimum_idle_connections

- **デフォルト**: 1
- **単位**: N/A
- **説明**: JDBC接続プール内のアイドル接続の最小数。

#### jdbc_connection_idle_timeout_ms

- **デフォルト**: 600000
- **単位**: N/A
- **説明**: JDBC接続プール内のアイドル接続が期限切れになるまでの時間。JDBC接続プール内の接続アイドル時間がこの値を超えると、接続プールは設定項目jdbc_minimum_idle_connectionsで指定された数を超えるアイドル接続を閉じます。

#### query_cache_capacity

- **デフォルト**: 536870912
- **単位**: N/A
- **説明**: BEのクエリキャッシュのサイズ。単位：バイト。デフォルトサイズは512MBです。サイズは4MB未満にすることはできません。BEのメモリ容量が期待されるクエリキャッシュサイズを提供するのに不十分な場合、BEのメモリ容量を増やすことができます。

#### enable_event_based_compaction_framework

- **デフォルト**: TRUE
- **単位**: N/A
- **説明**: イベントベースのコンパクションフレームワークを有効にするかどうか。TRUEはイベントベースのコンパクションフレームワークが有効であることを示し、FALSEは無効であることを示します。イベントベースのコンパクションフレームワークを有効にすると、多くのtabletがある場合や単一のtabletに大量のデータがある場合のコンパクションのオーバーヘッドを大幅に削減できます。

#### enable_size_tiered_compaction_strategy

- **デフォルト**: TRUE
- **単位**: N/A
- **説明**: サイズ階層型コンパクション戦略を有効にするかどうか。TRUEはサイズ階層型コンパクション戦略が有効であることを示し、FALSEは無効であることを示します。

#### update_compaction_size_threshold

- **デフォルト**: 67108864
- **単位**: バイト
- **説明**: 主キーテーブルのコンパクションスコアは、他のテーブルタイプとは異なり、ファイルサイズに基づいて計算されます。このパラメーターは、主キーテーブルのコンパクションスコアを他のテーブルタイプのコンパクションスコアに似せるために使用でき、ユーザーが理解しやすくなります。v2.5.20以降、このパラメーターのデフォルト値は`268435456`（256MB）から`67108864`（64MB）に変更され、コンパクションが加速されます。