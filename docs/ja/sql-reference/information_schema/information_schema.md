---
displayed_sidebar: docs
description: "StarRocks Information Schemaは各インスタンス内のメタデータ情報を格納する読み取り専用のシステム定義ビューです。"
---

# Information Schema

StarRocks Information Schema は、各 StarRocks インスタンス内のデータベースです。Information Schema には、StarRocks インスタンスが管理するすべてのオブジェクトの広範なメタデータ情報を格納する、読み取り専用のシステム定義ビューがいくつか含まれています。StarRocks Information Schema は、SQL-92 ANSI Information Schema に基づいていますが、StarRocks に特有のビューと関数が追加されています。

バージョン 3.2.0 から、StarRocks Information Schema は external catalogs のメタデータ管理をサポートしています。

## Information Schema を通じたメタデータの表示

StarRocks インスタンス内のメタデータ情報は、Information Schema 内のビューの内容をクエリすることで表示できます。

次の例では、StarRocks 内の `table1` という名前のテーブルに関するメタデータ情報を、ビュー `tables` をクエリすることで確認します。

```Plain
MySQL > SELECT * FROM information_schema.tables WHERE TABLE_NAME like 'table1'\G
*************************** 1. row ***************************
  TABLE_CATALOG: def
   TABLE_SCHEMA: test_db
     TABLE_NAME: table1
     TABLE_TYPE: BASE TABLE
         ENGINE: StarRocks
        VERSION: NULL
     ROW_FORMAT: 
     TABLE_ROWS: 4
 AVG_ROW_LENGTH: 1657
    DATA_LENGTH: 6630
MAX_DATA_LENGTH: NULL
   INDEX_LENGTH: NULL
      DATA_FREE: NULL
 AUTO_INCREMENT: NULL
    CREATE_TIME: 2023-06-13 11:37:00
    UPDATE_TIME: 2023-06-13 11:38:06
     CHECK_TIME: NULL
TABLE_COLLATION: utf8_general_ci
       CHECKSUM: NULL
 CREATE_OPTIONS: 
  TABLE_COMMENT: 
1 row in set (0.01 sec)
```

## Information Schema のビュー

StarRocks Information Schema には、以下のメタデータビューが含まれています。

| **ビュー** | **説明** |
| --- | --- |
| [analyze_status](./analyze_status.md) | 分析ジョブのステータス。 |
| [applicable_roles](./applicable_roles.md) | 現在のユーザーに適用可能なロール。 |
| [be_bvars](./be_bvars.md) | 一部の StarRocks コンポーネントの RPC レイテンシーや QPS など、bRPC の統計情報。 |
| [be_cloud_native_compactions](./be_cloud_native_compactions.md) | 共有データクラスタの CN で実行されている Compaction トランザクション。 |
| [be_compactions](./be_compactions.md) | Compaction タスクの統計情報。 |
| [be_configs](./be_configs.md) | 各 BE ノードの構成パラメータ。 |
| [be_logs](./be_logs.md) | 各 BE ノードのログ。 |
| [be_metrics](./be_metrics.md) | 各 BE ノードのメトリック。 |
| [be_tablets](./be_tablets.md) | 各 BE ノード上のタブレット。 |
| [be_threads](./be_threads.md) | 各 BE ノードで実行されているスレッド。 |
| [be_txns](./be_txns.md) | 各 BE ノード上のトランザクション。 |
| [character_sets](./character_sets.md) | 利用可能な文字セット。 |
| [collations](./collations.md) | 利用可能な照合順序。 |
| [column_stats_usage](./column_stats_usage.md) | 列統計情報の使用状況。 |
| [columns](./columns.md) | すべてのテーブル列およびビュー列。 |
| [fe_metrics](./fe_metrics.md) | 各 FE ノードのメトリック。 |
| [fe_tablet_schedules](./fe_tablet_schedules.md) | FE ノード上のタブレットスケジューリングタスク。 |
| [fe_threads](./fe_threads.md) | 各 FE ノードで実行されているスレッド。 |
| [global_variables](./global_variables.md) | グローバル変数。 |
| [load_tracking_logs](./load_tracking_logs.md) | ロードジョブのエラーログ。 |
| [loads](./loads.md) | ロードジョブの結果。 |
| [materialized_view_refresh_jobs](./materialized_view_refresh_jobs.md) | マテリアライズドビューのリフレッシュに関するジョブレベルの情報。 |
| [materialized_views](./materialized_views.md) | すべてのマテリアライズドビュー。 |
| [partitions_meta](./partitions_meta.md) | テーブルのパーティション。 |
| [pipe_files](./pipe_files.md) | 指定されたパイプを介してロードされるデータファイルのステータス。 |
| [pipes](./pipes.md) | 現在のデータベースまたは指定されたデータベースに保存されているすべてのパイプ。 |
| [recyclebin_catalogs](./recyclebin_catalogs.md) | FE リサイクルビンに保持されている削除済みデータベース、テーブル、パーティション。 |
| [routine_load_jobs](./routine_load_jobs.md) | Routine Load ジョブ。 |
| [schemata](./schemata.md) | データベース。 |
| [session_variables](./session_variables.md) | セッション変数。 |
| [table_bookmarks](./table_bookmarks.md) | OlapTable のアクティブなブックマーク。`table_bookmark_summary`、`table_bookmark_partitions`、`table_bookmark_references` の 3 つのビューで構成されます。 |
| [tables](./tables.md) | テーブル。 |
| [tables_config](./tables_config.md) | テーブルの設定。 |
| [task_runs](./task_runs.md) | 非同期タスクの実行。 |
| [tasks](./tasks.md) | 非同期タスク。 |
| [views](./views.md) | すべてのユーザー定義ビュー。 |
| [warehouse_metrics](./warehouse_metrics.md) | 各ウェアハウスのメトリック。 |
| [warehouse_queries](./warehouse_queries.md) | 各ウェアハウスで実行されているクエリ。 |

### 互換性のために定義されているビュー

以下のビューは定義されていますが、StarRocks では実装されていません。各ページにその旨の注記があります。

| **ビュー** | **説明** |
| --- | --- |
| [column_privileges](./column_privileges.md) | 列に付与された権限。 |
| [engines](./engines.md) | ストレージエンジン。 |
| [events](./events.md) | Event Manager のイベント。 |
| [key_column_usage](./key_column_usage.md) | ユニーク制約、主キー制約、外部キー制約によって制限される列。 |
| [partitions](./partitions.md) | テーブルのパーティション。代わりに `partitions_meta` を使用してください。 |
| [referential_constraints](./referential_constraints.md) | 参照 (外部キー) 制約。 |
| [routines](./routines.md) | ストアドルーチン (ストアドプロシージャおよびストアドファンクション)。 |
| [schema_privileges](./schema_privileges.md) | データベース権限。 |
| [statistics](./statistics.md) | テーブルのインデックス。 |
| [table_constraints](./table_constraints.md) | 制約を持つテーブル。 |
| [table_privileges](./table_privileges.md) | テーブル権限。 |
| [triggers](./triggers.md) | トリガー。 |
| [user_privileges](./user_privileges.md) | ユーザー権限。 |
