---
displayed_sidebar: docs
---

# HTTP インターフェース

StarRocks クラスターのメンテナンスを容易にするために、StarRocks はさまざまな種類の操作およびクエリインターフェースを提供しています。このトピックでは、これらの HTTP インターフェースとその使用方法を紹介します。

## FE

| リクエストメソッド | リクエストパス                                                    | 説明                                                                                                                 |
|---------------------|-----------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| PUT                 | `/api/{db}/{table}/_stream_load`                                | Stream Load 操作。詳細は [Stream Load](../loading/StreamLoad.md) を参照してください。                                |
| POST/PUT            | `/api/transaction/{txn_op}`                                     | Stream Load トランザクションインターフェース。詳細は [Stream Load Transaction Interface](../loading/Stream_Load_transaction_interface.md) を参照してください。 |
| GET                 | `/api/{db}/_load_info`                                          | |
| GET                 | `/api/_set_config?config_key1=config_value1`                    | FE 設定を更新します。                                                                                                |
| GET                 | `/api/_get_ddl?db={}&tbl={}`                                    | テーブル DDL ステートメントを表示します。                                                                            |
| GET                 | `/api/_migration?db={}&tbl={}`                                  | テーブルのタブレット情報を表示します。                                                                               |
| GET                 | `/api/_check_storagetype`                                       | |
| POST                | `/api/{db}/{label}/_cancel`                                     | |
| GET                 | `/api/{db}/get_load_state`                                      | |
| GET                 | `/api/health`                                                   | |
| GET                 | `/metrics?type={core/json}`                                     | 現在の FE のメトリクスを表示します。                                                                                 |
| GET                 | `/api/show_meta_info`                                           | |
| GET                 | `/api/show_proc`                                                | |
| GET                 | `/api/show_runtime_info`                                        | |
| HEAD/GET            | `/api/get_log_file`                                             | |
| GET                 | `/api/get_small_file`                                           | |
| GET                 | `/api/rowcount`                                                 | |
| GET                 | `/api/check_decommission`                                       | |
| GET                 | `/api/_meta_replay_state`                                       | |
| POST                | `/api/colocate/bucketseq`                                       | |
| GET                 | `/api/colocate`                                                 | |
| POST                | `/api/colocate/group_stable`                                    | |
| POST                | `/api/colocate/group_unstable`                                  | |
| POST                | `/api/colocate/update_group`                                    | |
| POST                | `/api/global_dict/table/enable`                                 | |
| GET                 | `/api/profile?query_id={}`                                      | 指定されたクエリ ID のプロファイル情報を取得します。                                                                |
| GET                 | `/api/query_detail`                                             | クエリ詳細を取得します。詳細は [Query detail API](./http_interface/query_detail.md) を参照してください。              |
| GET                 | `/api/connection`                                               | |
| GET                 | `/api/show_data?db={}`                                          | 指定されたデータベースのサイズをクエリします。                                                                       |
| POST                | `/api/query_dump`                                               | クエリダンプ情報を取得します。詳細は [Query Dump](../faq/Dump_query.md) を参照してください。                         |
| GET                 | `/api/stop`                                                     | |
| GET                 | `/image`                                                        | |
| GET                 | `/info`                                                         | |
| GET                 | `/version`                                                      | |
| GET                 | `/put`                                                          | |
| GET                 | `/journal_id`                                                   | |
| GET                 | `/check`                                                        | |
| GET                 | `/dump`                                                         | |
| GET                 | `/role`                                                         | |
| GET                 | `/api/{db}/{table}/_count`                                      | |
| GET                 | `/api/{db}/{table}/_schema`                                     | テーブルスキーマを表示します。                                                                                       |
| GET/POST            | `/api/{db}/{table}/_query_plan`                                 | |

## BE

| HTTP リクエストメソッド | HTTP リクエストパス                                               | 説明                                                                                                                 |
|-------------------------|-----------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| PUT                     | `/api/{db}/{table}/_stream_load`                                | Stream Load 操作。詳細は [Stream Load](../loading/StreamLoad.md) を参照してください。                                |
| POST/PUT                | `/api/transaction/{txn_op}`                                     | Stream Load トランザクションインターフェース。詳細は [Stream Load Transaction Interface](../loading/Stream_Load_transaction_interface.md) を参照してください。 |
| PUT                     | `/api/transaction/load`                                         | |
| HEAD/GET                | `/api/_download_load`                                           | |
| HEAD/GET                | `/api/_tablet/_download`                                        | |
| HEAD/GET                | `/api/_load_error_log`                                          | |
| GET                     | `/api/health`                                                   | |
| GET                     | `/api/_stop_be`                                                 | |
| GET                     | `/pprof/heap`                                                   | |
| GET                     | `/pprof/growth`                                                 | |
| GET                     | `/pprof/profile`                                                | |
| GET                     | `/pprof/pmuprofile`                                             | |
| GET                     | `/pprof/contention`                                             | |
| GET                     | `/pprof/cmdline`                                                | |
| HEAD/GET/POST           | `/pprof/symbol`                                                 | |
| GET                     | `/metrics`                                                      | 現在の BE のメトリクスを表示します。                                                                                 |
| HEAD                    | `/api/meta/header/{tablet_id}`                                  | |
| GET                     | `/api/checksum`                                                 | |
| GET                     | `/api/reload_tablet`                                            | |
| POST                    | `/api/restore_tablet`                                           | |
| GET                     | `/api/snapshot`                                                 | |
| GET                     | `/api/compaction/show?tablet_id={}`                             | 指定されたタブレットの Compaction 情報を表示します。                                                                 |
| POST                    | `/api/compact?tablet_id={}&compaction_type={base/cumulative}`   | 指定されたタブレットで手動で Compaction を実行します。                                                              |
| GET                     | `/api/compaction/show_repair`                                   | |
| PUT                     | `/api/compaction/submit_repair`                                 | |
| POST                    | `/api/update_config`                                            | BE 設定を更新します。詳細は [Update BE Configuration](../administration/management/BE_configuration.md) を参照してください。 |
| GET/PUT                 | `/api/runtime_filter_cache/{action}`                            | |
| POST                    | `/api/compact_rocksdb_meta`                                     | |
| GET/PUT                 | `/api/query_cache/{action}`                                     | |
| GET                     | `/api/pipeline_blocking_drivers/{action}`                       | |
| GET                     | `/greplog`                                                      | |
| GET                     | `/varz`                                                         | 現在の BE 設定を表示します。                                                                                         |

## CN

| リクエストメソッド | リクエストパス                                                    | 説明                                                                                                                 |
|---------------------|-----------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| GET                 | `/api/health`                                                   | |
| GET                 | `/pprof/heap`                                                   | |
| GET                 | `/pprof/growth`                                                 | |
| GET                 | `/pprof/profile`                                                | |
| GET                 | `/pprof/pmuprofile`                                             | |
| GET                 | `/pprof/contention`                                             | |
| GET                 | `/pprof/cmdline`                                                | |
| HEAD/GET/POST       | `/pprof/symbol`                                                 | |
| GET                 | `/metrics`                                                      | 現在の CN のメトリクスを表示します。                                                                                 |
