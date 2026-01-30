---
displayed_sidebar: docs
---

# HTTP Interface

To facilitate the maintenance of StarRocks clusters, StarRocks provides various types of operation and query interfaces. This topic introduces these HTTP interfaces and their usage.

## FE

| Request Method      | Request Path                                                    | Description                                                                                                          |
|---------------------|-----------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| PUT                 | `/api/{db}/{table}/_stream_load`                                | Stream Load operation, see [Stream Load](../loading/StreamLoad.md) for details.                                      |
| POST/PUT            | `/api/transaction/{txn_op}`                                     | Stream Load transaction interface, see [Stream Load Transaction Interface](../loading/Stream_Load_transaction_interface.md) for details. |
| GET                 | `/api/{db}/_load_info`                                          | |
| GET                 | `/api/_set_config?config_key1=config_value1`                    | Update FE configuration.                                                                                             |
| GET                 | `/api/_get_ddl?db={}&tbl={}`                                    | View table DDL statement.                                                                                            |
| GET                 | `/api/_migration?db={}&tbl={}`                                  | View table tablet information.                                                                                       |
| GET                 | `/api/_check_storagetype`                                       | |
| POST                | `/api/{db}/{table}/_cancel?label={}`                            | |
| GET                 | `/api/{db}/get_load_state`                                      | |
| GET                 | `/api/health`                                                   | |
| GET                 | `/metrics?type={core/json}`                                     | View metrics of the current FE.                                                                                      |
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
| GET                 | `/api/profile?query_id={}`                                      | Get profile information for the specified query ID.                                                                  |
| GET                 | `/api/query_detail`                                             | Get query details. See [Query detail API](./http_interface/query_detail.md).                                         |
| GET                 | `/api/connection`                                               | |
| GET                 | `/api/show_data?db={}`                                          | Query the size of the specified database.                                                                            |
| POST                | `/api/query_dump`                                               | Get query dump information, see [Query Dump](../faq/Dump_query.md) for details.                                      |
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
| GET                 | `/api/{db}/{table}/_schema`                                     | View table schema.                                                                                                   |
| GET/POST            | `/api/{db}/{table}/_query_plan`                                 | |

## BE

| HTTP Request Method | HTTP Request Path                                               | Description                                                                                                          |
|---------------------|-----------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| PUT                 | `/api/{db}/{table}/_stream_load`                                | Stream Load operation, see [Stream Load](../loading/StreamLoad.md) for details.                                      |
| POST/PUT            | `/api/transaction/{txn_op}`                                     | Stream Load transaction interface, see [Stream Load Transaction Interface](../loading/Stream_Load_transaction_interface.md) for details. |
| PUT                 | `/api/transaction/load`                                         | |
| HEAD/GET            | `/api/_download_load`                                           | |
| HEAD/GET            | `/api/_tablet/_download`                                        | |
| HEAD/GET            | `/api/_load_error_log`                                          | |
| GET                 | `/api/health`                                                   | |
| GET                 | `/api/_stop_be`                                                 | |
| GET                 | `/pprof/heap`                                                   | |
| GET                 | `/pprof/growth`                                                 | |
| GET                 | `/pprof/profile`                                                | |
| GET                 | `/pprof/pmuprofile`                                             | |
| GET                 | `/pprof/contention`                                             | |
| GET                 | `/pprof/cmdline`                                                | |
| HEAD/GET/POST       | `/pprof/symbol`                                                 | |
| GET                 | `/metrics`                                                      | View metrics of the current BE.                                                                                      |
| HEAD                | `/api/meta/header/{tablet_id}`                                  | |
| GET                 | `/api/checksum`                                                 | |
| GET                 | `/api/reload_tablet`                                            | |
| POST                | `/api/restore_tablet`                                           | |
| GET                 | `/api/snapshot`                                                 | |
| GET                 | `/api/compaction/show?tablet_id={}`                             | View compaction information for the specified tablet.                                                                |
| POST                | `/api/compact?tablet_id={}&compaction_type={base/cumulative}`   | Manually perform compaction on the specified tablet.                                                                 |
| GET                 | `/api/compaction/show_repair`                                   | |
| PUT                 | `/api/compaction/submit_repair`                                 | |
| POST                | `/api/update_config`                                            | Update BE configuration, see [Update BE Configuration](../administration/management/BE_configuration.md) for details. |
| GET/PUT             | `/api/runtime_filter_cache/{action}`                            | |
| POST                | `/api/compact_rocksdb_meta`                                     | |
| GET/PUT             | `/api/query_cache/{action}`                                     | |
| GET                 | `/api/pipeline_blocking_drivers/{action}`                       | |
| GET                 | `/greplog`                                                      | |
| GET                 | `/varz`                                                         | View current BE configuration.                                                                                       |

## CN

| Request Method      | Request Path                                                    | Description                                                                                                          |
|---------------------|-----------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| GET                 | `/api/health`                                                   | |
| GET                 | `/pprof/heap`                                                   | |
| GET                 | `/pprof/growth`                                                 | |
| GET                 | `/pprof/profile`                                                | |
| GET                 | `/pprof/pmuprofile`                                             | |
| GET                 | `/pprof/contention`                                             | |
| GET                 | `/pprof/cmdline`                                                | |
| HEAD/GET/POST       | `/pprof/symbol`                                                 | |
| GET                 | `/metrics`                                                      | View metrics of the current CN.                                                                                      |
