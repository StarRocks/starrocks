---
displayed_sidebar: "Chinese"
---

# HTTP 接口

为了方便维护 StarRocks 集群，StarRocks 提供了不同类型的操作和查询接口。本文介绍 HTTP 接口和使用方法。

## FE

| HTTP 请求方法       | HTTP 请求路径                                                   | 描述                                                                                                                |
|------------------| --------------------------------------------------------------  |-------------------------------------------------------------------------------------------------------------------- |
| PUT              | `/api/{db}/{table}/_stream_load`                                  | Stream Load 操作，详见 [Stream Load](../loading/StreamLoad.md)。                               |
| POST/PUT         | `/api/transaction/{txn_op}`                                       | Stream Load 事务接口，详见 [Stream Load 事务接口](../loading/Stream_Load_transaction_interface.md)     |
| GET              | `/api/{db}/_load_info`
| GET              | `/api/_set_config?config_key1=config_value1`                      | 更新 FE 配置。                                                                                                            |
| GET              | `/api/_get_ddl?db={}&tbl={}`                                      | 查看表 DDL 语句。
| GET              | `/api/_migration?db={}&tbl={}`                                    | 查看表 tablet 信息。                                                                                                    |
| GET              | `/api/_check_storagetype`
| POST             | `/api/{db}/{label}/_cancel`
| GET              | `/api/{db}/get_load_state`
| GET              | `/api/health`
| GET              | `/metrics?type={core/json}`                                       | 查看当前 FE 的 metrics。                                                                                                |
| GET              | `/api/show_meta_info`
| GET              | `/api/show_proc`
| GET              | `/api/show_runtime_info`
| HEAD/GET         | `/api/get_log_file`
| GET              | `/api/get_small_file`
| GET              | `/api/rowcount`
| GET              | `/api/check_decommission`
| GET              | `/api/_meta_replay_state`
| POST             | `/api/colocate/bucketseq`
| GET              | `/api/colocate`
| POST             | `/api/colocate/group_stable`
| POST             | `/api/colocate/group_unstable`
| POST             | `/api/colocate/update_group`
| POST             | `/api/global_dict/table/enable`
| GET              | `/api/profile?query_id={}`                                        | 获取指定 query ID 的 profile 信息。                                                                                       |
| GET              | `/api/query_detail`
| GET              | `/api/connection`
| GET              | `/api/show_data?db={}`                                            | 查询指定数据库的大小。                                                                                                    |
| POST             | `/api/query_dump`                                                 | 获取 query dump 信息，详见 [Query dump](../faq/Dump_query.md) 。                       |
| GET              | `/api/stop`
| GET              | `/image`
| GET              | `/info`
| GET              | `/version`
| GET              | `/put`
| GET              | `/journal_id`
| GET              | `/check`
| GET              | `/dump`
| GET              | `/role`
| GET              | `/api/{db}/{table}/_count`
| GET              | `/api/{db}/{table}/_schema`                                      | 查看表结构。                                                                                                          |
| GET/POST         | `/api/{db}/{table}/_query_plan`

## BE

| HTTP 请求方法       | HTTP 请求路径                                                     | 描述                                                                                                                |
|------------------| --------------------------------------------------------------  |-------------------------------------------------------------------------------------------------------------------- |
| PUT              | `/api/{db}/{table}/_stream_load`                                  | Stream Load 操作，详见 [Stream Load](../loading/StreamLoad.md)                          |
| POST/PUT         | `/api/transaction/{txn_op}`                                       | Stream Load 事务接口，详见 [Stream Load 事务接口](../loading/Stream_Load_transaction_interface.md)   |
| PUT              | `/api/transaction/load`                                           |
| HEAD/GET         | `/api/_download_load`                                             |
| HEAD/GET         | `/api/_tablet/_download`                                          |
| HEAD/GET         | `/api/_load_error_log`                                            |
| GET              | `/api/health`                                                     |
| GET              | `/api/_stop_be`                                                   |
| GET              | `/pprof/heap`                                                     |
| GET              | `/pprof/growth`                                                   |
| GET              | `/pprof/profile`                                                  |
| GET              | `/pprof/pmuprofile`                                               |
| GET              | `/pprof/contention`                                               |
| GET              | `/pprof/cmdline`                                                  |
| HEAD/GET/POST    | `/pprof/symbol`                                                   |
| GET              | `/metrics`                                                        | 查看当前 BE 的 metrics。                                                                                                 |
| HEAD             | `/api/meta/header/{tablet_id}`                                    |
| GET              | `/api/checksum`                                                   |
| GET              | `/api/reload_tablet`                                              |
| POST             | `/api/restore_tablet`                                             |
| GET              | `/api/snapshot`                                                   |
| GET              | `/api/compaction/show?tablet_id={}`                               | 查看指定 tablet 的 compaction 信息。
| POST             | `/api/compact?tablet_id={}&compaction_type={base/cumulative}`     | 手动对指定 tablet 进行 compaction。                                                                                       |
| GET              | `/api/compaction/show_repair`                                     |
| PUT              | `/api/compaction/submit_repair`                                   |
| POST             | `/api/update_config`                                              | 更新 BE 配置，详见 [更新 BE 配置](../administration/Configuration.md#be-配置项)。  |
| GET/PUT          | `/api/runtime_filter_cache/{action}`                              |
| POST             | `/api/compact_rocksdb_meta`                                       |
| GET/PUT          | `/api/query_cache/{action}`                                       |
| GET              | `/api/pipeline_blocking_drivers/{action}`                         |
| GET              | `/greplog`                                                        |
| GET              | `/varz`                                                           | 查看当前 BE 配置。                                                                                                     |

## CN

| HTTP 请求方法       | HTTP 请求路径                                                   | 描述                                                                                                                |
|------------------| --------------------------------------------------------------  |-------------------------------------------------------------------------------------------------------------------- |
| GET              | `/api/health`                                                     |
| GET              | `/pprof/heap`                                                     |
| GET              | `/pprof/growth`                                                   |
| GET              | `/pprof/profile`                                                  |
| GET              | `/pprof/pmuprofile`                                               |
| GET              | `/pprof/contention`                                               |
| GET              | `/pprof/cmdline`                                                  |
| HEAD/GET/POST    | `/pprof/symbol`                                                   |
| GET              | `/metrics`                                                        | 查看当前 CN 的 metrics。                                                                                                 |
