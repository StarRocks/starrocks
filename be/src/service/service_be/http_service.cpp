// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/service/http_service.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http_service.h"

#include "fs/fs_util.h"
#include "gutil/stl_util.h"
#include "http/action/checksum_action.h"
#include "http/action/compact_rocksdb_meta_action.h"
#include "http/action/compaction_action.h"
#include "http/action/health_action.h"
#include "http/action/meta_action.h"
#include "http/action/metrics_action.h"
#include "http/action/pipeline_blocking_drivers_action.h"
#include "http/action/pprof_actions.h"
#include "http/action/query_cache_action.h"
#include "http/action/reload_tablet_action.h"
#include "http/action/restore_tablet_action.h"
#include "http/action/runtime_filter_cache_action.h"
#include "http/action/snapshot_action.h"
#include "http/action/stream_load.h"
#include "http/action/transaction_stream_load.h"
#include "http/action/update_config_action.h"
#include "http/default_path_handlers.h"
#include "http/download_action.h"
#include "http/ev_http_server.h"
#include "http/http_method.h"
#include "http/web_page_handler.h"
#include "runtime/exec_env.h"
#include "runtime/load_path_mgr.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

HttpServiceBE::HttpServiceBE(ExecEnv* env, int port, int num_threads)
        : _env(env),
          _ev_http_server(new EvHttpServer(port, num_threads)),
          _web_page_handler(new WebPageHandler(_ev_http_server.get())) {}

HttpServiceBE::~HttpServiceBE() {
    _ev_http_server->stop();
    _ev_http_server.reset();
    _web_page_handler.reset();
    STLDeleteElements(&_http_handlers);
}

Status HttpServiceBE::start() {
    add_default_path_handlers(_web_page_handler.get(), _env->process_mem_tracker());

    // register load
    auto* stream_load_action = new StreamLoadAction(_env);
    _ev_http_server->register_handler(HttpMethod::PUT, "/api/{db}/{table}/_stream_load", stream_load_action);
    _http_handlers.emplace_back(stream_load_action);

    // Currently, we only support single-DB and single-table transactions.
    // In the future, when we support multi-table transactions across DBs,
    // we can keep the interface compatible, and users do not need to modify the URL.
    //
    // BeginTrasaction:     POST /api/transaction/begin
    // CommitTransaction:   POST /api/transaction/commit
    // RollbackTransaction: POST /api/transaction/rollback
    // PrepreTransaction:   POST /api/transaction/prepare
    //
    // ListTransactions:    POST /api/transaction/list
    auto* transaction_manager_action = new TransactionManagerAction(_env);
    _ev_http_server->register_handler(HttpMethod::POST, "/api/transaction/{txn_op}", transaction_manager_action);
    _ev_http_server->register_handler(HttpMethod::PUT, "/api/transaction/{txn_op}", transaction_manager_action);
    _http_handlers.emplace_back(transaction_manager_action);

    // LoadData:            PUT /api/transaction/load
    auto* transaction_stream_load_action = new TransactionStreamLoadAction(_env);
    _ev_http_server->register_handler(HttpMethod::PUT, "/api/transaction/load", transaction_stream_load_action);
    _http_handlers.emplace_back(transaction_stream_load_action);

    // register download action
    std::vector<std::string> allow_paths;
    for (auto& path : _env->store_paths()) {
        allow_paths.emplace_back(path.path);
    }
    auto* download_action = new DownloadAction(_env, allow_paths);
    _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_download_load", download_action);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/_download_load", download_action);
    _http_handlers.emplace_back(download_action);

    auto* tablet_download_action = new DownloadAction(_env, allow_paths);
    _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_tablet/_download", tablet_download_action);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/_tablet/_download", tablet_download_action);
    _http_handlers.emplace_back(tablet_download_action);

    auto* error_log_download_action = new DownloadAction(_env, _env->load_path_mgr()->get_load_error_file_dir());
    _ev_http_server->register_handler(HttpMethod::GET, "/api/_load_error_log", error_log_download_action);
    _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_load_error_log", error_log_download_action);
    _http_handlers.emplace_back(error_log_download_action);

    // Register BE health action
    auto* health_action = new HealthAction(_env);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/health", health_action);
    _http_handlers.emplace_back(health_action);

    // register pprof actions
    if (!config::pprof_profile_dir.empty()) {
        fs::create_directories(config::pprof_profile_dir);
    }

    auto* heap_action = new HeapAction();
    _ev_http_server->register_handler(HttpMethod::GET, "/pprof/heap", heap_action);
    _http_handlers.emplace_back(heap_action);

    auto* growth_action = new GrowthAction();
    _ev_http_server->register_handler(HttpMethod::GET, "/pprof/growth", growth_action);
    _http_handlers.emplace_back(growth_action);

    auto* profile_action = new ProfileAction();
    _ev_http_server->register_handler(HttpMethod::GET, "/pprof/profile", profile_action);
    _http_handlers.emplace_back(profile_action);

    auto* pmu_profile_action = new PmuProfileAction();
    _ev_http_server->register_handler(HttpMethod::GET, "/pprof/pmuprofile", pmu_profile_action);
    _http_handlers.emplace_back(pmu_profile_action);

    auto* contention_action = new ContentionAction();
    _ev_http_server->register_handler(HttpMethod::GET, "/pprof/contention", contention_action);
    _http_handlers.emplace_back(contention_action);

    auto* cmdline_action = new CmdlineAction();
    _ev_http_server->register_handler(HttpMethod::GET, "/pprof/cmdline", cmdline_action);
    _http_handlers.emplace_back(cmdline_action);

    auto* symbol_action = new SymbolAction(_env->bfd_parser());
    _ev_http_server->register_handler(HttpMethod::GET, "/pprof/symbol", symbol_action);
    _ev_http_server->register_handler(HttpMethod::HEAD, "/pprof/symbol", symbol_action);
    _ev_http_server->register_handler(HttpMethod::POST, "/pprof/symbol", symbol_action);
    _http_handlers.emplace_back(symbol_action);

    // register metrics
    {
        auto action = new MetricsAction(StarRocksMetrics::instance()->metrics());
        _ev_http_server->register_handler(HttpMethod::GET, "/metrics", action);
        _http_handlers.emplace_back(action);
    }

    auto* meta_action = new MetaAction(HEADER);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/meta/header/{tablet_id}", meta_action);
    _http_handlers.emplace_back(meta_action);

#ifndef BE_TEST
    // Register BE checksum action
    auto* checksum_action = new ChecksumAction(_env);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/checksum", checksum_action);
    _http_handlers.emplace_back(checksum_action);

    // Register BE reload tablet action
    auto* reload_tablet_action = new ReloadTabletAction(_env);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/reload_tablet", reload_tablet_action);
    _http_handlers.emplace_back(reload_tablet_action);

    auto* restore_tablet_action = new RestoreTabletAction(_env);
    _ev_http_server->register_handler(HttpMethod::POST, "/api/restore_tablet", restore_tablet_action);
    _http_handlers.emplace_back(restore_tablet_action);

    // Register BE snapshot action
    auto* snapshot_action = new SnapshotAction(_env);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/snapshot", snapshot_action);
    _http_handlers.emplace_back(snapshot_action);
#endif

    // 2 compaction actions
    auto* show_compaction_action = new CompactionAction(CompactionActionType::SHOW_INFO);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/compaction/show", show_compaction_action);
    _http_handlers.emplace_back(show_compaction_action);

    auto* run_compaction_action = new CompactionAction(CompactionActionType::RUN_COMPACTION);
    _ev_http_server->register_handler(HttpMethod::POST, "/api/compact", run_compaction_action);
    _http_handlers.emplace_back(run_compaction_action);

    auto* show_repair_action = new CompactionAction(CompactionActionType::SHOW_REPAIR);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/compaction/show_repair", show_repair_action);
    _http_handlers.emplace_back(show_repair_action);

    auto* submit_repair_action = new CompactionAction(CompactionActionType::SUBMIT_REPAIR);
    _ev_http_server->register_handler(HttpMethod::PUT, "/api/compaction/submit_repair", submit_repair_action);
    _http_handlers.emplace_back(submit_repair_action);

    auto* update_config_action = new UpdateConfigAction(_env);
    _ev_http_server->register_handler(HttpMethod::POST, "/api/update_config", update_config_action);
    _http_handlers.emplace_back(update_config_action);

    auto* runtime_filter_cache_action = new RuntimeFilterCacheAction(_env);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/runtime_filter_cache/{action}",
                                      runtime_filter_cache_action);
    _ev_http_server->register_handler(HttpMethod::PUT, "/api/runtime_filter_cache/{action}",
                                      runtime_filter_cache_action);
    _http_handlers.emplace_back(runtime_filter_cache_action);

    auto* compact_rocksdb_meta_action = new CompactRocksDbMetaAction(_env);
    _ev_http_server->register_handler(HttpMethod::POST, "/api/compact_rocksdb_meta", compact_rocksdb_meta_action);
    _http_handlers.emplace_back(compact_rocksdb_meta_action);

    auto* query_cache_action = new QueryCacheAction(_env);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/query_cache/{action}", query_cache_action);
    _ev_http_server->register_handler(HttpMethod::PUT, "/api/query_cache/{action}", query_cache_action);
    _http_handlers.emplace_back(query_cache_action);

    auto* pipeline_driver_poller_action = new PipelineBlockingDriversAction(_env);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/pipeline_blocking_drivers/{action}",
                                      pipeline_driver_poller_action);
    _http_handlers.emplace_back(pipeline_driver_poller_action);

    RETURN_IF_ERROR(_ev_http_server->start());
    return Status::OK();
}

} // namespace starrocks
