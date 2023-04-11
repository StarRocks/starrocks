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

#include "http_service.h"

#include "fs/fs_util.h"
#include "gutil/stl_util.h"
#include "http/action/health_action.h"
#include "http/action/metrics_action.h"
#include "http/action/pprof_actions.h"
#include "http/action/snapshot_action.h"
#include "http/action/stream_load.h"
#include "http/default_path_handlers.h"
#include "http/download_action.h"
#include "http/ev_http_server.h"
#include "http/http_method.h"
#include "http/web_page_handler.h"
#include "runtime/exec_env.h"
#include "runtime/load_path_mgr.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

HttpServiceCN::HttpServiceCN(ExecEnv* env, int port, int num_threads)
        : _env(env),
          _ev_http_server(new EvHttpServer(port, num_threads)),
          _web_page_handler(new WebPageHandler(_ev_http_server.get())) {}

HttpServiceCN::~HttpServiceCN() {
    _ev_http_server->stop();
    _ev_http_server.reset();
    _web_page_handler.reset();
    STLDeleteElements(&_http_handlers);
}

Status HttpServiceCN::start() {
    add_default_path_handlers(_web_page_handler.get(), _env->process_mem_tracker());

    // Register CN health action
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

    RETURN_IF_ERROR(_ev_http_server->start());
    return Status::OK();
}

} // namespace starrocks
