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

#include "platform/platform_env.h"

#include "common/config_exec_env_fwd.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/TFileBrokerService.h"

namespace starrocks {

PlatformEnv::PlatformEnv() = default;

PlatformEnv::~PlatformEnv() = default;

Status PlatformEnv::init(MetricRegistry* metrics) {
    if (_backend_client_cache != nullptr) {
        return Status::OK();
    }

    _backend_client_cache = std::make_unique<BackendServiceClientCache>(config::max_client_cache_size_per_host);
    _frontend_client_cache = std::make_unique<FrontendServiceClientCache>(config::max_client_cache_size_per_host);
    _broker_client_cache = std::make_unique<BrokerServiceClientCache>(config::max_client_cache_size_per_host);

    _backend_client_cache->init_metrics(metrics, "backend");
    _frontend_client_cache->init_metrics(metrics, "frontend");
    _broker_client_cache->init_metrics(metrics, "broker");

    return Status::OK();
}

void PlatformEnv::destroy() {
    _broker_client_cache.reset();
    _frontend_client_cache.reset();
    _backend_client_cache.reset();
}

} // namespace starrocks
