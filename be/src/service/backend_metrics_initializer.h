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

#pragma once

#include <string>
#include <vector>

namespace starrocks {

class ProcessMetricsRegistry;
class StarRocksMetrics;

struct BackendMetricsInitOptions {
    std::vector<std::string> storage_paths;
    bool collect_hook_enabled = true;
    bool init_system_metrics = false;
    bool init_jvm_metrics = false;
    bool bind_ipv6 = false;
};

class BackendMetricsInitializer {
public:
    static BackendMetricsInitOptions from_config(std::vector<std::string> storage_paths);

    static void initialize(ProcessMetricsRegistry* process_metrics_registry, StarRocksMetrics* fast_metrics,
                           const BackendMetricsInitOptions& options);
};

} // namespace starrocks
