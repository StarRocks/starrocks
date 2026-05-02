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

#include "util/global_metrics_registry.h"

namespace starrocks {

GlobalMetricsRegistry* GlobalMetricsRegistry::instance() {
    static GlobalMetricsRegistry instance;
    return &instance;
}

const std::string GlobalMetricsRegistry::_s_registry_name = "starrocks_be";

GlobalMetricsRegistry::GlobalMetricsRegistry() : _process_metrics_registry(_s_registry_name) {}

} // namespace starrocks
