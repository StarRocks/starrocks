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

#include <atomic>
#include <cstddef>
#include <string>

namespace starrocks {

class MetricRegistry;

namespace compression {

void register_compression_context_pool_metric(const std::string& pool_name, const std::atomic<size_t>* created_counter);
void install_compression_context_pool_metrics(MetricRegistry* metrics);

} // namespace compression
} // namespace starrocks
