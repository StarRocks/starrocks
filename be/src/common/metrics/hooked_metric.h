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
#include <utility>

#include "base/metrics.h"

namespace starrocks {

template <typename Metric, typename UpdateFunc>
bool register_hooked_metric(MetricRegistry* registry, const std::string& name, Metric* metric,
                            UpdateFunc&& update_func) {
    const bool metric_registered = registry->register_metric(name, metric);
    const bool hook_registered = registry->register_hook(
            name, [metric, update_func = std::forward<UpdateFunc>(update_func)]() mutable {
                metric->set_value(update_func());
            });
    return metric_registered && hook_registered;
}

} // namespace starrocks
