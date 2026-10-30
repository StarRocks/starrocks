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

#include "compute_env/pipeline/driver_limiter.h"

#include "base/metrics.h"
#include "gutil/macros.h"
#include "gutil/strings/substitute.h"

namespace starrocks::pipeline {

namespace {
const char* const kPipeDriversMetricName = "pipe_drivers";
} // namespace

DriverLimiter::~DriverLimiter() {
    if (_metrics != nullptr) {
        _metrics->deregister_hook(kPipeDriversMetricName);
    }
}

void DriverLimiter::init(MetricRegistry* metrics) {
    if (metrics == nullptr) {
        return;
    }
    if (_metrics != nullptr) {
        DCHECK_EQ(_metrics, metrics);
        return;
    }
    _metrics = metrics;

    metrics->register_metric(kPipeDriversMetricName, &_pipe_drivers);
    metrics->register_hook(kPipeDriversMetricName, [this] { _pipe_drivers.set_value(num_total_drivers()); });
}

StatusOr<DriverLimiter::TokenPtr> DriverLimiter::try_acquire(int num_drivers) {
    int prev_num_total_drivers = _num_total_drivers.fetch_add(num_drivers);
    if (prev_num_total_drivers + num_drivers > _max_num_drivers) {
        _num_total_drivers.fetch_sub(num_drivers);
        return Status::TooManyTasks(
                strings::Substitute("BE has overloaded with pipeline drivers [limit=$0] [used=$1] [request=$2]",
                                    _max_num_drivers, prev_num_total_drivers, num_drivers));
    }
    return std::make_unique<DriverLimiter::Token>(_num_total_drivers, num_drivers);
}

} // namespace starrocks::pipeline
