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

#include "exec/pipeline/driver_limiter.h"

namespace starrocks::pipeline {

StatusOr<DriverLimiter::TokenPtr> DriverLimiter::try_acquire(int num_drivers) {
    int prev_num_total_drivers = _num_total_drivers.fetch_add(num_drivers);
    if (prev_num_total_drivers >= _max_num_drivers) {
        _num_total_drivers.fetch_sub(num_drivers);
        return Status::TooManyTasks(
                strings::Substitute("BE has overloaded with $0 pipeline drivers", prev_num_total_drivers));
    }
    return std::make_unique<DriverLimiter::Token>(_num_total_drivers, num_drivers);
}

} // namespace starrocks::pipeline
