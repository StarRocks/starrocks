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

#include "exec/pipeline/fragment_driver_registry.h"

#include <utility>

#include "common/logging.h"

namespace starrocks::pipeline {

const Drivers* FragmentDriverRegistry::register_pipeline(PipelineRawPtr pipeline) {
    DCHECK(pipeline != nullptr);
    auto& group = _driver_groups.emplace_back(std::make_unique<PipelineDriverGroup>());
    group->pipeline = pipeline;
    return &group->drivers;
}

void FragmentDriverRegistry::reserve(const Drivers* drivers, size_t driver_count) {
    _mutable_drivers(drivers)->reserve(driver_count);
}

void FragmentDriverRegistry::register_driver(const Drivers* drivers, DriverPtr driver) {
    DCHECK(driver != nullptr);
    _mutable_drivers(drivers)->emplace_back(std::move(driver));
    ++_total_driver_count;
}

size_t FragmentDriverRegistry::driver_count(const Drivers* drivers) const {
    return drivers == nullptr ? 0 : drivers->size();
}

void FragmentDriverRegistry::for_each_driver(const DriverConsumer& call) const {
    for (const auto& group : _driver_groups) {
        for (const auto& driver : group->drivers) {
            call(driver);
        }
    }
}

void FragmentDriverRegistry::clear() {
    for (const auto& group : _driver_groups) {
        _total_driver_count -= group->drivers.size();
        group->drivers.clear();
    }
    DCHECK_EQ(_total_driver_count, 0);
    _total_driver_count = 0;
}

Drivers* FragmentDriverRegistry::_mutable_drivers(const Drivers* drivers) {
    DCHECK(drivers != nullptr);
    return const_cast<Drivers*>(drivers);
}

} // namespace starrocks::pipeline
