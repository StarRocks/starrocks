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

#include <cstddef>
#include <functional>
#include <memory>
#include <vector>

#include "exec/pipeline/pipeline_fwd.h"

namespace starrocks::pipeline {

class FragmentDriverRegistry {
public:
    using DriverConsumer = std::function<void(const DriverPtr&)>;

    const Drivers* register_pipeline(PipelineRawPtr pipeline);
    void reserve(const Drivers* drivers, size_t driver_count);
    void register_driver(const Drivers* drivers, DriverPtr driver);

    size_t driver_count(const Drivers* drivers) const;
    size_t total_driver_count() const { return _total_driver_count; }

    void for_each_driver(const DriverConsumer& call) const;
    void clear();

private:
    struct PipelineDriverGroup {
        PipelineRawPtr pipeline = nullptr;
        Drivers drivers;
    };

    static Drivers* _mutable_drivers(const Drivers* drivers);

    std::vector<std::unique_ptr<PipelineDriverGroup>> _driver_groups;
    size_t _total_driver_count = 0;
};

} // namespace starrocks::pipeline
