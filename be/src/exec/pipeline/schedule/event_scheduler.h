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

#include "exec/pipeline/pipeline_fwd.h"
#include "gutil/macros.h"

namespace starrocks::pipeline {
class DriverQueue;
class EventScheduler {
public:
    EventScheduler() = default;
    DISALLOW_COPY(EventScheduler);

    void add_blocked_driver(const DriverRawPtr driver);

    void try_schedule(const DriverRawPtr driver);

    void attach_queue(DriverQueue* queue) {
        if (_driver_queue == nullptr) {
            _driver_queue = queue;
        }
    }

private:
    DriverQueue* _driver_queue = nullptr;
};
} // namespace starrocks::pipeline