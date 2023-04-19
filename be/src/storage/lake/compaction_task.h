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
#include <ostream>

#include "common/status.h"

namespace starrocks::lake {

class CompactionTask {
public:
    class Progress {
    public:
        int value() const { return _value.load(std::memory_order_acquire); }

        void update(int value) { _value.store(value, std::memory_order_release); }

    private:
        std::atomic<int> _value{0};
    };

    virtual ~CompactionTask() = default;

    virtual Status execute(Progress* stats) = 0;
};

} // namespace starrocks::lake