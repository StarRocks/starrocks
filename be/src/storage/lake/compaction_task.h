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
    struct Stats {
        std::atomic<int64_t> input_bytes{0};
        std::atomic<int64_t> input_rows{0};
        std::atomic<int64_t> output_bytes{0};
        std::atomic<int64_t> output_rows{0};

        void merge(const Stats& stats2);
    };

    virtual ~CompactionTask() = default;

    virtual Status execute(Stats* stats) = 0;
};

inline void CompactionTask::Stats::merge(const CompactionTask::Stats& stats2) {
    input_bytes.fetch_add(stats2.input_bytes, std::memory_order_relaxed);
    input_rows.fetch_add(stats2.input_rows, std::memory_order_relaxed);
    output_bytes.fetch_add(stats2.output_bytes, std::memory_order_relaxed);
    output_rows.fetch_add(stats2.output_rows, std::memory_order_relaxed);
}

inline std::ostream& operator<<(std::ostream& os, const CompactionTask::Stats& stats) {
    os << "Stats{input_bytes=" << stats.input_bytes.load(std::memory_order_relaxed)
       << " input_rows=" << stats.input_rows.load(std::memory_order_relaxed)
       << " output_bytes=" << stats.output_bytes.load(std::memory_order_relaxed)
       << " output_rows=" << stats.output_rows.load(std::memory_order_relaxed) << "}";
    return os;
}

} // namespace starrocks::lake