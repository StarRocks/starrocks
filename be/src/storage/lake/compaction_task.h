// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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