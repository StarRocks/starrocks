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

namespace starrocks {

class TopnRuntimeFilterUpdateContext {
public:
    enum Mode : int32_t {
        OFF = 0,
        AUTO = 1,
        FORCE = 2,
    };

    explicit TopnRuntimeFilterUpdateContext(int32_t mode) : _mode(mode) {}

    void update_stats(int64_t raw_rows_delta, int64_t runtime_filtered_delta) {
        if (raw_rows_delta > 0) {
            _raw_rows.fetch_add(raw_rows_delta, std::memory_order_relaxed);
        }
        if (runtime_filtered_delta > 0) {
            _runtime_filtered.fetch_add(runtime_filtered_delta, std::memory_order_relaxed);
        }
    }

    void on_chunk_output() { _chunks.fetch_add(1, std::memory_order_relaxed); }

    bool should_skip_chunk_accumulate() const {
        if (_mode == FORCE) {
            return true;
        }
        if (_mode == OFF) {
            return false;
        }
        // first 10 chunks skip accumulating, then depends on topn filter rate, this context is per operator
        // since if topn rf on sort key, we want to have a good topn filter asap to skip data by index
        // but if topn can't filter much data after 10 chunks, then it's better to accumulate chunk to decrease push_chunk times
        // TODO: find a better strategy
        return _is_in_warmup() || _is_filtered_over_half();
    }

    bool should_use_small_chunk_threshold() const { return should_skip_chunk_accumulate(); }

private:
    static constexpr int64_t kWarmupChunks = 10;

    bool _is_in_warmup() const { return _chunks.load(std::memory_order_relaxed) < kWarmupChunks; }

    bool _is_filtered_over_half() const {
        const int64_t raw = _raw_rows.load(std::memory_order_relaxed);
        const int64_t filtered = _runtime_filtered.load(std::memory_order_relaxed);
        const int64_t denom = raw + filtered;
        if (denom <= 0) {
            return false;
        }
        return filtered * 2 > denom;
    }

    const int32_t _mode;
    std::atomic<int64_t> _chunks{0};
    std::atomic<int64_t> _raw_rows{0};
    std::atomic<int64_t> _runtime_filtered{0};
};

} // namespace starrocks
