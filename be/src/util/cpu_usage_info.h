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
#include <cstdint>

namespace starrocks {

/// Record the cpu usage percentage at an interval.
/// The interval is between the last two invocations of update_interval
/// or from constructor to the first invocation of update_interval.
/// It is not thread-safe.
class CpuUsageRecorder {
public:
    CpuUsageRecorder();
    ~CpuUsageRecorder();

    void update_interval();
    // Return the cpu usage ratio in parts per thousand at an interval.
    int cpu_used_permille() const;

private:
    uint64_t _get_proc_time();

private:
    static const int NUM_HARDWARE_CORES;
    static const int SECOND_CLOCK_TICK;
    static constexpr int ABSENT_INDEX = -1;

    char* _line_ptr = nullptr;
    size_t _line_buf_size = 0;

    // Record the latest two time points, where the current recorded point is at `_curr_idx`,
    // and the previous recorded point is at `(_curr_idx+1)%2`.
    int _curr_idx = ABSENT_INDEX;
    int64_t _timestamp[2];
    uint64_t _proc_time[2];
};

} // namespace starrocks