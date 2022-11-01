// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
    int cpu_used_permille() const;

private:
    uint64_t _get_proc_time();

private:
    static const int NUM_HARDWARE_CORES;
    static const int SECOND_CLOCK_TICK;
    static constexpr int ABSENT_INDEX = -1;

    char* _line_ptr = nullptr;
    size_t _line_buf_size = 0;

    int _curr_idx = ABSENT_INDEX;
    int64_t _timestamp[2];
    uint64_t _proc_time[2];
};

} // namespace starrocks