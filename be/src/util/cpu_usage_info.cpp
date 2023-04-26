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

#include "util/cpu_usage_info.h"

#include <glog/logging.h>

#include <cstdio>
#include <thread>

#include "util/cpu_info.h"
#include "util/defer_op.h"
#include "util/time.h"

namespace starrocks {

const int CpuUsageRecorder::NUM_HARDWARE_CORES = std::thread::hardware_concurrency();
const int CpuUsageRecorder::SECOND_CLOCK_TICK = sysconf(_SC_CLK_TCK);

CpuUsageRecorder::CpuUsageRecorder() : _timestamp{0, MonotonicNanos()}, _proc_time{0, _get_proc_time()} {}

CpuUsageRecorder::~CpuUsageRecorder() {
    if (_line_ptr != nullptr) {
        free(_line_ptr);
    }
}

void CpuUsageRecorder::update_interval() {
    _curr_idx = (_curr_idx + 1) % 2;
    _proc_time[_curr_idx] = _get_proc_time();
    _timestamp[_curr_idx] = MonotonicNanos();
}

int CpuUsageRecorder::cpu_used_permille() const {
    if (_curr_idx == ABSENT_INDEX) {
        // Haven't called update_interval.
        return 0;
    }
    int prev_idx = (_curr_idx + 1) % 2;
    return (_proc_time[_curr_idx] - _proc_time[prev_idx]) * 1000'000'000L / SECOND_CLOCK_TICK * 1000 /
           NUM_HARDWARE_CORES / (_timestamp[_curr_idx] - _timestamp[prev_idx]);
}

uint64_t CpuUsageRecorder::_get_proc_time() {
    FILE* fp = fopen("/proc/self/stat", "r");
    if (fp == nullptr) {
        LOG(WARNING) << "open /proc/self/stat failed";
        return 0;
    }
    DeferOp close_fp([fp] { fclose(fp); });

    if (getline(&_line_ptr, &_line_buf_size, fp) < 0) {
        LOG(WARNING) << "getline from /proc/self/stat failed";
        return 0;
    }

    uint64_t utime = 0;
    uint64_t stime = 0;
    sscanf(_line_ptr, "%*d %*s %*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %lu %lu", &utime, &stime);
    return utime + stime;
}

} // namespace starrocks
