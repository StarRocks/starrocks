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

#include <chrono>
#include <cstdint>
#include <string>

#include "common/logging.h"
#include "gutil/bits.h"

namespace starrocks::pipeline {

enum DriverState : uint32_t {
    NOT_READY = 0,
    READY = 1,
    RUNNING = 2,
    INPUT_EMPTY = 3,
    OUTPUT_FULL = 4,
    PRECONDITION_BLOCK = 5,
    FINISH = 6,
    CANCELED = 7,
    INTERNAL_ERROR = 8,
    // PENDING_FINISH means a driver's SinkOperator has finished, but its SourceOperator still have a pending
    // io task executed by io threads synchronously, a driver turns to FINISH from PENDING_FINISH after the
    // pending io task's completion.
    PENDING_FINISH = 9,
    // In some cases, the output of SourceOperator::has_output may change frequently, it's better to wait
    // in the working thread other than moving the driver frequently between ready queue and pending queue, which
    // will lead to drastic performance deduction (the "ScheduleTime" in profile will be super high).
    // We can enable this optimization by overriding SourceOperator::is_mutable to return true.
    LOCAL_WAITING = 10
};

[[maybe_unused]] static inline std::string ds_to_string(DriverState ds) {
    switch (ds) {
    case NOT_READY:
        return "NOT_READY";
    case READY:
        return "READY";
    case RUNNING:
        return "RUNNING";
    case INPUT_EMPTY:
        return "INPUT_EMPTY";
    case OUTPUT_FULL:
        return "OUTPUT_FULL";
    case PRECONDITION_BLOCK:
        return "PRECONDITION_BLOCK";
    case FINISH:
        return "FINISH";
    case CANCELED:
        return "CANCELED";
    case INTERNAL_ERROR:
        return "INTERNAL_ERROR";
    case PENDING_FINISH:
        return "PENDING_FINISH";
    case LOCAL_WAITING:
        return "LOCAL_WAITING";
    }
    DCHECK(false);
    return "UNKNOWN_STATE";
}

// DriverAcct is used to keep statistics of drivers' runtime information, such as time spent
// on core, number of chunks already processed, which are taken into consideration by DriverQueue
// for schedule.
class DriverAcct {
public:
    DriverAcct() = default;
    // TODO:
    // get_level return a non-negative value that is a hint used by DriverQueue to choose
    // the target internal queue for put_back.
    int get_level() { return Bits::Log2Floor64(schedule_times + 1); }

    // get last elapsed time for process.
    int64_t get_last_time_spent() { return last_time_spent; }

    void update_last_time_spent(int64_t time_spent) {
        this->last_time_spent = time_spent;
        this->accumulated_time_spent += time_spent;
        this->accumulated_local_wait_time_spent += time_spent;
    }
    // This method must be invoked when adding back to ready queue or pending queue.
    void clean_local_queue_infos() {
        this->accumulated_local_wait_time_spent = 0;
        this->enter_local_queue_timestamp = 0;
    }
    void update_enter_local_queue_timestamp() {
        enter_local_queue_timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              std::chrono::steady_clock::now().time_since_epoch())
                                              .count();
    }
    void update_last_chunks_moved(int64_t chunks_moved) {
        this->last_chunks_moved = chunks_moved;
        this->accumulated_chunks_moved += chunks_moved;
        this->schedule_effective_times += (chunks_moved > 0) ? 1 : 0;
    }
    void update_accumulated_rows_moved(int64_t rows_moved) { this->accumulated_rows_moved += rows_moved; }
    void increment_schedule_times() { this->schedule_times += 1; }

    int64_t get_accumulated_local_wait_time_spent() { return accumulated_local_wait_time_spent; }
    int64_t get_local_queue_time_spent() {
        const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                 std::chrono::steady_clock::now().time_since_epoch())
                                 .count();
        return now - enter_local_queue_timestamp;
    }
    int64_t get_schedule_times() { return schedule_times; }
    int64_t get_schedule_effective_times() { return schedule_effective_times; }
    int64_t get_rows_per_chunk() {
        if (accumulated_chunks_moved > 0) {
            return accumulated_rows_moved / accumulated_chunks_moved;
        } else {
            return 0;
        }
    }
    int64_t get_accumulated_chunks_moved() { return accumulated_chunks_moved; }
    int64_t get_accumulated_time_spent() const { return accumulated_time_spent; }

private:
    int64_t schedule_times{0};
    int64_t schedule_effective_times{0};
    int64_t last_time_spent{0};
    int64_t last_chunks_moved{0};
    int64_t enter_local_queue_timestamp{0};
    int64_t accumulated_time_spent{0};
    int64_t accumulated_local_wait_time_spent{0};
    int64_t accumulated_chunks_moved{0};
    int64_t accumulated_rows_moved{0};
};

} // namespace starrocks::pipeline
