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

#include "common/process_exit.h"

#include <mutex>

#include "base/time/time.h"
#include "config_exec_env_fwd.h"

namespace starrocks {

// TODO: use bitmask to merge all the atomic variables into one atomic variable.

// SIGTERM flag; admission follows should_accept_new_request().
std::atomic<bool> k_starrocks_exit;

// NOTE: when call `/api/_stop_be` http interface, this flag will be set to true. Then BE will reject
// all ExecPlanFragments call by returning a fail status(brpc::EINTERNAL).
// After all existing fragments executed, BE will exit.
// The difference between k_starrocks_exit and the flag is that
// k_starrocks_exit not only require waiting for all existing fragment to complete,
// but also waiting for all threads to exit gracefully.
std::atomic<bool> k_starrocks_quick_exit;

// First FE shutdown-ack timestamp; starts the admission delay.
std::atomic<int64_t> k_starrocks_fe_aware_shutdown_ms = 0;

// First shutdown timestamp; anchors the fallback deadline.
std::atomic<int64_t> k_starrocks_exit_start_ms = 0;
// NOTE: when BE is crashing (e.g., due to fatal signal), this flag will be set to true.
// In this case, BE will return not alive status to FE's heartbeat request.
// This flag prevents infinite loops when errors occur in jemalloc data structures.
std::atomic<bool> k_starrocks_be_crashing = false;

// Set to reject new fragments and bypass the delay/fallback windows.
std::atomic<bool> k_starrocks_force_reject = false;
std::mutex k_starrocks_admission_mutex;
std::atomic<size_t> k_starrocks_request_admissions_inflight = 0;

RequestAdmissionGuard::RequestAdmissionGuard() {
    std::lock_guard<std::mutex> lock(k_starrocks_admission_mutex);
    _accepted = !k_starrocks_force_reject.load(std::memory_order_seq_cst) && !is_process_crashing() &&
                !process_quick_exit_in_progress();
    if (_accepted) {
        k_starrocks_request_admissions_inflight.fetch_add(1, std::memory_order_seq_cst);
    }
}

RequestAdmissionGuard::~RequestAdmissionGuard() {
    if (_accepted) {
        k_starrocks_request_admissions_inflight.fetch_sub(1, std::memory_order_seq_cst);
    }
}

size_t request_admissions_inflight() {
    return k_starrocks_request_admissions_inflight.load(std::memory_order_seq_cst);
}

bool set_process_exit() {
    bool expected = false;
    if (k_starrocks_exit.compare_exchange_strong(expected, true)) {
        int64_t now = MonotonicMillis();
        int64_t zero = 0;
        k_starrocks_exit_start_ms.compare_exchange_strong(zero, now);
        return true;
    }
    return false;
}

bool set_process_quick_exit() {
    bool expected = false;
    return k_starrocks_quick_exit.compare_exchange_strong(expected, true);
}

bool process_exit_in_progress() {
    return k_starrocks_exit.load(std::memory_order_relaxed) || k_starrocks_quick_exit.load(std::memory_order_relaxed);
}

bool process_quick_exit_in_progress() {
    return k_starrocks_quick_exit.load(std::memory_order_relaxed);
}

void set_frontend_aware_of_exit() {
    // Keep the first heartbeat timestamp; repeats must not extend the window.
    int64_t now = MonotonicMillis();
    int64_t expected = 0;
    k_starrocks_fe_aware_shutdown_ms.compare_exchange_strong(expected, now);
}

void clear_frontend_aware_of_exit() {
    k_starrocks_fe_aware_shutdown_ms.store(0);
}

bool is_frontend_aware_of_exit() {
    return k_starrocks_fe_aware_shutdown_ms.load(std::memory_order_relaxed) != 0;
}

bool should_accept_new_request() {
    if (is_process_crashing()) {
        return false;
    }
    if (!process_exit_in_progress()) {
        return true;
    }
    if (k_starrocks_force_reject.load(std::memory_order_seq_cst) || process_quick_exit_in_progress()) {
        return false;
    }
    if (!config::graceful_exit_wait_for_frontend_heartbeat) {
        return false;
    }

    int64_t now = MonotonicMillis();
    int64_t aware_ms = k_starrocks_fe_aware_shutdown_ms.load(std::memory_order_relaxed);
    if (aware_ms != 0 && now - aware_ms >= config::graceful_exit_reject_delay_ms) {
        return false;
    }

    int64_t exit_start_ms = k_starrocks_exit_start_ms.load(std::memory_order_relaxed);
    if (exit_start_ms == 0) {
        return false;
    }
    return now - exit_start_ms < config::graceful_exit_reject_fallback_ms;
}

void force_reject_exec_plan_fragment() {
    std::lock_guard<std::mutex> lock(k_starrocks_admission_mutex);
    k_starrocks_force_reject.store(true, std::memory_order_seq_cst);
}

void set_process_is_crashing() {
    k_starrocks_be_crashing.store(true, std::memory_order_relaxed);
}

bool is_process_crashing() {
    return k_starrocks_be_crashing.load(std::memory_order_relaxed);
}

} // namespace starrocks
