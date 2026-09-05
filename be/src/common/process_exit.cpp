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

// Latest heartbeat ack (acking FE address, time) seen during shutdown. Advances are compared
// only within one source: a leader handover may come with an unsynchronized wall clock, so a
// source switch re-anchors the baseline instead of dead-locking on cross-clock comparison.
std::mutex k_starrocks_ack_mutex;
std::string k_starrocks_last_ack_source;
int64_t k_starrocks_last_ack_ms = 0;

// Monotonic timestamp of the first heartbeat ack observed after shutdown began; 0 means the
// FE has not yet confirmed the shutdown, so the delay window (reject_delay_ms) is not open.
std::atomic<int64_t> k_starrocks_fe_aware_shutdown_ms = 0;

// First shutdown timestamp; anchors the fallback deadline.
std::atomic<int64_t> k_starrocks_exit_start_ms = 0;
// NOTE: when BE is crashing (e.g., due to fatal signal), this flag will be set to true.
// In this case, BE will return not alive status to FE's heartbeat request.
// This flag prevents infinite loops when errors occur in jemalloc data structures.
std::atomic<bool> k_starrocks_be_crashing = false;

// Set to reject new fragments and bypass the delay/fallback windows.
std::atomic<bool> k_starrocks_force_reject = false;
// Set when a different FE leader (source) starts acking during this shutdown: the 307 redirect
// path is disabled for the rest of the shutdown (conservative failover downgrade).
std::atomic<bool> k_starrocks_redirect_disabled = false;
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
    // Called when the heartbeat ack advances: the FE processed a heartbeat response this BE sent
    // after shutdown began (it reports SHUTDOWN), so the node is marked SHUTDOWN/not-alive
    // globally.
    int64_t now = MonotonicMillis();
    int64_t expected = 0;
    k_starrocks_fe_aware_shutdown_ms.compare_exchange_strong(expected, now);
}

void clear_frontend_aware_of_exit() {
    k_starrocks_fe_aware_shutdown_ms.store(0);
    k_starrocks_redirect_disabled.store(false, std::memory_order_relaxed);
    std::lock_guard<std::mutex> l(k_starrocks_ack_mutex);
    k_starrocks_last_ack_source.clear();
    k_starrocks_last_ack_ms = 0;
}

bool is_frontend_aware_of_exit() {
    return k_starrocks_fe_aware_shutdown_ms.load(std::memory_order_relaxed) != 0;
}

// Tracks the FE's last-seen heartbeat time (the shutdown ack, echoed in every heartbeat
// request). `ack_source` identifies the FE that sent the ack (its heartbeat network address).
// Returns true when the ack advances relative to the current source's baseline, meaning that
// FE processed a heartbeat response this BE sent after shutdown began. The first value of a
// source is only the baseline (it may correspond to a pre-shutdown response, and its wall
// clock is not comparable with the previous leader's) and returns false.
bool advance_heartbeat_ack(const std::string& ack_source, int64_t ack) {
    std::lock_guard<std::mutex> l(k_starrocks_ack_mutex);
    if (ack_source != k_starrocks_last_ack_source) {
        if (!k_starrocks_last_ack_source.empty()) {
            // Leader handover while shutting down: the new leader's wall clock is not
            // comparable and the old redirect target may be gone, so disable redirect for the
            // rest of this shutdown. A later advance of the new source still opens the delay
            // window (new BEGINs keep being admitted) but never re-enables redirect.
            k_starrocks_redirect_disabled = true;
        }
        k_starrocks_last_ack_source = ack_source;
        k_starrocks_last_ack_ms = ack;
        return false;
    }
    if (ack > k_starrocks_last_ack_ms) {
        k_starrocks_last_ack_ms = ack;
        return true;
    }
    return false;
}

bool may_redirect_to_fe_leader() {
    if (k_starrocks_redirect_disabled.load(std::memory_order_relaxed)) {
        return false;
    }
    return is_frontend_aware_of_exit();
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
