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
#include <cstddef>

namespace starrocks {

// set the process exit flag.
// returns:
//  - true: the exit flag is set from `false` to `true`
//  - false: the exit flag is already `true`
bool set_process_exit();

// set the process quick exit flag.
// returns:
//  - true: the quick exit flag is set from `false` to `true`
//  - false: the quick exit flag is already `true`
bool set_process_quick_exit();

// whether the process is in the progress of exiting
// returns:
//  - true: either in exit or quick exit
//  - false: neither exit nor quick exit
bool process_exit_in_progress();

// whether the process is in the progress of quick exiting
// returns:
//  - true: process is in quick exit
//  - false: process is not in quick exit
bool process_quick_exit_in_progress();

// Mark that the heartbeat ack advanced: the FE processed a heartbeat response this BE sent
// after shutdown began (it reports SHUTDOWN), so the node is marked SHUTDOWN/not-alive globally.
void set_frontend_aware_of_exit();

// whether the FE leader is aware of the shutdown
// returns:
//  - true: the heartbeat ack advanced at least once during shutdown
//  - false: the ack has not advanced yet
bool is_frontend_aware_of_exit();

// Tracks the FE's last-seen heartbeat time (the shutdown ack). Returns true when the ack value
// advances; the first observed value anchors the baseline and returns false.
bool advance_heartbeat_ack(int64_t ack);

// clear the flag of frontend awareness of the shutdown.
void clear_frontend_aware_of_exit();

// Whether a new request may be accepted during graceful shutdown.
bool should_accept_new_request();

class RequestAdmissionGuard {
public:
    RequestAdmissionGuard();
    ~RequestAdmissionGuard();
    RequestAdmissionGuard(const RequestAdmissionGuard&) = delete;
    RequestAdmissionGuard& operator=(const RequestAdmissionGuard&) = delete;

    bool accepted() const { return _accepted; }

private:
    bool _accepted = false;
};

size_t request_admissions_inflight();

// Force the BE to reject new fragments immediately before teardown.
void force_reject_exec_plan_fragment();

void set_process_is_crashing();
bool is_process_crashing();

} // namespace starrocks
