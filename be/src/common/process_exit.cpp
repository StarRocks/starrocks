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

namespace starrocks {

// NOTE: when BE receiving SIGTERM, this flag will be set to true. Then BE will reject
// all ExecPlanFragments call by returning a fail status(brpc::EINTERNAL).
// After all existing fragments executed, BE will exit.
std::atomic<bool> k_starrocks_exit;

// NOTE: when call `/api/_stop_be` http interface, this flag will be set to true. Then BE will reject
// all ExecPlanFragments call by returning a fail status(brpc::EINTERNAL).
// After all existing fragments executed, BE will exit.
// The difference between k_starrocks_exit and the flag is that
// k_starrocks_exit not only require waiting for all existing fragment to complete,
// but also waiting for all threads to exit gracefully.
std::atomic<bool> k_starrocks_quick_exit;

bool set_process_exit() {
    bool expected = false;
    return k_starrocks_exit.compare_exchange_strong(expected, true);
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

} // namespace starrocks
