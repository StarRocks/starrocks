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

// Test-only lifecycle trace for the merge-commit latency investigation (config::merge_commit_lifecycle_trace).
// Every line starts with "MCTRACE" followed by space-separated key=value fields. mono_us is CLOCK_MONOTONIC
// and is only comparable within one process; use the glog wall-clock prefix across nodes.

#include <pthread.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "common/config.h"
#include "common/logging.h"
#include "gen_cpp/InternalService_types.h"
#include "util/time.h"

namespace starrocks::mctrace {

inline bool enabled() {
    return config::merge_commit_lifecycle_trace;
}

inline bool enabled_for(TQueryType::type query_type) {
    return config::merge_commit_lifecycle_trace && query_type == TQueryType::LOAD;
}

inline int64_t slow_ns() {
    return config::merge_commit_lifecycle_trace_slow_us * 1000;
}

inline long tid() {
    return syscall(SYS_gettid);
}

inline std::string thread_name() {
    char buf[32] = {0};
    pthread_getname_np(pthread_self(), buf, sizeof(buf));
    return buf;
}

} // namespace starrocks::mctrace

#define MCTRACE_LOG                                                                                        \
    LOG(INFO) << "MCTRACE mono_us=" << (MonotonicNanos() / 1000) << " tid=" << ::starrocks::mctrace::tid() \
              << " tname=" << ::starrocks::mctrace::thread_name() << " "
