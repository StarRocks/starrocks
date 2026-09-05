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

#include <gtest/gtest.h>

#include <cstdio>
#include <memory>

#include "common/configbase.h"
#include "common/system/cpu_info.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "types/time_types.h"

namespace starrocks {
namespace {

bool g_env_initialized = false;
MemTracker* g_process_mem_tracker = nullptr;

bool is_env_initialized_for_test() {
    return g_env_initialized;
}

MemTracker* process_mem_tracker_for_test() {
    return g_process_mem_tracker;
}

} // namespace
} // namespace starrocks

int main(int argc, char** argv) {
    if (!starrocks::config::init(nullptr)) {
        std::fprintf(stderr, "failed to initialize config defaults\n");
        return 1;
    }
    starrocks::date::init_date_cache();
    starrocks::CpuInfo::init();

    auto process_mem_tracker = std::make_unique<starrocks::MemTracker>(-1, "connector_iceberg_test");
    starrocks::g_env_initialized = true;
    starrocks::g_process_mem_tracker = process_mem_tracker.get();
    starrocks::tls_mem_tracker = nullptr;
    starrocks::CurrentThread::set_mem_tracker_source(starrocks::is_env_initialized_for_test,
                                                     starrocks::process_mem_tracker_for_test);

    ::testing::InitGoogleTest(&argc, argv);
    const int result = RUN_ALL_TESTS();

    starrocks::CurrentThread::current().set_mem_tracker(nullptr);
    starrocks::CurrentThread::set_mem_tracker_source(nullptr, nullptr);
    starrocks::tls_mem_tracker = nullptr;
    starrocks::g_env_initialized = false;
    starrocks::g_process_mem_tracker = nullptr;
    return result;
}
