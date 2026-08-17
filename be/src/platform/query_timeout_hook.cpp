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

#include "platform/query_timeout_hook.h"

#include <jemalloc/jemalloc.h>
#include <unistd.h>

#include <cstdio>
#include <limits>
#include <mutex>
#include <string>

#include "base/process/lite_exec.h"
#include "base/uid_util.h"
#include "common/config_diagnostic_fwd.h"
#include "common/logging.h"

namespace starrocks {
namespace {

#define JEMALLOC_CTL je_mallctl

int jemalloc_purge() {
    char buffer[100];
    int res = snprintf(buffer, sizeof(buffer), "arena.%d.purge", MALLCTL_ARENAS_ALL);
    buffer[res] = '\0';
    return JEMALLOC_CTL(buffer, nullptr, nullptr, nullptr, 0);
}

int jemalloc_dontdump() {
    char buffer[100];
    int res = snprintf(buffer, sizeof(buffer), "arena.%d.dontdump", MALLCTL_ARENAS_ALL);
    buffer[res] = '\0';
    return JEMALLOC_CTL(buffer, nullptr, nullptr, nullptr, 0);
}

std::mutex gcore_mutex;
bool gcore_done = false;

} // namespace

void hook_on_query_timeout(const TUniqueId& query_id, size_t timeout_seconds) {
    if (config::pipeline_gcore_timeout_threshold_sec > 0 &&
        timeout_seconds > static_cast<size_t>(config::pipeline_gcore_timeout_threshold_sec)) {
        std::unique_lock<std::mutex> lock(gcore_mutex);
        if (gcore_done) {
            return;
        }

        if (config::enable_core_file_size_optimization) {
            jemalloc_purge();
            jemalloc_dontdump();
        }

        std::string core_file = config::pipeline_gcore_output_dir + "/core-" + print_id(query_id);
        LOG(WARNING) << "dump gcore via query timeout:" << timeout_seconds;
        pid_t pid = getpid();
        // gcore have too many verbose output. skip them
        (void)lite_exec({"gcore", "-o", core_file, std::to_string(pid)}, std::numeric_limits<int>::max());
        LOG(WARNING) << "gcore finished ";
        gcore_done = true;
    }
}

} // namespace starrocks
