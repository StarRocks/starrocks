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

#include "common/status.h"

#include "util/crc32c.h"

namespace starrocks {

enum class DummyCacheStatus { NORMAL, UPDATING, ABNORMAL, LOADING };

<<<<<<< HEAD:be/src/cache/block_cache/dummy_types.h
struct DummyCacheMetrics {
    struct DirSpace {
        std::string path;
        size_t quota_bytes;
    };
    DummyCacheStatus status;
    int64_t mem_quota_bytes;
    size_t mem_used_bytes;
    size_t disk_quota_bytes;
    size_t disk_used_bytes;
    std::vector<DirSpace> disk_dir_spaces;
    size_t meta_used_bytes = 0;
};
=======
uint32_t Extend(uint32_t crc, const char* buf, size_t size);
static void BenchMark_crc32c_Eval(benchmark::State& state) {
    uint32_t crc;
    auto offset = state.range(0);
    while (state.KeepRunning()) crc = starrocks::crc32c::Extend(0, buff + offset, 16 * 1024);
}
BENCHMARK(BenchMark_crc32c_Eval)->Arg(0)->Arg(5)->Arg(10)->Arg(15);
>>>>>>> d71cc3d2c7 ([BugFix] reduce lock contention of TableMetricsManager (#58911)):be/src/bench/crc32c_bench.cpp

} // namespace starrocks
