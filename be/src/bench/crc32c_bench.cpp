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

#include <benchmark/benchmark.h>

namespace starrocks {

static char __attribute__((aligned((64)))) buff[1024 * 128];

uint32_t Extend(uint32_t crc, const char* buf, size_t size);
static void BenchMark_crc32c_Eval(benchmark::State& state) {
    uint32_t crc;
    auto offset = state.range(0);
    while (state.KeepRunning()) crc = Extend(0, buff + offset, 16 * 1024);
}
BENCHMARK(BenchMark_crc32c_Eval)->Arg(0)->Arg(5)->Arg(10)->Arg(15);

} // namespace starrocks

BENCHMARK_MAIN();
