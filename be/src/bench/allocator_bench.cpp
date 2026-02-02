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
#include <glog/logging.h>
#include <jemalloc/jemalloc.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/memory/memory_allocator.h"

namespace starrocks {

constexpr int64_t kBytesPerOp = 1024;
constexpr int kAllocsPerIteration = 10000;

void ensure_global_env() {
    if (!GlobalEnv::is_init()) {
        auto env = GlobalEnv::GetInstance();
        env->_process_mem_tracker = std::make_shared<MemTracker>(-1, "allocator_bench_root");
        env->_is_init = true;
    }
}

namespace {

template <typename Alloc>
void BM_allocator_alloc_free(benchmark::State& state) {
    MemTracker* tracker = GlobalEnv::GetInstance()->process_mem_tracker();
    if (state.thread_index == 0) {
        tracker->set(0);
    }

    // Prepare TLS and bind to process tracker.
    CurrentThread& current = CurrentThread::current();
    CurrentThreadMemTrackerSetter mem_tracker_setter(tracker);
    CHECK(current.mem_tracker() == tracker);
    SCOPED_SET_CATCHED(true);

    Alloc allocator;
    std::vector<void*> ptrs;
    ptrs.reserve(kAllocsPerIteration);

    for (auto _ : state) {
        ptrs.clear();
        for (int i = 0; i < kAllocsPerIteration; ++i) {
            void* ptr = allocator.alloc(kBytesPerOp);
            CHECK(ptr != nullptr);
            ptrs.push_back(ptr);
        }
        benchmark::DoNotOptimize(ptrs.data());
        benchmark::ClobberMemory();
        for (void* ptr : ptrs) {
            allocator.free(ptr, kBytesPerOp);
        }
    }

    // Flush cached counters before reporting.
    current.mem_tracker_ctx_shift();
}

void process_args(benchmark::internal::Benchmark* b) {
    std::vector<int> threads_list = {1, 2, 4, 8, 16, 32, 64};
    for (auto threads : threads_list) {
        b->Threads(threads);
    }
}

} // namespace

using DefaultTrackedAllocator = memory::TrackedAllocator<memory::JemallocAllocator<false>>;
using JemallocAllocator = memory::JemallocAllocator<false>;
using MallocAllocator = memory::MallocAllocator<false>;

BENCHMARK_TEMPLATE(BM_allocator_alloc_free, DefaultTrackedAllocator)
        ->Unit(benchmark::kMillisecond)
        ->Apply(process_args);
BENCHMARK_TEMPLATE(BM_allocator_alloc_free, JemallocAllocator)->Unit(benchmark::kMillisecond)->Apply(process_args);
BENCHMARK_TEMPLATE(BM_allocator_alloc_free, MallocAllocator)->Unit(benchmark::kMillisecond)->Apply(process_args);
} // namespace starrocks

int main(int argc, char** argv) {
    starrocks::ensure_global_env();
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    benchmark::RunSpecifiedBenchmarks();
    return 0;
}

// ------------------------------------------------------------------------------------------------------
// Benchmark                                                            Time             CPU   Iterations
// ------------------------------------------------------------------------------------------------------
// BM_allocator_alloc_free<DefaultTrackedAllocator>/threads:1        1.29 ms         1.29 ms          546
// BM_allocator_alloc_free<DefaultTrackedAllocator>/threads:2       0.678 ms         1.36 ms          494
// BM_allocator_alloc_free<DefaultTrackedAllocator>/threads:4       0.330 ms         1.32 ms          496
// BM_allocator_alloc_free<DefaultTrackedAllocator>/threads:8       0.168 ms         1.34 ms          512
// BM_allocator_alloc_free<DefaultTrackedAllocator>/threads:16      0.083 ms         1.33 ms          512
// BM_allocator_alloc_free<DefaultTrackedAllocator>/threads:32      0.066 ms         2.11 ms          320
// BM_allocator_alloc_free<DefaultTrackedAllocator>/threads:64      0.042 ms         2.66 ms          192
// BM_allocator_alloc_free<JemallocAllocator>/threads:1              1.05 ms         1.05 ms          669
// BM_allocator_alloc_free<JemallocAllocator>/threads:2             0.528 ms         1.06 ms          666
// BM_allocator_alloc_free<JemallocAllocator>/threads:4             0.263 ms         1.05 ms          664
// BM_allocator_alloc_free<JemallocAllocator>/threads:8             0.132 ms         1.06 ms          632
// BM_allocator_alloc_free<JemallocAllocator>/threads:16            0.066 ms         1.06 ms          624
// BM_allocator_alloc_free<JemallocAllocator>/threads:32            0.054 ms         1.72 ms          320
// BM_allocator_alloc_free<JemallocAllocator>/threads:64            0.036 ms         2.20 ms          256
// BM_allocator_alloc_free<MallocAllocator>/threads:1                1.35 ms         1.35 ms          531
// BM_allocator_alloc_free<MallocAllocator>/threads:2               0.663 ms         1.33 ms          528
// BM_allocator_alloc_free<MallocAllocator>/threads:4               0.333 ms         1.33 ms          508
// BM_allocator_alloc_free<MallocAllocator>/threads:8               0.167 ms         1.34 ms          512
// BM_allocator_alloc_free<MallocAllocator>/threads:16              0.099 ms         1.57 ms          496
// BM_allocator_alloc_free<MallocAllocator>/threads:32              0.066 ms         2.12 ms          352
// BM_allocator_alloc_free<MallocAllocator>/threads:64              0.044 ms         2.84 ms          192