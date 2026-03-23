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
#include <jemalloc/jemalloc.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <initializer_list>
#include <vector>

#include "column/buffer.h"
#include "common/memory/column_allocator.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/memory/memory_allocator.h"

namespace starrocks {

MemTracker* process_mem_tracker_provider() {
    return GlobalEnv::GetInstance()->process_mem_tracker();
}

void ensure_global_env() {
    CurrentThread::set_mem_tracker_source(&GlobalEnv::is_init, process_mem_tracker_provider);
    if (!GlobalEnv::is_init()) {
        auto env = GlobalEnv::GetInstance();
        env->_process_mem_tracker = std::make_shared<MemTracker>(-1, "buffer_bench_root");
        env->_is_init = true;
    }
}

namespace {

memory::Allocator* kCachedBufferBenchAllocator = memory::get_default_allocator();

struct ScopedBenchMemTrackerContext {
    explicit ScopedBenchMemTrackerContext(benchmark::State& state)
            : tracker(GlobalEnv::GetInstance()->process_mem_tracker()), mem_tracker_setter(tracker) {
        if (state.thread_index == 0) {
            tracker->set(0);
        }
    }

    ~ScopedBenchMemTrackerContext() { CurrentThread::current().mem_tracker_ctx_shift(); }

    MemTracker* tracker;
    CurrentThreadMemTrackerSetter mem_tracker_setter;
};

// Reset jemalloc arena state by purging unused memory
static void reset_arena_state() {
#ifdef __APPLE__
#define JEMALLOC_CTL mallctl
#else
#define JEMALLOC_CTL je_mallctl
#endif
    char buffer[100];
#ifndef MALLCTL_ARENAS_ALL
    constexpr unsigned int MALLCTL_ARENAS_ALL_VALUE = 4096;
#else
    constexpr unsigned int MALLCTL_ARENAS_ALL_VALUE = MALLCTL_ARENAS_ALL;
#endif
    int res = snprintf(buffer, sizeof(buffer), "arena.%u.purge", MALLCTL_ARENAS_ALL_VALUE);
    if (res > 0 && res < static_cast<int>(sizeof(buffer))) {
        buffer[res] = '\0';
        JEMALLOC_CTL(buffer, nullptr, nullptr, nullptr, 0);
    }
#undef JEMALLOC_CTL
}

template <typename T>
using ColumnVector = std::vector<T, ColumnAllocator<T>>;
template <typename T, size_t padding = 0>
using Buffer = util::Buffer<T, padding>;

constexpr int64_t kInitListSize = 8;

template <typename T>
struct ValueFactory {
    static T make(int64_t value) { return static_cast<T>(value); }
};

template <typename T>
struct InitListFactory;

template <>
struct InitListFactory<int64_t> {
    static const std::initializer_list<int64_t>& list() {
        static const std::initializer_list<int64_t> values = {0, 1, 2, 3, 4, 5, 6, 7};
        return values;
    }
};

template <typename T>
struct BufferOps {
    using ValueType = T;
    using Container = Buffer<T>;

    static Container make() { return Container(kCachedBufferBenchAllocator); }
    static void reserve(Container& c, int64_t n) { c.reserve(static_cast<size_t>(n)); }
    static void shrink_to_fit(Container& c) { c.shrink_to_fit(); }
    static void resize(Container& c, int64_t n) { c.resize(static_cast<size_t>(n)); }
    static void resize_value(Container& c, int64_t n, const T& value) { c.resize(static_cast<size_t>(n), value); }
    static void assign(Container& c, int64_t n, const T& value) { c.assign(static_cast<size_t>(n), value); }
    template <typename InputIt>
    static void assign_range(Container& c, InputIt first, InputIt last) {
        c.assign(first, last);
    }
    static void assign_init_list(Container& c, std::initializer_list<T> list) { c.assign(list); }
    static void push_back(Container& c, int64_t i) { c.push_back(ValueFactory<T>::make(i)); }
    static void append(Container& c, int64_t n, const T& value) { c.append(static_cast<size_t>(n), value); }
    template <typename InputIt>
    static void append_range(Container& c, InputIt first, InputIt last) {
        c.append(first, last);
    }
    static void append_init_list(Container& c, std::initializer_list<T> list) { c.append(list); }
};

template <typename T>
struct VectorOps {
    using ValueType = T;
    using Container = ColumnVector<T>;

    static Container make() { return Container(); }
    static void reserve(Container& c, int64_t n) { c.reserve(static_cast<size_t>(n)); }
    static void shrink_to_fit(Container& c) { c.shrink_to_fit(); }
    static void resize(Container& c, int64_t n) { c.resize(static_cast<size_t>(n)); }
    static void resize_value(Container& c, int64_t n, const T& value) { c.resize(static_cast<size_t>(n), value); }
    static void assign(Container& c, int64_t n, const T& value) { c.assign(static_cast<size_t>(n), value); }
    template <typename InputIt>
    static void assign_range(Container& c, InputIt first, InputIt last) {
        c.assign(first, last);
    }
    static void assign_init_list(Container& c, std::initializer_list<T> list) { c.assign(list); }
    static void push_back(Container& c, int64_t i) { c.push_back(ValueFactory<T>::make(i)); }
    static void append(Container& c, int64_t n, const T& value) { c.insert(c.end(), static_cast<size_t>(n), value); }
    template <typename InputIt>
    static void append_range(Container& c, InputIt first, InputIt last) {
        c.insert(c.end(), first, last);
    }
    static void append_init_list(Container& c, std::initializer_list<T> list) { c.insert(c.end(), list); }
};

static void apply_counts(benchmark::internal::Benchmark* b) {
    for (int64_t n :
         {16, 128, 512, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 134217728}) {
        b->Arg(n);
    }
}

// Focused range for single-grow benchmark: covers the region where jemalloc
// xallocx transitions from failing (small slab allocs) to succeeding (large
// extent allocs) and back to failing (virtual address fragmentation).
static void apply_grow_counts(benchmark::internal::Benchmark* b) {
    for (int64_t n : {1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216}) {
        b->Arg(n);
    }
}

static void apply_init_list(benchmark::internal::Benchmark* b) {
    b->Arg(kInitListSize);
}

template <typename Ops>
void run_reserve_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    ScopedBenchMemTrackerContext tracker_ctx(state);
    reset_arena_state();
    const int64_t n = state.range(0);
    for (auto _ : state) {
        auto container = Ops::make();
        Ops::reserve(container, n);
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_shrink_to_fit_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    const int64_t n = state.range(0);
    const auto value = ValueFactory<typename Ops::ValueType>::make(1);
    for (auto _ : state) {
        state.PauseTiming();
        auto container = Ops::make();
        // reserve(n) for excess capacity, then assign(n/2) to fill real data.
        // This ensures pages are faulted in equally for both Buffer and Vector.
        Ops::reserve(container, n);
        Ops::assign(container, n / 2, value);
        state.ResumeTiming();
        Ops::shrink_to_fit(container);
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_resize_value_grow_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    const int64_t n = state.range(0);
    const int64_t base = n / 2;
    const auto value = ValueFactory<typename Ops::ValueType>::make(1);
    for (auto _ : state) {
        state.PauseTiming();
        auto container = Ops::make();
        if (base > 0) {
            Ops::assign(container, base, value);
        }
        state.ResumeTiming();
        Ops::resize_value(container, n, value);
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_assign_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    const int64_t n = state.range(0);
    const auto value = ValueFactory<typename Ops::ValueType>::make(1);
    for (auto _ : state) {
        auto container = Ops::make();
        Ops::assign(container, n, value);
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_assign_range_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    const int64_t n = state.range(0);
    std::vector<typename Ops::ValueType> values;
    values.reserve(static_cast<size_t>(n));
    for (int64_t i = 0; i < n; ++i) {
        values.push_back(ValueFactory<typename Ops::ValueType>::make(i));
    }
    const auto* data = values.data();
    for (auto _ : state) {
        auto container = Ops::make();
        Ops::assign_range(container, data, data + values.size());
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_assign_init_list_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    const auto& values = InitListFactory<typename Ops::ValueType>::list();
    for (auto _ : state) {
        auto container = Ops::make();
        Ops::assign_init_list(container, values);
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_push_back_no_reserve_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    const int64_t n = state.range(0);
    for (auto _ : state) {
        auto container = Ops::make();
        for (int64_t i = 0; i < n; ++i) {
            Ops::push_back(container, i);
        }
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_push_back_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    const int64_t n = state.range(0);
    for (auto _ : state) {
        state.PauseTiming();
        auto container = Ops::make();
        Ops::reserve(container, n);
        state.ResumeTiming();
        for (int64_t i = 0; i < n; ++i) {
            Ops::push_back(container, i);
        }
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_append_reserve_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    const int64_t n = state.range(0);
    const auto value = ValueFactory<typename Ops::ValueType>::make(1);
    for (auto _ : state) {
        state.PauseTiming();
        auto container = Ops::make();
        Ops::reserve(container, n);
        state.ResumeTiming();
        Ops::append(container, n, value);
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_append_range_reserve_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    const int64_t n = state.range(0);
    std::vector<typename Ops::ValueType> values;
    values.reserve(static_cast<size_t>(n));
    for (int64_t i = 0; i < n; ++i) {
        values.push_back(ValueFactory<typename Ops::ValueType>::make(i));
    }
    const auto* data = values.data();
    for (auto _ : state) {
        state.PauseTiming();
        auto container = Ops::make();
        Ops::reserve(container, n);
        state.ResumeTiming();
        Ops::append_range(container, data, data + values.size());
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_append_init_list_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    const auto& values = InitListFactory<typename Ops::ValueType>::list();
    for (auto _ : state) {
        auto container = Ops::make();
        Ops::append_init_list(container, values);
        benchmark::DoNotOptimize(container);
    }
}

// Measures the cost of a single grow event: buffer is full at n elements,
// then reserve(2n) triggers a relocate. This isolates realloc/xallocx benefit:
// - Vector: always alloc(2n) + memcpy(n) + free(old)
// - Buffer (xallocx success): in-place extend, no copy
// - Buffer (xallocx fail): same as Vector
// NOTE: uses assign(n, value) instead of resize(n) to ensure both sides
// write real data, so pages are faulted in equally before the measured grow.
template <typename Ops>
void run_single_grow_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    const int64_t n = state.range(0);
    const auto value = ValueFactory<typename Ops::ValueType>::make(1);
    for (auto _ : state) {
        state.PauseTiming();
        auto container = Ops::make();
        Ops::assign(container, n, value);
        state.ResumeTiming();
        Ops::reserve(container, n * 2);
        benchmark::DoNotOptimize(container);
        benchmark::ClobberMemory();
    }
}

#define REGISTER_OP_BENCH(op, type)                               \
    BENCHMARK_TEMPLATE(op, VectorOps<type>)->Apply(apply_counts); \
    BENCHMARK_TEMPLATE(op, BufferOps<type>)->Apply(apply_counts)

#define REGISTER_INIT_LIST_BENCH(op, type)                           \
    BENCHMARK_TEMPLATE(op, BufferOps<type>)->Apply(apply_init_list); \
    BENCHMARK_TEMPLATE(op, VectorOps<type>)->Apply(apply_init_list)

REGISTER_OP_BENCH(run_reserve_bench, int64_t);
REGISTER_OP_BENCH(run_shrink_to_fit_bench, int64_t);
REGISTER_OP_BENCH(run_resize_value_grow_bench, int64_t);
REGISTER_OP_BENCH(run_assign_bench, int64_t);
REGISTER_OP_BENCH(run_assign_range_bench, int64_t);
REGISTER_INIT_LIST_BENCH(run_assign_init_list_bench, int64_t);
REGISTER_OP_BENCH(run_push_back_no_reserve_bench, int64_t);
REGISTER_OP_BENCH(run_push_back_bench, int64_t);
BENCHMARK_TEMPLATE(run_single_grow_bench, VectorOps<int64_t>)->Apply(apply_grow_counts);
BENCHMARK_TEMPLATE(run_single_grow_bench, BufferOps<int64_t>)->Apply(apply_grow_counts);
REGISTER_OP_BENCH(run_append_reserve_bench, int64_t);
REGISTER_OP_BENCH(run_append_range_reserve_bench, int64_t);
REGISTER_INIT_LIST_BENCH(run_append_init_list_bench, int64_t);

#undef REGISTER_OP_BENCH
#undef REGISTER_INIT_LIST_BENCH

} // namespace
} // namespace starrocks

int main(int argc, char** argv) {
    starrocks::ensure_global_env();
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}

/*
Run on (104 X 3800 MHz CPU s)
CPU Caches:
  L1 Data 32 KiB (x52)
  L1 Instruction 32 KiB (x52)
  L2 Unified 1024 KiB (x52)
  L3 Unified 36608 KiB (x2)
Load Average: 1.95, 2.13, 1.97
***WARNING*** CPU scaling is enabled, the benchmark real time measurements may be noisy and will incur extra overhead.
-------------------------------------------------------------------------------------------------------
Benchmark                                                             Time             CPU   Iterations
-------------------------------------------------------------------------------------------------------
run_reserve_bench<VectorOps<int64_t>>/16                           32.3 ns         32.3 ns     21674505
run_reserve_bench<VectorOps<int64_t>>/128                          33.6 ns         33.6 ns     20798126
run_reserve_bench<VectorOps<int64_t>>/512                          38.2 ns         38.2 ns     18310030
run_reserve_bench<VectorOps<int64_t>>/1024                         49.6 ns         49.6 ns     14196030
run_reserve_bench<VectorOps<int64_t>>/4096                         72.3 ns         72.3 ns      9688721
run_reserve_bench<VectorOps<int64_t>>/16384                         335 ns          335 ns      2092640
run_reserve_bench<VectorOps<int64_t>>/65536                         333 ns          333 ns      2103856
run_reserve_bench<VectorOps<int64_t>>/262144                        345 ns          345 ns      2031272
run_reserve_bench<VectorOps<int64_t>>/1048576                       956 ns          956 ns       732117
run_reserve_bench<VectorOps<int64_t>>/4194304                      1065 ns         1065 ns       657085
run_reserve_bench<VectorOps<int64_t>>/16777216                     1422 ns         1422 ns       489715
run_reserve_bench<VectorOps<int64_t>>/67108864                     1221 ns         1221 ns       572554
run_reserve_bench<VectorOps<int64_t>>/134217728                     900 ns          900 ns       777732
run_reserve_bench<BufferOps<int64_t>>/16                           31.7 ns         31.7 ns     22100814
run_reserve_bench<BufferOps<int64_t>>/128                          32.7 ns         32.7 ns     21472416
run_reserve_bench<BufferOps<int64_t>>/512                          36.1 ns         36.1 ns     19413134
run_reserve_bench<BufferOps<int64_t>>/1024                         47.9 ns         47.9 ns     14609525
run_reserve_bench<BufferOps<int64_t>>/4096                         71.8 ns         71.7 ns      9754549
run_reserve_bench<BufferOps<int64_t>>/16384                         336 ns          336 ns      2081683
run_reserve_bench<BufferOps<int64_t>>/65536                         334 ns          334 ns      2092507
run_reserve_bench<BufferOps<int64_t>>/262144                        347 ns          347 ns      2018214
run_reserve_bench<BufferOps<int64_t>>/1048576                       987 ns          987 ns       705792
run_reserve_bench<BufferOps<int64_t>>/4194304                      1085 ns         1085 ns       645010
run_reserve_bench<BufferOps<int64_t>>/16777216                     1442 ns         1442 ns       480257
run_reserve_bench<BufferOps<int64_t>>/67108864                     1236 ns         1236 ns       566507
run_reserve_bench<BufferOps<int64_t>>/134217728                     904 ns          903 ns       775535
run_shrink_to_fit_bench<VectorOps<int64_t>>/16                      304 ns          306 ns      2261769
run_shrink_to_fit_bench<VectorOps<int64_t>>/128                     306 ns          309 ns      2249856
run_shrink_to_fit_bench<VectorOps<int64_t>>/512                     329 ns          331 ns      2152036
run_shrink_to_fit_bench<VectorOps<int64_t>>/1024                    354 ns          357 ns      1947019
run_shrink_to_fit_bench<VectorOps<int64_t>>/4096                    790 ns          803 ns       872834
run_shrink_to_fit_bench<VectorOps<int64_t>>/16384                  2404 ns         2410 ns       290031
run_shrink_to_fit_bench<VectorOps<int64_t>>/65536                  6960 ns         6965 ns       100313
run_shrink_to_fit_bench<VectorOps<int64_t>>/262144                76849 ns        76885 ns         8985
run_shrink_to_fit_bench<VectorOps<int64_t>>/1048576              458915 ns       458836 ns         1534
run_shrink_to_fit_bench<VectorOps<int64_t>>/4194304             7313247 ns      7312331 ns           98
run_shrink_to_fit_bench<VectorOps<int64_t>>/16777216           39543078 ns     39539662 ns           18
run_shrink_to_fit_bench<VectorOps<int64_t>>/67108864          150274227 ns    150258132 ns            5
run_shrink_to_fit_bench<VectorOps<int64_t>>/134217728         290593175 ns    290563101 ns            2
run_shrink_to_fit_bench<BufferOps<int64_t>>/16                      295 ns          298 ns      2335014
run_shrink_to_fit_bench<BufferOps<int64_t>>/128                     306 ns          309 ns      2253448
run_shrink_to_fit_bench<BufferOps<int64_t>>/512                     300 ns          303 ns      2274753
run_shrink_to_fit_bench<BufferOps<int64_t>>/1024                    308 ns          311 ns      2276964
run_shrink_to_fit_bench<BufferOps<int64_t>>/4096                    888 ns          887 ns       785259
run_shrink_to_fit_bench<BufferOps<int64_t>>/16384                   942 ns          930 ns       761973
run_shrink_to_fit_bench<BufferOps<int64_t>>/65536                   921 ns          908 ns       744077
run_shrink_to_fit_bench<BufferOps<int64_t>>/262144                 1334 ns         1284 ns       567190
run_shrink_to_fit_bench<BufferOps<int64_t>>/1048576              117442 ns       117245 ns         5986
run_shrink_to_fit_bench<BufferOps<int64_t>>/4194304              568661 ns       568402 ns         1242
run_shrink_to_fit_bench<BufferOps<int64_t>>/16777216            3103867 ns      3100757 ns          222
run_shrink_to_fit_bench<BufferOps<int64_t>>/67108864           17130152 ns     17124298 ns           40
run_shrink_to_fit_bench<BufferOps<int64_t>>/134217728          30454094 ns     30383589 ns           23
run_resize_value_grow_bench<VectorOps<int64_t>>/16                  316 ns          319 ns      2195885
run_resize_value_grow_bench<VectorOps<int64_t>>/128                 321 ns          324 ns      2150237
run_resize_value_grow_bench<VectorOps<int64_t>>/512                 353 ns          356 ns      1988055
run_resize_value_grow_bench<VectorOps<int64_t>>/1024                405 ns          409 ns      1712741
run_resize_value_grow_bench<VectorOps<int64_t>>/4096               1122 ns         1138 ns       613237
run_resize_value_grow_bench<VectorOps<int64_t>>/16384              3631 ns         3638 ns       192140
run_resize_value_grow_bench<VectorOps<int64_t>>/65536             12634 ns        12638 ns        55172
run_resize_value_grow_bench<VectorOps<int64_t>>/262144           126778 ns       126949 ns         5507
run_resize_value_grow_bench<VectorOps<int64_t>>/1048576         2726288 ns      2724989 ns          258
run_resize_value_grow_bench<VectorOps<int64_t>>/4194304        15655112 ns     15645952 ns           46
run_resize_value_grow_bench<VectorOps<int64_t>>/16777216       74942154 ns     74823736 ns            9
run_resize_value_grow_bench<VectorOps<int64_t>>/67108864      278734439 ns    278610178 ns            3
run_resize_value_grow_bench<VectorOps<int64_t>>/134217728     543408900 ns    543339158 ns            1
run_resize_value_grow_bench<BufferOps<int64_t>>/16                  303 ns          306 ns      2279527
run_resize_value_grow_bench<BufferOps<int64_t>>/128                 317 ns          320 ns      2178988
run_resize_value_grow_bench<BufferOps<int64_t>>/512                 355 ns          358 ns      1947002
run_resize_value_grow_bench<BufferOps<int64_t>>/1024                437 ns          441 ns      1596807
run_resize_value_grow_bench<BufferOps<int64_t>>/4096               1180 ns         1195 ns       585646
run_resize_value_grow_bench<BufferOps<int64_t>>/16384              3959 ns         3970 ns       176382
run_resize_value_grow_bench<BufferOps<int64_t>>/65536              5320 ns         5318 ns       131515
run_resize_value_grow_bench<BufferOps<int64_t>>/262144            92305 ns        92331 ns         6558
run_resize_value_grow_bench<BufferOps<int64_t>>/1048576         2542995 ns      2542400 ns          261
run_resize_value_grow_bench<BufferOps<int64_t>>/4194304         7051240 ns      7045279 ns           96
run_resize_value_grow_bench<BufferOps<int64_t>>/16777216       36423193 ns     36399306 ns           19
run_resize_value_grow_bench<BufferOps<int64_t>>/67108864      142044351 ns    141983729 ns            5
run_resize_value_grow_bench<BufferOps<int64_t>>/134217728     558407394 ns    558246408 ns            1
run_assign_bench<VectorOps<int64_t>>/16                            37.3 ns         37.3 ns     18779519
run_assign_bench<VectorOps<int64_t>>/128                           47.1 ns         47.0 ns     14537733
run_assign_bench<VectorOps<int64_t>>/512                           80.0 ns         80.0 ns      8751036
run_assign_bench<VectorOps<int64_t>>/1024                           127 ns          127 ns      5503819
run_assign_bench<VectorOps<int64_t>>/4096                           727 ns          727 ns       964590
run_assign_bench<VectorOps<int64_t>>/16384                         2658 ns         2657 ns       263712
run_assign_bench<VectorOps<int64_t>>/65536                         9081 ns         9078 ns        77019
run_assign_bench<VectorOps<int64_t>>/262144                       85090 ns        85046 ns         8228
run_assign_bench<VectorOps<int64_t>>/1048576                    2559167 ns      2558273 ns          280
run_assign_bench<VectorOps<int64_t>>/4194304                   11934074 ns     11925126 ns           60
run_assign_bench<VectorOps<int64_t>>/16777216                  65684861 ns     65663880 ns           11
run_assign_bench<VectorOps<int64_t>>/67108864                 256768283 ns    256755982 ns            3
run_assign_bench<VectorOps<int64_t>>/134217728                503026305 ns    503000775 ns            1
run_assign_bench<BufferOps<int64_t>>/16                            34.8 ns         34.8 ns     20148793
run_assign_bench<BufferOps<int64_t>>/128                           47.4 ns         47.3 ns     14828284
run_assign_bench<BufferOps<int64_t>>/512                           78.0 ns         77.9 ns      8980933
run_assign_bench<BufferOps<int64_t>>/1024                           128 ns          128 ns      5470780
run_assign_bench<BufferOps<int64_t>>/4096                           732 ns          732 ns       956464
run_assign_bench<BufferOps<int64_t>>/16384                         2485 ns         2484 ns       281878
run_assign_bench<BufferOps<int64_t>>/65536                         9208 ns         9207 ns        75338
run_assign_bench<BufferOps<int64_t>>/262144                       82511 ns        82508 ns         8650
run_assign_bench<BufferOps<int64_t>>/1048576                    2503753 ns      2503110 ns          279
run_assign_bench<BufferOps<int64_t>>/4194304                   11725465 ns     11722988 ns           59
run_assign_bench<BufferOps<int64_t>>/16777216                  64949426 ns     64930510 ns           11
run_assign_bench<BufferOps<int64_t>>/67108864                 256389481 ns    256365159 ns            3
run_assign_bench<BufferOps<int64_t>>/134217728                503595093 ns    503506450 ns            1
run_assign_range_bench<VectorOps<int64_t>>/16                      33.6 ns         33.5 ns     20862786
run_assign_range_bench<VectorOps<int64_t>>/128                     42.4 ns         42.4 ns     16493022
run_assign_range_bench<VectorOps<int64_t>>/512                     77.7 ns         77.7 ns      9002531
run_assign_range_bench<VectorOps<int64_t>>/1024                     129 ns          129 ns      5443076
run_assign_range_bench<VectorOps<int64_t>>/4096                     865 ns          865 ns       801764
run_assign_range_bench<VectorOps<int64_t>>/16384                   3327 ns         3325 ns       210795
run_assign_range_bench<VectorOps<int64_t>>/65536                  30275 ns        30273 ns        23083
run_assign_range_bench<VectorOps<int64_t>>/262144                165718 ns       165709 ns         4236
run_assign_range_bench<VectorOps<int64_t>>/1048576              2785322 ns      2783985 ns          249
run_assign_range_bench<VectorOps<int64_t>>/4194304             14759414 ns     14757488 ns           47
run_assign_range_bench<VectorOps<int64_t>>/16777216            69511557 ns     69501957 ns           10
run_assign_range_bench<VectorOps<int64_t>>/67108864           267418799 ns    267320906 ns            3
run_assign_range_bench<VectorOps<int64_t>>/134217728          532946037 ns    532736235 ns            1
run_assign_range_bench<BufferOps<int64_t>>/16                      38.9 ns         38.9 ns     18020693
run_assign_range_bench<BufferOps<int64_t>>/128                     47.1 ns         47.1 ns     14908423
run_assign_range_bench<BufferOps<int64_t>>/512                     81.8 ns         81.8 ns      8559954
run_assign_range_bench<BufferOps<int64_t>>/1024                     133 ns          133 ns      5268692
run_assign_range_bench<BufferOps<int64_t>>/4096                     820 ns          820 ns       837832
run_assign_range_bench<BufferOps<int64_t>>/16384                   3522 ns         3521 ns       198224
run_assign_range_bench<BufferOps<int64_t>>/65536                  15745 ns        15741 ns        42923
run_assign_range_bench<BufferOps<int64_t>>/262144                165364 ns       165289 ns         4257
run_assign_range_bench<BufferOps<int64_t>>/1048576              2831966 ns      2830706 ns          241
run_assign_range_bench<BufferOps<int64_t>>/4194304             14867371 ns     14861270 ns           47
run_assign_range_bench<BufferOps<int64_t>>/16777216            69391874 ns     69361071 ns           10
run_assign_range_bench<BufferOps<int64_t>>/67108864           267109751 ns    266958393 ns            3
run_assign_range_bench<BufferOps<int64_t>>/134217728          519376839 ns    519230669 ns            1
run_assign_init_list_bench<BufferOps<int64_t>>/8                   37.6 ns         37.6 ns     18606864
run_assign_init_list_bench<VectorOps<int64_t>>/8                   30.8 ns         30.8 ns     22716941
run_push_back_no_reserve_bench<VectorOps<int64_t>>/16               176 ns          175 ns      3988789
run_push_back_no_reserve_bench<VectorOps<int64_t>>/128              349 ns          349 ns      2009570
run_push_back_no_reserve_bench<VectorOps<int64_t>>/512              714 ns          714 ns       980746
run_push_back_no_reserve_bench<VectorOps<int64_t>>/1024            1156 ns         1155 ns       605878
run_push_back_no_reserve_bench<VectorOps<int64_t>>/4096            4661 ns         4661 ns       150099
run_push_back_no_reserve_bench<VectorOps<int64_t>>/16384          18781 ns        18775 ns        36864
run_push_back_no_reserve_bench<VectorOps<int64_t>>/65536          76618 ns        76614 ns         9138
run_push_back_no_reserve_bench<VectorOps<int64_t>>/262144        376169 ns       376151 ns         1850
run_push_back_no_reserve_bench<VectorOps<int64_t>>/1048576      3847387 ns      3846851 ns          184
run_push_back_no_reserve_bench<VectorOps<int64_t>>/4194304     25061967 ns     25055116 ns           28
run_push_back_no_reserve_bench<VectorOps<int64_t>>/16777216   138278845 ns    138254233 ns            5
run_push_back_no_reserve_bench<VectorOps<int64_t>>/67108864   567973120 ns    567914349 ns            1
run_push_back_no_reserve_bench<VectorOps<int64_t>>/134217728 1128184112 ns   1127921251 ns            1
run_push_back_no_reserve_bench<BufferOps<int64_t>>/16               182 ns          182 ns      3847677
run_push_back_no_reserve_bench<BufferOps<int64_t>>/128              369 ns          369 ns      1899483
run_push_back_no_reserve_bench<BufferOps<int64_t>>/512              745 ns          745 ns       940095
run_push_back_no_reserve_bench<BufferOps<int64_t>>/1024            1237 ns         1236 ns       566562
run_push_back_no_reserve_bench<BufferOps<int64_t>>/4096            5022 ns         5022 ns       139384
run_push_back_no_reserve_bench<BufferOps<int64_t>>/16384          17724 ns        17720 ns        40042
run_push_back_no_reserve_bench<BufferOps<int64_t>>/65536          63287 ns        63269 ns        10829
run_push_back_no_reserve_bench<BufferOps<int64_t>>/262144        261684 ns       261493 ns         2780
run_push_back_no_reserve_bench<BufferOps<int64_t>>/1048576      3552840 ns      3552704 ns          192
run_push_back_no_reserve_bench<BufferOps<int64_t>>/4194304     18908510 ns     18907602 ns           39
run_push_back_no_reserve_bench<BufferOps<int64_t>>/16777216    95889252 ns     95830708 ns            8
run_push_back_no_reserve_bench<BufferOps<int64_t>>/67108864   389440971 ns    389143712 ns            2
run_push_back_no_reserve_bench<BufferOps<int64_t>>/134217728  961547224 ns    960279909 ns            1
run_push_back_bench<VectorOps<int64_t>>/16                          263 ns          266 ns      2579610
run_push_back_bench<VectorOps<int64_t>>/128                         336 ns          338 ns      2054410
run_push_back_bench<VectorOps<int64_t>>/512                         583 ns          586 ns      1191869
run_push_back_bench<VectorOps<int64_t>>/1024                        921 ns          923 ns       755865
run_push_back_bench<VectorOps<int64_t>>/4096                       3726 ns         3745 ns       182509
run_push_back_bench<VectorOps<int64_t>>/16384                     14292 ns        14314 ns        48773
run_push_back_bench<VectorOps<int64_t>>/65536                     55772 ns        55810 ns        12572
run_push_back_bench<VectorOps<int64_t>>/262144                   238899 ns       239101 ns         2940
run_push_back_bench<VectorOps<int64_t>>/1048576                 3062111 ns      3062237 ns          230
run_push_back_bench<VectorOps<int64_t>>/4194304                15131609 ns     15124114 ns           42
run_push_back_bench<VectorOps<int64_t>>/16777216               72921255 ns     72908371 ns           10
run_push_back_bench<VectorOps<int64_t>>/67108864              292150486 ns    292114286 ns            2
run_push_back_bench<VectorOps<int64_t>>/134217728             574065536 ns    573970044 ns            1
run_push_back_bench<BufferOps<int64_t>>/16                          261 ns          264 ns      2635696
run_push_back_bench<BufferOps<int64_t>>/128                         345 ns          347 ns      2023156
run_push_back_bench<BufferOps<int64_t>>/512                         586 ns          589 ns      1185618
run_push_back_bench<BufferOps<int64_t>>/1024                        924 ns          926 ns       756294
run_push_back_bench<BufferOps<int64_t>>/4096                       3715 ns         3732 ns       184976
run_push_back_bench<BufferOps<int64_t>>/16384                     14644 ns        14667 ns        47733
run_push_back_bench<BufferOps<int64_t>>/65536                     57005 ns        57024 ns        12275
run_push_back_bench<BufferOps<int64_t>>/262144                   242898 ns       243041 ns         2877
run_push_back_bench<BufferOps<int64_t>>/1048576                 3061003 ns      3060971 ns          231
run_push_back_bench<BufferOps<int64_t>>/4194304                14467523 ns     14461216 ns           43
run_push_back_bench<BufferOps<int64_t>>/16777216               70669482 ns     70667387 ns           10
run_push_back_bench<BufferOps<int64_t>>/67108864              302934320 ns    302530694 ns            2
run_push_back_bench<BufferOps<int64_t>>/134217728             589757133 ns    589458417 ns            1
run_single_grow_bench<VectorOps<int64_t>>/1024                      437 ns          441 ns      1595203
run_single_grow_bench<VectorOps<int64_t>>/4096                     1527 ns         1534 ns       453734
run_single_grow_bench<VectorOps<int64_t>>/16384                    4199 ns         4204 ns       175401
run_single_grow_bench<VectorOps<int64_t>>/65536                   18585 ns        18661 ns        37164
run_single_grow_bench<VectorOps<int64_t>>/262144                 166833 ns       166657 ns         4196
run_single_grow_bench<VectorOps<int64_t>>/1048576               3078137 ns      3077552 ns          227
run_single_grow_bench<VectorOps<int64_t>>/4194304              18088804 ns     18083535 ns           40
run_single_grow_bench<VectorOps<int64_t>>/16777216             80881831 ns     80859163 ns            8
run_single_grow_bench<BufferOps<int64_t>>/1024                      466 ns          469 ns      1473720
run_single_grow_bench<BufferOps<int64_t>>/4096                     1728 ns         1731 ns       401924
run_single_grow_bench<BufferOps<int64_t>>/16384                    4113 ns         4114 ns       170183
run_single_grow_bench<BufferOps<int64_t>>/65536                     813 ns          810 ns       717340
run_single_grow_bench<BufferOps<int64_t>>/262144                  14003 ns        13694 ns        62550
run_single_grow_bench<BufferOps<int64_t>>/1048576               3110613 ns      3110606 ns          224
run_single_grow_bench<BufferOps<int64_t>>/4194304              17943547 ns     17939136 ns           39
run_single_grow_bench<BufferOps<int64_t>>/16777216             81766033 ns     81751784 ns            8
run_append_reserve_bench<VectorOps<int64_t>>/16                     262 ns          264 ns      2655296
run_append_reserve_bench<VectorOps<int64_t>>/128                    292 ns          295 ns      2424853
run_append_reserve_bench<VectorOps<int64_t>>/512                    303 ns          306 ns      2344710
run_append_reserve_bench<VectorOps<int64_t>>/1024                   337 ns          339 ns      2038397
run_append_reserve_bench<VectorOps<int64_t>>/4096                   990 ns         1007 ns       699129
run_append_reserve_bench<VectorOps<int64_t>>/16384                 2782 ns         2797 ns       271982
run_append_reserve_bench<VectorOps<int64_t>>/65536                11773 ns        11793 ns        72789
run_append_reserve_bench<VectorOps<int64_t>>/262144               86426 ns        86528 ns         7456
run_append_reserve_bench<VectorOps<int64_t>>/1048576            3547228 ns      3545217 ns          208
run_append_reserve_bench<VectorOps<int64_t>>/4194304           14908509 ns     14908493 ns           53
run_append_reserve_bench<VectorOps<int64_t>>/16777216          62005657 ns     62000709 ns           11
run_append_reserve_bench<VectorOps<int64_t>>/67108864         290339681 ns    290273461 ns            2
run_append_reserve_bench<VectorOps<int64_t>>/134217728        576557785 ns    576167378 ns            1
run_append_reserve_bench<BufferOps<int64_t>>/16                     255 ns          257 ns      2415326
run_append_reserve_bench<BufferOps<int64_t>>/128                    261 ns          264 ns      2635139
run_append_reserve_bench<BufferOps<int64_t>>/512                    303 ns          305 ns      2387346
run_append_reserve_bench<BufferOps<int64_t>>/1024                   341 ns          344 ns      2031610
run_append_reserve_bench<BufferOps<int64_t>>/4096                   979 ns          997 ns       700725
run_append_reserve_bench<BufferOps<int64_t>>/16384                 2582 ns         2599 ns       268148
run_append_reserve_bench<BufferOps<int64_t>>/65536                 8997 ns         9018 ns        77643
run_append_reserve_bench<BufferOps<int64_t>>/262144               82819 ns        83006 ns         8326
run_append_reserve_bench<BufferOps<int64_t>>/1048576            2540152 ns      2540222 ns          282
run_append_reserve_bench<BufferOps<int64_t>>/4194304           14147142 ns     14138216 ns           50
run_append_reserve_bench<BufferOps<int64_t>>/16777216          65915772 ns     65908552 ns           11
run_append_reserve_bench<BufferOps<int64_t>>/67108864         266010001 ns    265924224 ns            3
run_append_reserve_bench<BufferOps<int64_t>>/134217728        525372762 ns    525353128 ns            1
run_append_range_reserve_bench<VectorOps<int64_t>>/16               267 ns          269 ns      2602970
run_append_range_reserve_bench<VectorOps<int64_t>>/128              270 ns          273 ns      2593554
run_append_range_reserve_bench<VectorOps<int64_t>>/512              304 ns          306 ns      2299001
run_append_range_reserve_bench<VectorOps<int64_t>>/1024             347 ns          349 ns      1992280
run_append_range_reserve_bench<VectorOps<int64_t>>/4096            1026 ns         1043 ns       670762
run_append_range_reserve_bench<VectorOps<int64_t>>/16384           3397 ns         3409 ns       205296
run_append_range_reserve_bench<VectorOps<int64_t>>/65536          20974 ns        21029 ns        33171
run_append_range_reserve_bench<VectorOps<int64_t>>/262144        165788 ns       165925 ns         4227
run_append_range_reserve_bench<VectorOps<int64_t>>/1048576      2757739 ns      2756118 ns          259
run_append_range_reserve_bench<VectorOps<int64_t>>/4194304     14558076 ns     14552747 ns           46
run_append_range_reserve_bench<VectorOps<int64_t>>/16777216    69946117 ns     69941585 ns           10
run_append_range_reserve_bench<VectorOps<int64_t>>/67108864   278008079 ns    277997760 ns            2
run_append_range_reserve_bench<VectorOps<int64_t>>/134217728  541964881 ns    541899123 ns            1
run_append_range_reserve_bench<BufferOps<int64_t>>/16               263 ns          265 ns      2665010
run_append_range_reserve_bench<BufferOps<int64_t>>/128              269 ns          271 ns      2608696
run_append_range_reserve_bench<BufferOps<int64_t>>/512              301 ns          304 ns      2293002
run_append_range_reserve_bench<BufferOps<int64_t>>/1024             348 ns          351 ns      1972700
run_append_range_reserve_bench<BufferOps<int64_t>>/4096            1080 ns         1095 ns       636094
run_append_range_reserve_bench<BufferOps<int64_t>>/16384           3393 ns         3409 ns       203572
run_append_range_reserve_bench<BufferOps<int64_t>>/65536          14349 ns        14362 ns        49362
run_append_range_reserve_bench<BufferOps<int64_t>>/262144        164785 ns       164941 ns         4240
run_append_range_reserve_bench<BufferOps<int64_t>>/1048576      2726600 ns      2726318 ns          253
run_append_range_reserve_bench<BufferOps<int64_t>>/4194304     15707127 ns     15703781 ns           47
run_append_range_reserve_bench<BufferOps<int64_t>>/16777216    72965820 ns     72963965 ns           10
run_append_range_reserve_bench<BufferOps<int64_t>>/67108864   277054254 ns    277027886 ns            3
run_append_range_reserve_bench<BufferOps<int64_t>>/134217728  531748893 ns    531254133 ns            1
run_append_init_list_bench<BufferOps<int64_t>>/8                   38.4 ns         38.3 ns     18196121
run_append_init_list_bench<VectorOps<int64_t>>/8                   39.2 ns         39.2 ns     17813523
*/