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
#include <cstring>
#include <initializer_list>
#include <vector>

#include "runtime/current_thread.h"
#include "common/memory/column_allocator.h"
#include "runtime/memory/memory_allocator.h"
#include "column/buffer.h"

namespace starrocks {

namespace {

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

struct LargePod {
    std::array<int64_t, 16> payload{};
    LargePod() = default;
    explicit LargePod(int64_t seed) { payload.fill(seed); }
};

struct SmallNonPod {
    int64_t value{0};
    std::array<char, 16> payload{};
    SmallNonPod() = default;
    explicit SmallNonPod(int64_t id) : value(id) { payload.fill('x'); }
    ~SmallNonPod() = default;
};

template <typename T>
struct ValueFactory {
    static T make(int64_t value) { return static_cast<T>(value); }

    template <typename Container>
    static void emplace(Container& c, int64_t value) {
        c.emplace_back(static_cast<T>(value));
    }
};

template <>
struct ValueFactory<LargePod> {
    static LargePod make(int64_t value) { return LargePod(value); }

    template <typename Container>
    static void emplace(Container& c, int64_t value) {
        c.emplace_back(value);
    }
};

template <>
struct ValueFactory<SmallNonPod> {
    static SmallNonPod make(int64_t value) { return SmallNonPod(value); }

    template <typename Container>
    static void emplace(Container& c, int64_t value) {
        c.emplace_back(value);
    }
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

template <>
struct InitListFactory<LargePod> {
    static const std::initializer_list<LargePod>& list() {
        static const std::initializer_list<LargePod> values = {LargePod(0), LargePod(1), LargePod(2), LargePod(3),
                                                               LargePod(4), LargePod(5), LargePod(6), LargePod(7)};
        return values;
    }
};

template <>
struct InitListFactory<SmallNonPod> {
    static const std::initializer_list<SmallNonPod>& list() {
        static const std::initializer_list<SmallNonPod> values = {SmallNonPod(0), SmallNonPod(1), SmallNonPod(2),
                                                                  SmallNonPod(3), SmallNonPod(4), SmallNonPod(5),
                                                                  SmallNonPod(6), SmallNonPod(7)};
        return values;
    }
};

template <typename T>
struct BufferOps {
    using ValueType = T;
    using Container = Buffer<T>;

    static Container make() { return Container(memory::get_default_allocator()); }
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
    static void emplace_back(Container& c, int64_t i) { ValueFactory<T>::emplace(c, i); }
    static void insert(Container& c, int64_t n, const T& value) { c.append(static_cast<size_t>(n), value); }
    template <typename InputIt>
    static void insert_range(Container& c, InputIt first, InputIt last) {
        c.append(first, last);
    }
    static void insert_init_list(Container& c, std::initializer_list<T> list) { c.append(list); }
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
    static void emplace_back(Container& c, int64_t i) { ValueFactory<T>::emplace(c, i); }
    static void insert(Container& c, int64_t n, const T& value) { c.insert(c.end(), static_cast<size_t>(n), value); }
    template <typename InputIt>
    static void insert_range(Container& c, InputIt first, InputIt last) {
        c.insert(c.end(), first, last);
    }
    static void insert_init_list(Container& c, std::initializer_list<T> list) { c.insert(c.end(), list); }
};

static void apply_counts(benchmark::internal::Benchmark* b) {
    for (int64_t n :
         {16, 128, 512, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 134217728}) {
        b->Arg(n);
    }
}

static void apply_init_list(benchmark::internal::Benchmark* b) {
    b->Arg(kInitListSize);
}

template <typename Ops>
void run_reserve_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
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
    reset_arena_state();
    const int64_t n = state.range(0);
    for (auto _ : state) {
        state.PauseTiming();
        auto container = Ops::make();
        Ops::reserve(container, n);
        Ops::resize(container, n / 2);
        state.ResumeTiming();
        Ops::shrink_to_fit(container);
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_resize_grow_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    reset_arena_state();
    const int64_t n = state.range(0);
    const int64_t base = n / 2;
    for (auto _ : state) {
        state.PauseTiming();
        auto container = Ops::make();
        if (base > 0) {
            Ops::resize(container, base);
        }
        state.ResumeTiming();
        Ops::resize(container, n);
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_resize_grow_from_zero_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    reset_arena_state();
    const int64_t n = state.range(0);
    for (auto _ : state) {
        auto container = Ops::make();
        Ops::resize(container, n);
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_resize_shrink_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    reset_arena_state();
    const int64_t n = state.range(0);
    const int64_t target = n / 2;
    for (auto _ : state) {
        state.PauseTiming();
        auto container = Ops::make();
        Ops::resize(container, n);
        state.ResumeTiming();
        Ops::resize(container, target);
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_resize_value_grow_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    reset_arena_state();
    const int64_t n = state.range(0);
    const int64_t base = n / 2;
    const auto value = ValueFactory<typename Ops::ValueType>::make(1);
    for (auto _ : state) {
        state.PauseTiming();
        auto container = Ops::make();
        if (base > 0) {
            Ops::resize(container, base);
        }
        state.ResumeTiming();
        Ops::resize_value(container, n, value);
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_assign_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    reset_arena_state();
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
    reset_arena_state();
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
    reset_arena_state();
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
    reset_arena_state();
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
    reset_arena_state();
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
void run_emplace_back_no_reserve_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    reset_arena_state();
    const int64_t n = state.range(0);
    for (auto _ : state) {
        auto container = Ops::make();
        for (int64_t i = 0; i < n; ++i) {
            Ops::emplace_back(container, i);
        }
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_emplace_back_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    reset_arena_state();
    const int64_t n = state.range(0);
    for (auto _ : state) {
        state.PauseTiming();
        auto container = Ops::make();
        Ops::reserve(container, n);
        state.ResumeTiming();
        for (int64_t i = 0; i < n; ++i) {
            Ops::emplace_back(container, i);
        }
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_insert_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    reset_arena_state();
    const int64_t n = state.range(0);
    const auto value = ValueFactory<typename Ops::ValueType>::make(1);
    for (auto _ : state) {
        auto container = Ops::make();
        Ops::insert(container, n, value);
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_insert_reserve_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    reset_arena_state();
    const int64_t n = state.range(0);
    const auto value = ValueFactory<typename Ops::ValueType>::make(1);
    for (auto _ : state) {
        state.PauseTiming();
        auto container = Ops::make();
        Ops::reserve(container, n);
        state.ResumeTiming();
        Ops::insert(container, n, value);
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_insert_range_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    reset_arena_state();
    const int64_t n = state.range(0);
    std::vector<typename Ops::ValueType> values;
    values.reserve(static_cast<size_t>(n));
    for (int64_t i = 0; i < n; ++i) {
        values.push_back(ValueFactory<typename Ops::ValueType>::make(i));
    }
    const auto* data = values.data();
    for (auto _ : state) {
        auto container = Ops::make();
        Ops::insert_range(container, data, data + values.size());
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_insert_range_reserve_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    reset_arena_state();
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
        Ops::insert_range(container, data, data + values.size());
        benchmark::DoNotOptimize(container);
    }
}

template <typename Ops>
void run_insert_init_list_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    reset_arena_state();
    const auto& values = InitListFactory<typename Ops::ValueType>::list();
    for (auto _ : state) {
        auto container = Ops::make();
        Ops::insert_init_list(container, values);
        benchmark::DoNotOptimize(container);
    }
}

void run_buffer_assign_zero_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    reset_arena_state();
    const int64_t n = state.range(0);
    for (auto _ : state) {
        Buffer<uint8_t> buffer(memory::get_default_allocator());
        buffer.assign(static_cast<size_t>(n), 0);
        benchmark::DoNotOptimize(buffer);
    }
}

void run_buffer_resize_memset_zero_bench(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    reset_arena_state();
    const int64_t n = state.range(0);
    for (auto _ : state) {
        Buffer<uint8_t> buffer(memory::get_default_allocator());
        buffer.resize(static_cast<size_t>(n));
        if (n > 0) {
            std::memset(buffer.data(), 0, static_cast<size_t>(n));
        }
        benchmark::DoNotOptimize(buffer);
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
REGISTER_OP_BENCH(run_resize_grow_bench, int64_t);
REGISTER_OP_BENCH(run_resize_grow_from_zero_bench, int64_t);
REGISTER_OP_BENCH(run_resize_shrink_bench, int64_t);
REGISTER_OP_BENCH(run_resize_value_grow_bench, int64_t);
REGISTER_OP_BENCH(run_assign_bench, int64_t);
REGISTER_OP_BENCH(run_assign_range_bench, int64_t);
REGISTER_INIT_LIST_BENCH(run_assign_init_list_bench, int64_t);
REGISTER_OP_BENCH(run_push_back_no_reserve_bench, int64_t);
REGISTER_OP_BENCH(run_push_back_bench, int64_t);
REGISTER_OP_BENCH(run_emplace_back_no_reserve_bench, int64_t);
REGISTER_OP_BENCH(run_emplace_back_bench, int64_t);
REGISTER_OP_BENCH(run_insert_bench, int64_t);
REGISTER_OP_BENCH(run_insert_reserve_bench, int64_t);
REGISTER_OP_BENCH(run_insert_range_bench, int64_t);
REGISTER_OP_BENCH(run_insert_range_reserve_bench, int64_t);
REGISTER_INIT_LIST_BENCH(run_insert_init_list_bench, int64_t);

BENCHMARK(run_buffer_assign_zero_bench)->Apply(apply_counts);
BENCHMARK(run_buffer_resize_memset_zero_bench)->Apply(apply_counts);

#undef REGISTER_OP_BENCH
#undef REGISTER_INIT_LIST_BENCH

} // namespace

int main(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}

} // namespace starrocks
