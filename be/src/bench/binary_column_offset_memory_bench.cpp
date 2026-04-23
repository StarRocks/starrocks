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

#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>

#include "column/binary_column.h"
#include "gutil/logging.h"

namespace starrocks::bench {
namespace {

static constexpr const char* kSuite = "BinaryColumnOffsetMemory";

enum class BuildMode {
    RESIZE_WRITE = 0,
    PUSH_BACK_LOOP = 1,
};

struct NoopStorageVisitor {
    template <typename Buffer>
    void operator()(Buffer&) const {}
};

template <typename T, typename = void>
struct has_value_type : std::false_type {};

template <typename T>
struct has_value_type<T, std::void_t<typename T::value_type>> : std::true_type {};

template <typename T, typename = void>
struct has_visit_storage : std::false_type {};

template <typename T>
struct has_visit_storage<T, std::void_t<decltype(std::declval<T&>().visit_storage(NoopStorageVisitor{}))>>
        : std::true_type {};

template <typename T, typename = void>
struct has_resize_uninitialized_with_max : std::false_type {};

template <typename T>
struct has_resize_uninitialized_with_max<T, std::void_t<decltype(std::declval<T&>().resize_uninitialized(
                                                    std::declval<size_t>(), std::declval<uint64_t>()))>>
        : std::true_type {};

template <typename T, typename = void>
struct has_element_size : std::false_type {};

template <typename T>
struct has_element_size<T, std::void_t<decltype(std::declval<const T&>().element_size())>> : std::true_type {};

template <typename T, typename = void>
struct has_is_large : std::false_type {};

template <typename T>
struct has_is_large<T, std::void_t<decltype(std::declval<const T&>().is_large())>> : std::true_type {};

template <typename T, typename = void>
struct has_memory_usage : std::false_type {};

template <typename T>
struct has_memory_usage<T, std::void_t<decltype(std::declval<const T&>().memory_usage())>> : std::true_type {};

static const char* build_mode_name(BuildMode mode) {
    switch (mode) {
    case BuildMode::RESIZE_WRITE:
        return "resize_write";
    case BuildMode::PUSH_BACK_LOOP:
        return "push_back_loop";
    }
    return "unknown";
}

template <typename Offsets>
static size_t active_element_size(const Offsets& offsets) {
    if constexpr (has_element_size<Offsets>::value) {
        return offsets.element_size();
    } else {
        static_assert(has_value_type<Offsets>::value, "Old offset buffer must expose value_type");
        return sizeof(typename Offsets::value_type);
    }
}

template <typename Offsets>
static bool offset_is_large(const Offsets& offsets) {
    if constexpr (has_is_large<Offsets>::value) {
        return offsets.is_large();
    } else {
        return false;
    }
}

template <typename Offsets>
static size_t offset_accounted_bytes(const Offsets& offsets) {
    if constexpr (has_memory_usage<Offsets>::value) {
        return offsets.memory_usage();
    } else {
        return offsets.capacity() * active_element_size(offsets);
    }
}

template <typename Offsets>
static void resize_for_write(Offsets& offsets, size_t count, uint64_t max_offset) {
    if constexpr (has_resize_uninitialized_with_max<Offsets>::value) {
        offsets.resize_uninitialized(count, max_offset);
    } else {
        offsets.resize(count);
    }
}

struct OffsetWriter {
    size_t count;
    uint64_t step;

    template <typename Buffer>
    void operator()(Buffer& buffer) const {
        using Value = typename std::decay_t<Buffer>::value_type;
        for (size_t i = 0; i < count; ++i) {
            buffer[i] = static_cast<Value>(i * step);
        }
    }
};

template <typename Offsets>
static void write_offsets(Offsets& offsets, size_t count, uint64_t step) {
    if constexpr (has_visit_storage<Offsets>::value) {
        offsets.visit_storage(OffsetWriter{count, step});
    } else {
        using Value = typename Offsets::value_type;
        for (size_t i = 0; i < count; ++i) {
            offsets[i] = static_cast<Value>(i * step);
        }
    }
}

template <typename Offsets>
static void append_offset(Offsets& offsets, uint64_t value) {
    if constexpr (has_value_type<Offsets>::value) {
        offsets.emplace_back(static_cast<typename Offsets::value_type>(value));
    } else {
        offsets.emplace_back(value);
    }
}

template <typename Offsets>
static Offsets build_offsets(size_t count, uint64_t step, BuildMode mode) {
    Offsets offsets;
    const uint64_t max_offset = count == 0 ? 0 : (count - 1) * step;

    if (mode == BuildMode::RESIZE_WRITE) {
        resize_for_write(offsets, count, max_offset);
        write_offsets(offsets, count, step);
    } else {
        offsets.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            append_offset(offsets, i * step);
        }
    }
    return offsets;
}

template <typename Offsets>
static void publish_memory_counters(benchmark::State& state, const Offsets& offsets) {
    const size_t size = offsets.size();
    const size_t capacity = offsets.capacity();
    const size_t element_size = active_element_size(offsets);
    const bool is_large = offset_is_large(offsets);

    const size_t logical_bytes = size * element_size;
    const size_t capacity_bytes = capacity * element_size;
    const size_t accounted_bytes = offset_accounted_bytes(offsets);
    CHECK_GE(accounted_bytes, capacity_bytes);
    const size_t inactive_bytes = accounted_bytes - capacity_bytes;
    const size_t object_size = sizeof(Offsets);
    const size_t estimated_instance_bytes = object_size + accounted_bytes;

    // These are diagnostic values derived from the current accounting contract:
    // accounted bytes = active buffer capacity bytes + inactive buffer capacity bytes.
    const size_t u32_capacity = is_large ? inactive_bytes / sizeof(uint32_t) : capacity;
    const size_t u64_capacity = is_large ? capacity : inactive_bytes / sizeof(uint64_t);

    state.counters["offset_size"] = static_cast<double>(size);
    state.counters["offset_capacity"] = static_cast<double>(capacity);
    state.counters["offset_element_size"] = static_cast<double>(element_size);
    state.counters["offset_logical_bytes"] = static_cast<double>(logical_bytes);
    state.counters["offset_capacity_bytes"] = static_cast<double>(capacity_bytes);
    state.counters["offset_accounted_bytes"] = static_cast<double>(accounted_bytes);
    state.counters["offset_object_size"] = static_cast<double>(object_size);
    state.counters["offset_estimated_instance_bytes"] = static_cast<double>(estimated_instance_bytes);
    state.counters["offset_is_large"] = is_large ? 1.0 : 0.0;

    // Diagnostic counters. Old Buffer<uint32_t> naturally maps to u32-only storage.
    state.counters["u32_capacity"] = static_cast<double>(u32_capacity);
    state.counters["u64_capacity"] = static_cast<double>(u64_capacity);
    state.counters["inactive_capacity_bytes"] = static_cast<double>(inactive_bytes);
}

static void bench_offset_memory(benchmark::State& state, size_t offset_count, uint64_t step, BuildMode mode) {
    using Offsets = BinaryColumn::Offsets;
    auto offsets = build_offsets<Offsets>(offset_count, step, mode);
    CHECK(!offset_is_large(offsets)) << "Main offset memory benchmark only covers no-promote cases";

    // This is a memory-counter benchmark. Ignore Time/CPU columns; the result is in counters.
    for (auto _ : state) {
        benchmark::DoNotOptimize(offsets.size());
        benchmark::DoNotOptimize(offsets.capacity());
        benchmark::ClobberMemory();
    }

    publish_memory_counters(state, offsets);
}

template <typename Fn>
static void register_case(const std::string& name, Fn&& fn) {
    benchmark::RegisterBenchmark(name.c_str(), std::forward<Fn>(fn))->Unit(benchmark::kMicrosecond)->Iterations(1);
}

static void register_offset_memory_cases() {
    for (size_t offset_count : {4097UL, 65537UL, 1048577UL}) {
        for (uint64_t step : {4UL, 128UL}) {
            for (BuildMode mode : {BuildMode::RESIZE_WRITE, BuildMode::PUSH_BACK_LOOP}) {
                const std::string name = std::string(kSuite) +
                                         "/offset_only_no_promote/offset_count:" + std::to_string(offset_count) +
                                         "/step:" + std::to_string(step) + "/build:" + build_mode_name(mode);
                register_case(name,
                              [=](benchmark::State& state) { bench_offset_memory(state, offset_count, step, mode); });
            }
        }
    }
}

const bool kRegistered = [] {
    register_offset_memory_cases();
    return true;
}();

} // namespace
} // namespace starrocks::bench

BENCHMARK_MAIN();
