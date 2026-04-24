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

#include <benchmark/benchmark.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "column/binary_column.h"

namespace starrocks::bench {

enum class LenPattern {
    FIXED,
    UNIFORM,
    TAIL_ZERO,
};

static constexpr size_t kMinStringLength = 4;

enum class IndexPattern {
    SEQ,
    REVERSE,
    RANDOM,
    RANDOM_10,
    DUPLICATE,
    CLUSTERED,
};

enum class FilterPattern {
    KEEP_ALL,
    KEEP_NONE,
    ALTERNATING,
    RANDOM_10,
    RANDOM_50,
};

enum class ReplicatePattern {
    ZERO,
    ONE,
    FIXED_2,
    RANDOM_0_4,
};

static const char* len_pattern_name(LenPattern pattern) {
    switch (pattern) {
    case LenPattern::FIXED:
        return "fixed";
    case LenPattern::UNIFORM:
        return "uniform";
    case LenPattern::TAIL_ZERO:
        return "tail_zero";
    }
    return "unknown";
}

static const char* index_pattern_name(IndexPattern pattern) {
    switch (pattern) {
    case IndexPattern::SEQ:
        return "seq";
    case IndexPattern::REVERSE:
        return "reverse";
    case IndexPattern::RANDOM:
        return "random";
    case IndexPattern::RANDOM_10:
        return "random10";
    case IndexPattern::DUPLICATE:
        return "duplicate";
    case IndexPattern::CLUSTERED:
        return "clustered";
    }
    return "unknown";
}

static const char* filter_pattern_name(FilterPattern pattern) {
    switch (pattern) {
    case FilterPattern::KEEP_ALL:
        return "keep_all";
    case FilterPattern::KEEP_NONE:
        return "keep_none";
    case FilterPattern::ALTERNATING:
        return "alternating";
    case FilterPattern::RANDOM_10:
        return "random10";
    case FilterPattern::RANDOM_50:
        return "random50";
    }
    return "unknown";
}

static const char* replicate_pattern_name(ReplicatePattern pattern) {
    switch (pattern) {
    case ReplicatePattern::ZERO:
        return "zero";
    case ReplicatePattern::ONE:
        return "one";
    case ReplicatePattern::FIXED_2:
        return "fixed2";
    case ReplicatePattern::RANDOM_0_4:
        return "random0_4";
    }
    return "unknown";
}

static size_t pseudo_random(size_t value) {
    value ^= value >> 33;
    value *= 0xff51afd7ed558ccdULL;
    value ^= value >> 33;
    value *= 0xc4ceb9fe1a85ec53ULL;
    value ^= value >> 33;
    return value;
}

static size_t generated_length(size_t row, size_t max_len, LenPattern pattern) {
    CHECK_GE(max_len, kMinStringLength);
    switch (pattern) {
    case LenPattern::FIXED:
    case LenPattern::TAIL_ZERO:
        return max_len;
    case LenPattern::UNIFORM:
        return kMinStringLength + pseudo_random(row) % (max_len - kMinStringLength + 1);
    }
    return max_len;
}

struct SliceBlock {
    std::vector<std::string> backing;
    std::vector<Slice> slices;
    size_t total_bytes = 0;
};

static void fill_string(std::string* str, size_t logical_size, size_t row, bool tail_zero) {
    for (size_t i = 0; i < logical_size; ++i) {
        (*str)[i] = static_cast<char>('a' + ((row + i) % 26));
    }
    if (tail_zero && logical_size > 0) {
        (*str)[logical_size - 1] = '\0';
    }
}

static SliceBlock make_slice_block(size_t rows, size_t max_len, LenPattern pattern, size_t min_backing_len = 0) {
    SliceBlock block;
    block.backing.reserve(rows);
    block.slices.reserve(rows);

    for (size_t i = 0; i < rows; ++i) {
        const size_t len = generated_length(i, max_len, pattern);
        const size_t backing_len = std::max(len, min_backing_len);
        block.backing.emplace_back(backing_len, '\0');
        fill_string(&block.backing.back(), len, i, pattern == LenPattern::TAIL_ZERO);
        block.slices.emplace_back(block.backing.back().data(), len);
        block.total_bytes += len;
    }
    return block;
}

struct ContinuousBlock {
    std::string bytes;
    std::vector<Slice> slices;
    size_t total_bytes = 0;
};

static ContinuousBlock make_continuous_block(size_t rows, size_t max_len, LenPattern pattern) {
    std::vector<size_t> lengths(rows);
    size_t total_bytes = 0;
    for (size_t i = 0; i < rows; ++i) {
        lengths[i] = generated_length(i, max_len, pattern);
        total_bytes += lengths[i];
    }

    ContinuousBlock block;
    block.bytes.resize(total_bytes);
    block.slices.reserve(rows);
    block.total_bytes = total_bytes;

    size_t offset = 0;
    for (size_t i = 0; i < rows; ++i) {
        const size_t len = lengths[i];
        for (size_t j = 0; j < len; ++j) {
            block.bytes[offset + j] = static_cast<char>('a' + ((i + j) % 26));
        }
        if (pattern == LenPattern::TAIL_ZERO && len > 0) {
            block.bytes[offset + len - 1] = '\0';
        }
        block.slices.emplace_back(block.bytes.data() + offset, len);
        offset += len;
    }
    return block;
}

static size_t fixed_copy_len(size_t max_len) {
    if (max_len <= 8) {
        return 8;
    }
    if (max_len <= 16) {
        return 16;
    }
    if (max_len <= 32) {
        return 32;
    }
    if (max_len <= 64) {
        return 64;
    }
    if (max_len <= 128) {
        return 128;
    }
    return max_len;
}

static BinaryColumn::MutablePtr make_column(size_t rows, size_t max_len, LenPattern pattern) {
    auto block = make_slice_block(rows, max_len, pattern);
    auto column = BinaryColumn::create();
    column->append_strings(block.slices.data(), block.slices.size());
    return column;
}

static BinaryColumn::MutablePtr make_fixed_column(size_t rows, size_t len) {
    return make_column(rows, len, LenPattern::FIXED);
}

static std::vector<uint32_t> make_indexes(size_t src_rows, size_t selected, IndexPattern pattern, uint32_t from = 0) {
    std::vector<uint32_t> indexes(from + selected, 0);
    for (size_t i = 0; i < selected; ++i) {
        size_t idx = 0;
        switch (pattern) {
        case IndexPattern::SEQ:
            idx = i;
            break;
        case IndexPattern::REVERSE:
            idx = src_rows - 1 - (i % src_rows);
            break;
        case IndexPattern::RANDOM:
            idx = pseudo_random(i) % src_rows;
            break;
        case IndexPattern::RANDOM_10:
            idx = pseudo_random(i) % std::max<size_t>(1, src_rows / 10);
            break;
        case IndexPattern::DUPLICATE:
            idx = (i / 8) % src_rows;
            break;
        case IndexPattern::CLUSTERED: {
            const size_t cluster = i / 64;
            idx = (cluster * 128 + i % 64) % src_rows;
            break;
        }
        }
        indexes[from + i] = static_cast<uint32_t>(idx);
    }
    return indexes;
}

static std::vector<uint32_t> make_update_indexes(size_t rows, size_t replace_rows) {
    std::vector<uint32_t> indexes(replace_rows);
    const size_t step = std::max<size_t>(1, rows / replace_rows);
    for (size_t i = 0; i < replace_rows; ++i) {
        indexes[i] = static_cast<uint32_t>(std::min(rows - 1, i * step));
    }
    return indexes;
}

static Filter make_filter(size_t rows, FilterPattern pattern) {
    Filter filter(rows);
    for (size_t i = 0; i < rows; ++i) {
        uint8_t keep = 0;
        switch (pattern) {
        case FilterPattern::KEEP_ALL:
            keep = 1;
            break;
        case FilterPattern::KEEP_NONE:
            keep = 0;
            break;
        case FilterPattern::ALTERNATING:
            keep = static_cast<uint8_t>((i & 1) == 0);
            break;
        case FilterPattern::RANDOM_10:
            keep = static_cast<uint8_t>(pseudo_random(i) % 10 == 0);
            break;
        case FilterPattern::RANDOM_50:
            keep = static_cast<uint8_t>(pseudo_random(i) & 1);
            break;
        }
        filter[i] = keep;
    }
    return filter;
}

static Buffer<uint32_t> make_replicate_offsets(size_t rows, ReplicatePattern pattern) {
    Buffer<uint32_t> offsets;
    offsets.resize(rows + 1);
    offsets[0] = 0;
    for (size_t i = 0; i < rows; ++i) {
        uint32_t repeat = 0;
        switch (pattern) {
        case ReplicatePattern::ZERO:
            repeat = 0;
            break;
        case ReplicatePattern::ONE:
            repeat = 1;
            break;
        case ReplicatePattern::FIXED_2:
            repeat = 2;
            break;
        case ReplicatePattern::RANDOM_0_4:
            repeat = static_cast<uint32_t>(pseudo_random(i) % 5);
            break;
        }
        offsets[i + 1] = offsets[i] + repeat;
    }
    return offsets;
}

static std::vector<std::string> make_serialized_rows(size_t rows, size_t len, bool nullable, FilterPattern nulls) {
    std::vector<std::string> encoded;
    encoded.reserve(rows);
    Filter null_filter;
    if (nullable) {
        null_filter = make_filter(rows, nulls);
    }
    for (size_t i = 0; i < rows; ++i) {
        const bool is_null = nullable && null_filter[i] != 0;
        std::string row;
        if (nullable) {
            row.append(reinterpret_cast<const char*>(&is_null), sizeof(bool));
        }
        if (!is_null) {
            const auto string_size = static_cast<uint32_t>(len);
            row.append(reinterpret_cast<const char*>(&string_size), sizeof(uint32_t));
            const size_t old_size = row.size();
            row.resize(old_size + len);
            for (size_t j = 0; j < len; ++j) {
                row[old_size + j] = static_cast<char>('a' + ((i + j) % 26));
            }
        }
        encoded.emplace_back(std::move(row));
    }
    return encoded;
}

static Buffer<Slice> make_src_slices_from_serialized(const std::vector<std::string>& encoded) {
    Buffer<Slice> srcs;
    srcs.resize(encoded.size());
    for (size_t i = 0; i < encoded.size(); ++i) {
        srcs[i] = Slice(const_cast<char*>(encoded[i].data()), encoded[i].size());
    }
    return srcs;
}

static int64_t fixed_benchmark_iterations() {
    // Keep the historical fixed-iteration mode by default. Set this env var to
    // 0 to let Google Benchmark use --benchmark_min_time for tail-sensitive runs.
    static const int64_t fixed_iterations = [] {
        const char* env = std::getenv("STARROCKS_BINARY_COLUMN_BENCH_ITERATIONS");
        if (env == nullptr || env[0] == '\0') {
            return int64_t{128};
        }

        char* end = nullptr;
        const auto value = std::strtoll(env, &end, 10);
        if (end == env || *end != '\0' || value < 0) {
            LOG(WARNING) << "Invalid STARROCKS_BINARY_COLUMN_BENCH_ITERATIONS=" << env << ", use 128";
            return int64_t{128};
        }
        return static_cast<int64_t>(value);
    }();
    return fixed_iterations;
}

template <typename Fn>
static void register_case(const std::string& name, Fn&& fn) {
    auto* registered = benchmark::RegisterBenchmark(name.c_str(), std::forward<Fn>(fn))->Unit(benchmark::kMicrosecond);

    const int64_t fixed_iterations = fixed_benchmark_iterations();
    if (fixed_iterations > 0) {
        registered->Iterations(fixed_iterations);
    }
}

static std::string case_name(const std::string& suite, const std::string& op, const std::string& params) {
    return suite + "/" + op + "/" + params;
}

static std::string rows_len_params(size_t rows, size_t len, LenPattern pattern) {
    return "rows:" + std::to_string(rows) + "/len:" + std::to_string(len) + "/dist:" + len_pattern_name(pattern);
}

static void bench_append_default(benchmark::State& state, size_t rows) {
    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = BinaryColumn::create();
            state.ResumeTiming();

            dst->append_default(rows);
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
}

static void bench_append_strings(benchmark::State& state, size_t rows, size_t len, LenPattern pattern) {
    auto block = make_slice_block(rows, len, pattern);

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = BinaryColumn::create();
            state.ResumeTiming();

            dst->append_strings(block.slices.data(), block.slices.size());
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * block.total_bytes));
}

static void bench_append_slice_loop(benchmark::State& state, size_t rows, size_t len) {
    auto block = make_slice_block(rows, len, LenPattern::FIXED);

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = BinaryColumn::create();
            state.ResumeTiming();

            for (const auto& slice : block.slices) {
                dst->append(slice);
            }
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * block.total_bytes));
}

static void bench_append_string_loop(benchmark::State& state, size_t rows, size_t len) {
    auto block = make_slice_block(rows, len, LenPattern::FIXED);

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = BinaryColumn::create();
            state.ResumeTiming();

            for (const auto& str : block.backing) {
                dst->append_string(str);
            }
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * block.total_bytes));
}

static void bench_append_strings_overflow(benchmark::State& state, size_t rows, size_t max_len) {
    auto block = make_slice_block(rows, max_len, LenPattern::UNIFORM, fixed_copy_len(max_len));

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = BinaryColumn::create();
            state.ResumeTiming();

            dst->append_strings_overflow(block.slices.data(), block.slices.size(), max_len);
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * block.total_bytes));
}

static void bench_append_continuous_strings(benchmark::State& state, size_t rows, size_t len, LenPattern pattern) {
    auto block = make_continuous_block(rows, len, pattern);

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = BinaryColumn::create();
            state.ResumeTiming();

            dst->append_continuous_strings(block.slices.data(), block.slices.size());
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * block.total_bytes));
}

static void bench_append_continuous_fixed_length_strings(benchmark::State& state, size_t rows, size_t len) {
    std::string bytes(rows * len, '\0');
    for (size_t i = 0; i < bytes.size(); ++i) {
        bytes[i] = static_cast<char>('a' + (i % 26));
    }

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = BinaryColumn::create();
            state.ResumeTiming();

            dst->append_continuous_fixed_length_strings(bytes.data(), rows, static_cast<int>(len));
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * bytes.size()));
}

static void bench_append_range(benchmark::State& state, size_t src_rows, size_t len, size_t offset, size_t count,
                               bool prefilled_dst) {
    auto src = make_fixed_column(src_rows, len);

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = BinaryColumn::create();
            if (prefilled_dst) {
                auto prefix = make_fixed_column(1024, len);
                dst->append(*prefix, 0, prefix->size());
            }
            state.ResumeTiming();

            dst->append(*src, offset, count);
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * count));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * count * len));
}

static void bench_append_selective(benchmark::State& state, size_t src_rows, size_t len, size_t selected,
                                   IndexPattern pattern, uint32_t from, bool prefilled_dst) {
    auto src = make_fixed_column(src_rows, len);
    auto indexes = make_indexes(src_rows, selected, pattern, from);

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = BinaryColumn::create();
            if (prefilled_dst) {
                auto prefix = make_fixed_column(1024, len);
                dst->append(*prefix, 0, prefix->size());
            }
            state.ResumeTiming();

            dst->append_selective(*src, indexes.data(), from, static_cast<uint32_t>(selected));
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * selected));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * selected * len));
}

static void bench_append_value_multiple_times_column(benchmark::State& state, size_t len, size_t count) {
    auto src = make_fixed_column(4, len);
    const uint32_t index = len == 0 ? 0 : 2;

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = BinaryColumn::create();
            state.ResumeTiming();

            dst->append_value_multiple_times(*src, index, static_cast<uint32_t>(count));
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * count));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * count * len));
}

static void bench_append_value_multiple_times_value(benchmark::State& state, size_t len, size_t count) {
    std::string value(len, 'x');
    Slice slice(value.data(), value.size());

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = BinaryColumn::create();
            state.ResumeTiming();

            dst->append_value_multiple_times(&slice, count);
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * count));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * count * len));
}

static void bench_replicate(benchmark::State& state, size_t rows, size_t len, ReplicatePattern pattern) {
    auto src = make_fixed_column(rows, len);
    auto offsets = make_replicate_offsets(rows, pattern);
    const size_t output_rows = offsets.back();

    for (auto _ : state) {
        {
            auto result_or = src->replicate(offsets);
            CHECK(result_or.ok()) << result_or.status();
            auto result = std::move(result_or).value();
            benchmark::DoNotOptimize(result.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * std::max(output_rows, rows)));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * output_rows * len));
}

static void bench_filter_range(benchmark::State& state, size_t rows, size_t len, FilterPattern pattern, bool partial) {
    auto filter = make_filter(rows, pattern);
    const size_t from = partial ? rows / 8 : 0;
    const size_t to = partial ? rows - rows / 8 : rows;

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = make_fixed_column(rows, len);
            state.ResumeTiming();

            const size_t result_size = dst->filter_range(filter, from, to);
            benchmark::DoNotOptimize(result_size);
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * (to - from)));
}

static void bench_cut(benchmark::State& state, size_t rows, size_t len, size_t start, size_t count) {
    auto src = make_fixed_column(rows, len);

    for (auto _ : state) {
        {
            auto result = src->cut(start, count);
            benchmark::DoNotOptimize(result.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * count));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * count * len));
}

static void bench_remove_first_n_values(benchmark::State& state, size_t rows, size_t len, size_t count) {
    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = make_fixed_column(rows, len);
            state.ResumeTiming();

            dst->remove_first_n_values(count);
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * count));
}

static void bench_assign(benchmark::State& state, size_t rows, size_t len, size_t idx, size_t count) {
    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = make_fixed_column(rows, len);
            state.ResumeTiming();

            dst->assign(count, idx);
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * count));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * count * len));
}

static void bench_update_rows(benchmark::State& state, size_t rows, size_t len, size_t replace_rows, bool same_len) {
    auto indexes = make_update_indexes(rows, replace_rows);
    auto replacement = make_fixed_column(replace_rows, same_len ? len : len + 1);

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = make_fixed_column(rows, len);
            state.ResumeTiming();

            dst->update_rows(*replacement, indexes.data());
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * (same_len ? replace_rows : rows)));
}

static void bench_fill_default(benchmark::State& state, size_t rows, size_t len, FilterPattern pattern) {
    auto filter = make_filter(rows, pattern);

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = make_fixed_column(rows, len);
            state.ResumeTiming();

            dst->fill_default(filter);
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
}

static void bench_build_slices(benchmark::State& state, size_t rows, size_t len) {
    auto src = make_fixed_column(rows, len);
    BinaryColumn::Container slices;
    slices.reserve(rows);

    for (auto _ : state) {
        slices.clear();
        src->build_slices(slices);
        benchmark::DoNotOptimize(slices.data());
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
}

static void bench_get_slice_scan(benchmark::State& state, size_t rows, size_t len, IndexPattern pattern) {
    auto src = make_fixed_column(rows, len);
    auto indexes = make_indexes(rows, rows, pattern);

    for (auto _ : state) {
        size_t total = 0;
        for (size_t i = 0; i < rows; ++i) {
            total += src->get_slice(indexes[i]).size;
        }
        benchmark::DoNotOptimize(total);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
}

static void bench_raw_data(benchmark::State& state, size_t rows, size_t len, bool warm_cache) {
    auto src = make_fixed_column(rows, len);
    if (warm_cache) {
        benchmark::DoNotOptimize(src->raw_data());
    }

    for (auto _ : state) {
        if (warm_cache) {
            benchmark::DoNotOptimize(src->raw_data());
        } else {
            state.PauseTiming();
            {
                auto cold = make_fixed_column(rows, len);
                state.ResumeTiming();
                benchmark::DoNotOptimize(cold->raw_data());
                benchmark::ClobberMemory();
                state.PauseTiming();
            }
            state.ResumeTiming();
        }
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
}

static void bench_max_one_element_serialize_size(benchmark::State& state, size_t rows, size_t len, LenPattern pattern) {
    auto src = make_column(rows, len, pattern);

    for (auto _ : state) {
        benchmark::DoNotOptimize(src->max_one_element_serialize_size());
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
}

static void bench_xor_checksum(benchmark::State& state, size_t rows, size_t len) {
    auto src = make_fixed_column(rows, len);

    for (auto _ : state) {
        benchmark::DoNotOptimize(src->xor_checksum(0, static_cast<uint32_t>(rows)));
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * rows * len));
}

static void bench_serialize_batch(benchmark::State& state, size_t rows, size_t len, bool nullable,
                                  FilterPattern null_pattern) {
    auto src = make_fixed_column(rows, len);
    const size_t max_row_size = sizeof(bool) + sizeof(uint32_t) + len;
    std::vector<uint8_t> null_masks;
    bool has_null = false;
    if (nullable) {
        auto filter = make_filter(rows, null_pattern);
        null_masks.assign(filter.begin(), filter.end());
        has_null = std::any_of(null_masks.begin(), null_masks.end(), [](uint8_t v) { return v != 0; });
    }

    for (auto _ : state) {
        state.PauseTiming();
        {
            std::vector<uint8_t> dst(rows * max_row_size);
            Buffer<uint32_t> slice_sizes;
            slice_sizes.resize(rows);
            std::fill(slice_sizes.begin(), slice_sizes.end(), 0);
            state.ResumeTiming();

            if (nullable) {
                src->serialize_batch_with_null_masks(dst.data(), slice_sizes, rows, static_cast<uint32_t>(max_row_size),
                                                     null_masks.data(), has_null);
            } else {
                src->serialize_batch(dst.data(), slice_sizes, rows, static_cast<uint32_t>(max_row_size));
            }
            benchmark::DoNotOptimize(dst.data());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
}

static void bench_serialize_batch_at_interval(benchmark::State& state, size_t rows, size_t len, bool reject_by_size,
                                              bool nullable, LenPattern pattern) {
    auto src = make_column(rows, len, pattern);
    std::vector<uint8_t> null_masks(rows, 0);
    if (nullable) {
        for (size_t i = 0; i < rows; i += 10) {
            null_masks[i] = 1;
        }
    }
    const auto max_row_size = static_cast<uint32_t>(reject_by_size && len > 0 ? len - 1 : len);
    const size_t byte_offset = nullable ? 1 : 0;
    const size_t byte_interval = byte_offset + len + 1;

    for (auto _ : state) {
        state.PauseTiming();
        {
            std::vector<uint8_t> dst(byte_interval * rows + byte_offset + 1);
            state.ResumeTiming();

            if (nullable) {
                benchmark::DoNotOptimize(src->serialize_batch_at_interval_with_null_masks(
                        dst.data(), byte_offset, byte_interval, max_row_size, 0, rows, null_masks.data()));
            } else {
                benchmark::DoNotOptimize(src->serialize_batch_at_interval(dst.data(), byte_offset, byte_interval,
                                                                          max_row_size, 0, rows));
            }
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
}

static void bench_deserialize_batch(benchmark::State& state, size_t rows, size_t len, bool nullable,
                                    FilterPattern null_pattern) {
    auto encoded = make_serialized_rows(rows, len, nullable, null_pattern);

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto srcs = make_src_slices_from_serialized(encoded);
            auto dst = BinaryColumn::create();
            Buffer<uint8_t> is_nulls;
            bool has_null = false;
            state.ResumeTiming();

            if (nullable) {
                dst->deserialize_and_append_batch_nullable(srcs, rows, is_nulls, has_null);
                benchmark::DoNotOptimize(has_null);
                benchmark::DoNotOptimize(is_nulls.data());
            } else {
                dst->deserialize_and_append_batch(srcs, rows);
            }
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
}

static void bench_serialize_scalar_scan(benchmark::State& state, size_t rows, size_t len) {
    auto src = make_fixed_column(rows, len);
    std::vector<uint8_t> dst(rows * (sizeof(uint32_t) + len));

    for (auto _ : state) {
        for (size_t i = 0; i < rows; ++i) {
            src->serialize(i, dst.data() + i * (sizeof(uint32_t) + len));
        }
        benchmark::DoNotOptimize(dst.data());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * rows * len));
}

static void bench_serialize_size_scan(benchmark::State& state, size_t rows, size_t len) {
    auto src = make_fixed_column(rows, len);

    for (auto _ : state) {
        uint64_t total_size = 0;
        for (size_t i = 0; i < rows; ++i) {
            total_size += src->serialize_size(i);
        }
        benchmark::DoNotOptimize(total_size);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
}

static void bench_deserialize_scalar_loop(benchmark::State& state, size_t rows, size_t len) {
    auto encoded = make_serialized_rows(rows, len, false, FilterPattern::KEEP_NONE);

    for (auto _ : state) {
        state.PauseTiming();
        {
            auto dst = BinaryColumn::create();
            state.ResumeTiming();

            for (const auto& row : encoded) {
                dst->deserialize_and_append(reinterpret_cast<const uint8_t*>(row.data()));
            }
            benchmark::DoNotOptimize(dst.get());
            benchmark::ClobberMemory();
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * rows * len));
}

static void bench_german_strings(benchmark::State& state, size_t rows, size_t len, bool warm_cache) {
    auto src = make_fixed_column(rows, len);
    if (warm_cache) {
        benchmark::DoNotOptimize(src->get_german_strings().data());
    }

    for (auto _ : state) {
        if (warm_cache) {
            benchmark::DoNotOptimize(src->get_german_strings().data());
        } else {
            state.PauseTiming();
            {
                auto cold = make_fixed_column(rows, len);
                state.ResumeTiming();
                benchmark::DoNotOptimize(cold->get_german_strings().data());
                benchmark::ClobberMemory();
                state.PauseTiming();
            }
            state.ResumeTiming();
        }
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
}

static void bench_byte_size_scan(benchmark::State& state, size_t rows, size_t len) {
    auto src = make_fixed_column(rows, len);

    for (auto _ : state) {
        uint64_t total_size = 0;
        for (size_t i = 0; i < rows; ++i) {
            total_size += src->byte_size(i);
        }
        total_size += src->byte_size(0, rows);
        benchmark::DoNotOptimize(total_size);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
}

static void register_binary_column_regression_full() {
    const std::string suite = "BinaryColumnRegressionFull";

    for (size_t rows : {4096UL, 65536UL, 262144UL}) {
        for (size_t len : {kMinStringLength, 8UL, 32UL, 128UL}) {
            for (LenPattern pattern : {LenPattern::FIXED, LenPattern::UNIFORM}) {
                register_case(case_name(suite, "append_strings_Slice_array", rows_len_params(rows, len, pattern)),
                              [rows, len, pattern](benchmark::State& state) {
                                  bench_append_strings(state, rows, len, pattern);
                              });
            }
        }
    }
    for (size_t len : {kMinStringLength, 8UL, 32UL, 128UL}) {
        register_case(case_name(suite, "append_Slice_loop", "rows:65536/len:" + std::to_string(len)),
                      [len](benchmark::State& state) { bench_append_slice_loop(state, 65536, len); });
        register_case(case_name(suite, "append_string_loop", "rows:65536/len:" + std::to_string(len)),
                      [len](benchmark::State& state) { bench_append_string_loop(state, 65536, len); });
    }

    for (size_t rows : {4096UL, 65536UL}) {
        for (size_t max_len : {8UL, 9UL, 16UL, 17UL, 32UL, 33UL, 64UL, 65UL, 128UL, 129UL}) {
            register_case(
                    case_name(suite, "append_strings_overflow",
                              "rows:" + std::to_string(rows) + "/max_len:" + std::to_string(max_len)),
                    [rows, max_len](benchmark::State& state) { bench_append_strings_overflow(state, rows, max_len); });
        }
    }
    for (size_t rows : {65536UL, 262144UL}) {
        for (size_t len : {kMinStringLength, 8UL, 32UL, 128UL}) {
            for (LenPattern pattern : {LenPattern::FIXED, LenPattern::UNIFORM}) {
                register_case(case_name(suite, "append_continuous_strings", rows_len_params(rows, len, pattern)),
                              [rows, len, pattern](benchmark::State& state) {
                                  bench_append_continuous_strings(state, rows, len, pattern);
                              });
            }
        }
    }

    for (size_t rows : {65536UL, 65539UL}) {
        for (size_t len : {8UL, 32UL, 128UL}) {
            register_case(case_name(suite, "append_continuous_fixed_length_strings",
                                    "rows:" + std::to_string(rows) + "/len:" + std::to_string(len)),
                          [rows, len](benchmark::State& state) {
                              bench_append_continuous_fixed_length_strings(state, rows, len);
                          });
        }
    }

    for (size_t len : {8UL, 128UL}) {
        for (bool prefilled : {false, true}) {
            register_case(case_name(suite, "append_Column_range",
                                    "src_rows:262144/len:" + std::to_string(len) +
                                            "/offset:7/count:65536/dst:" + (prefilled ? "prefilled" : "empty")),
                          [len, prefilled](benchmark::State& state) {
                              bench_append_range(state, 262144, len, 7, 65536, prefilled);
                          });
        }
        register_case(case_name(suite, "append_Column_range",
                                "src_rows:262144/len:" + std::to_string(len) + "/offset:0/count:65536/dst:empty"),
                      [len](benchmark::State& state) { bench_append_range(state, 262144, len, 0, 65536, false); });
    }
    register_case(case_name(suite, "append_Column_range", "src_rows:262144/len:4/offset:0/count:65536/dst:empty"),
                  [](benchmark::State& state) { bench_append_range(state, 262144, 4, 0, 65536, false); });
    register_case(case_name(suite, "append_Column_range", "src_rows:262144/len:4/offset:7/count:65536/dst:prefilled"),
                  [](benchmark::State& state) { bench_append_range(state, 262144, 4, 7, 65536, true); });

    for (auto [src_rows, len, selected] : {std::tuple<size_t, size_t, size_t>{65536, 32, 32768},
                                           std::tuple<size_t, size_t, size_t>{300000, 128, 65536}}) {
        for (IndexPattern pattern : {IndexPattern::SEQ, IndexPattern::RANDOM, IndexPattern::RANDOM_10,
                                     IndexPattern::REVERSE, IndexPattern::DUPLICATE, IndexPattern::CLUSTERED}) {
            register_case(case_name(suite, "append_selective",
                                    "src_rows:" + std::to_string(src_rows) + "/len:" + std::to_string(len) +
                                            "/selected:" + std::to_string(selected) +
                                            "/from:0/index:" + index_pattern_name(pattern)),
                          [src_rows, len, selected, pattern](benchmark::State& state) {
                              bench_append_selective(state, src_rows, len, selected, pattern, 0, false);
                          });
        }
    }
    register_case(case_name(suite, "append_selective",
                            "src_rows:65536/len:32/selected:32768/from:7/index:random/dst:prefilled"),
                  [](benchmark::State& state) {
                      bench_append_selective(state, 65536, 32, 32768, IndexPattern::RANDOM, 7, true);
                  });
    register_case(case_name(suite, "append_selective", "src_rows:65536/len:4/selected:32768/from:0/index:random"),
                  [](benchmark::State& state) {
                      bench_append_selective(state, 65536, 4, 32768, IndexPattern::RANDOM, 0, false);
                  });

    for (size_t len : {8UL, 128UL}) {
        for (size_t count : {4096UL, 65536UL}) {
            register_case(case_name(suite, "append_value_multiple_times_Column",
                                    "count:" + std::to_string(count) + "/len:" + std::to_string(len)),
                          [len, count](benchmark::State& state) {
                              bench_append_value_multiple_times_column(state, len, count);
                          });
            register_case(case_name(suite, "append_value_multiple_times_void_ptr",
                                    "count:" + std::to_string(count) + "/len:" + std::to_string(len)),
                          [len, count](benchmark::State& state) {
                              bench_append_value_multiple_times_value(state, len, count);
                          });
        }
    }

    for (size_t rows : {4096UL, 65536UL}) {
        for (size_t len : {8UL, 32UL}) {
            for (ReplicatePattern pattern : {ReplicatePattern::ZERO, ReplicatePattern::ONE, ReplicatePattern::FIXED_2,
                                             ReplicatePattern::RANDOM_0_4}) {
                register_case(
                        case_name(suite, "replicate",
                                  "rows:" + std::to_string(rows) + "/len:" + std::to_string(len) +
                                          "/repeat:" + replicate_pattern_name(pattern)),
                        [rows, len, pattern](benchmark::State& state) { bench_replicate(state, rows, len, pattern); });
            }
        }
    }

    for (size_t rows : {65536UL, 262144UL}) {
        for (size_t len : {8UL, 32UL}) {
            for (FilterPattern pattern : {FilterPattern::KEEP_ALL, FilterPattern::KEEP_NONE, FilterPattern::ALTERNATING,
                                          FilterPattern::RANDOM_10, FilterPattern::RANDOM_50}) {
                for (bool partial : {false, true}) {
                    register_case(case_name(suite, "filter_range",
                                            "rows:" + std::to_string(rows) + "/len:" + std::to_string(len) +
                                                    "/range:" + (partial ? "partial" : "full") +
                                                    "/filter:" + filter_pattern_name(pattern)),
                                  [rows, len, pattern, partial](benchmark::State& state) {
                                      bench_filter_range(state, rows, len, pattern, partial);
                                  });
                }
            }
        }
    }
    for (size_t rows : {65536UL, 262144UL}) {
        register_case(
                case_name(suite, "filter_range", "rows:" + std::to_string(rows) + "/len:4/range:full/filter:random50"),
                [rows](benchmark::State& state) {
                    bench_filter_range(state, rows, 4, FilterPattern::RANDOM_50, false);
                });
    }

    for (size_t len : {8UL, 128UL}) {
        register_case(case_name(suite, "cut_range", "rows:65536/len:" + std::to_string(len) + "/start:0/count:4096"),
                      [len](benchmark::State& state) { bench_cut(state, 65536, len, 0, 4096); });
        register_case(
                case_name(suite, "cut_range", "rows:65536/len:" + std::to_string(len) + "/start:1024/count:32768"),
                [len](benchmark::State& state) { bench_cut(state, 65536, len, 1024, 32768); });
        for (size_t count : {1UL, 32768UL, 65535UL, 65536UL}) {
            register_case(
                    case_name(suite, "remove_first_n_values",
                              "rows:65536/len:" + std::to_string(len) + "/count:" + std::to_string(count)),
                    [len, count](benchmark::State& state) { bench_remove_first_n_values(state, 65536, len, count); });
        }
        for (size_t idx : {0UL, 32768UL, 65535UL}) {
            register_case(
                    case_name(suite, "assign_repeated_index",
                              "rows:65536/len:" + std::to_string(len) + "/idx:" + std::to_string(idx) + "/count:65536"),
                    [len, idx](benchmark::State& state) { bench_assign(state, 65536, len, idx, 65536); });
        }
    }

    for (bool same_len : {true, false}) {
        register_case(case_name(suite, "update_rows",
                                std::string("rows:65536/len:32/replace:4096/mode:") +
                                        (same_len ? "same_len" : "different_len")),
                      [same_len](benchmark::State& state) { bench_update_rows(state, 65536, 32, 4096, same_len); });
    }

    for (size_t len : {8UL, 128UL}) {
        register_case(case_name(suite, "build_slices", "rows:262144/len:" + std::to_string(len)),
                      [len](benchmark::State& state) { bench_build_slices(state, 262144, len); });
        for (IndexPattern pattern : {IndexPattern::SEQ, IndexPattern::REVERSE, IndexPattern::RANDOM}) {
            register_case(
                    case_name(suite, "get_slice_idx_loop",
                              "rows:262144/len:" + std::to_string(len) + "/index:" + index_pattern_name(pattern)),
                    [len, pattern](benchmark::State& state) { bench_get_slice_scan(state, 262144, len, pattern); });
        }
    }
    for (bool warm : {false, true}) {
        register_case(case_name(suite, "raw_data_build_slices",
                                std::string("rows:65536/len:32/cache:") + (warm ? "warm" : "cold")),
                      [warm](benchmark::State& state) { bench_raw_data(state, 65536, 32, warm); });
        register_case(case_name(suite, "get_german_strings",
                                std::string("rows:65536/len:32/cache:") + (warm ? "warm" : "cold")),
                      [warm](benchmark::State& state) { bench_german_strings(state, 65536, 32, warm); });
    }
    for (LenPattern pattern : {LenPattern::FIXED, LenPattern::UNIFORM}) {
        register_case(case_name(suite, "max_one_element_serialize_size", rows_len_params(262144, 128, pattern)),
                      [pattern](benchmark::State& state) {
                          bench_max_one_element_serialize_size(state, 262144, 128, pattern);
                      });
    }
    register_case(case_name(suite, "xor_checksum", "rows:65536/len:32"),
                  [](benchmark::State& state) { bench_xor_checksum(state, 65536, 32); });
    for (size_t len : {8UL, 128UL}) {
        register_case(case_name(suite, "byte_size_idx_loop", "rows:262144/len:" + std::to_string(len)),
                      [len](benchmark::State& state) { bench_byte_size_scan(state, 262144, len); });
        register_case(case_name(suite, "serialize_size_idx_loop", "rows:262144/len:" + std::to_string(len)),
                      [len](benchmark::State& state) { bench_serialize_size_scan(state, 262144, len); });
    }

    for (size_t len : {8UL, 32UL, 128UL}) {
        register_case(case_name(suite, "serialize_batch", "rows:4096/len:" + std::to_string(len) + "/nullable:false"),
                      [len](benchmark::State& state) {
                          bench_serialize_batch(state, 4096, len, false, FilterPattern::KEEP_NONE);
                      });
        for (FilterPattern null_pattern :
             {FilterPattern::KEEP_NONE, FilterPattern::RANDOM_10, FilterPattern::RANDOM_50, FilterPattern::KEEP_ALL}) {
            register_case(case_name(suite, "serialize_batch",
                                    "rows:4096/len:" + std::to_string(len) +
                                            "/nullable:true/nulls:" + filter_pattern_name(null_pattern)),
                          [len, null_pattern](benchmark::State& state) {
                              bench_serialize_batch(state, 4096, len, true, null_pattern);
                          });
        }
        register_case(case_name(suite, "serialize_batch_at_interval",
                                "rows:4096/len:" + std::to_string(len) + "/mode:fit/nullable:false"),
                      [len](benchmark::State& state) {
                          bench_serialize_batch_at_interval(state, 4096, len, false, false, LenPattern::FIXED);
                      });
        register_case(case_name(suite, "serialize_batch_at_interval",
                                "rows:4096/len:" + std::to_string(len) + "/mode:reject_by_size/nullable:false"),
                      [len](benchmark::State& state) {
                          bench_serialize_batch_at_interval(state, 4096, len, true, false, LenPattern::FIXED);
                      });
        register_case(case_name(suite, "serialize_batch_at_interval",
                                "rows:4096/len:" + std::to_string(len) + "/mode:tail_zero_reject/nullable:false"),
                      [len](benchmark::State& state) {
                          bench_serialize_batch_at_interval(state, 4096, len, false, false, LenPattern::TAIL_ZERO);
                      });
        register_case(case_name(suite, "serialize_batch_at_interval",
                                "rows:4096/len:" + std::to_string(len) + "/mode:nullable_fit/nullable:true"),
                      [len](benchmark::State& state) {
                          bench_serialize_batch_at_interval(state, 4096, len, false, true, LenPattern::FIXED);
                      });
        register_case(case_name(suite, "deserialize_and_append_batch",
                                "rows:4096/len:" + std::to_string(len) + "/nullable:false"),
                      [len](benchmark::State& state) {
                          bench_deserialize_batch(state, 4096, len, false, FilterPattern::KEEP_NONE);
                      });
        register_case(case_name(suite, "deserialize_and_append_batch",
                                "rows:4096/len:" + std::to_string(len) + "/nullable:true/nulls:random10"),
                      [len](benchmark::State& state) {
                          bench_deserialize_batch(state, 4096, len, true, FilterPattern::RANDOM_10);
                      });
        register_case(case_name(suite, "serialize_idx_loop", "rows:4096/len:" + std::to_string(len)),
                      [len](benchmark::State& state) { bench_serialize_scalar_scan(state, 4096, len); });
        register_case(case_name(suite, "deserialize_and_append_loop", "rows:4096/len:" + std::to_string(len)),
                      [len](benchmark::State& state) { bench_deserialize_scalar_loop(state, 4096, len); });
    }
    register_case(case_name(suite, "serialize_batch", "rows:4096/len:4/nullable:false"), [](benchmark::State& state) {
        bench_serialize_batch(state, 4096, 4, false, FilterPattern::KEEP_NONE);
    });
    register_case(case_name(suite, "serialize_batch_at_interval", "rows:4096/len:4/mode:fit/nullable:false"),
                  [](benchmark::State& state) {
                      bench_serialize_batch_at_interval(state, 4096, 4, false, false, LenPattern::FIXED);
                  });
    register_case(
            case_name(suite, "deserialize_and_append_batch", "rows:4096/len:4/nullable:false"),
            [](benchmark::State& state) { bench_deserialize_batch(state, 4096, 4, false, FilterPattern::KEEP_NONE); });
}

} // namespace starrocks::bench
