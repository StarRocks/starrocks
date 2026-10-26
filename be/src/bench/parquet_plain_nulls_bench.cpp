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

// Nullable fixed-length PLAIN decoding: the generic Decoder::next_batch_with_nulls (one append_nulls /
// append_numbers pair per null run) against PlainDecoder<T>'s batch override (null column in one memcpy,
// values scattered straight from the page with SIMD::Expand).
//
//   ./parquet_plain_nulls_bench --benchmark_repetitions=5 --benchmark_report_aggregates_only=true

#include <benchmark/benchmark.h>

#include <cstring>
#include <random>
#include <vector>

#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "formats/parquet/encoding.h"
#include "formats/parquet/types.h"
#include "formats/parquet/utils.h"

namespace starrocks::parquet {

static constexpr size_t kPageRows = 64 * 1024;
static constexpr size_t kBatchSize = 4096;

enum DecodePath { GENERIC = 0, BATCH = 1 };

template <tparquet::Type::type PT>
static void BM_PlainWithNulls(benchmark::State& state) {
    using T = typename PhysicalTypeTraits<PT>::CppType;
    const auto path = static_cast<DecodePath>(state.range(0));
    const int null_pct = static_cast<int>(state.range(1));
    // bytes in front of the values: definition levels of any length leave the values misaligned for T
    const size_t misalign = static_cast<size_t>(state.range(2));

    std::mt19937 rng(42);
    std::uniform_int_distribution<int> pct(0, 99);
    std::vector<uint8_t> nulls(kPageRows);
    size_t num_values = 0;
    for (auto& n : nulls) {
        n = pct(rng) < null_pct;
        num_values += !n;
    }
    std::vector<uint8_t> page(misalign + num_values * sizeof(T));
    for (size_t i = 0; i < num_values; i++) {
        T v = static_cast<T>(rng());
        memcpy(page.data() + misalign + i * sizeof(T), &v, sizeof(T));
    }

    std::vector<NullInfos> batches((kPageRows + kBatchSize - 1) / kBatchSize);
    for (size_t b = 0; b < batches.size(); b++) {
        size_t begin = b * kBatchSize;
        size_t count = std::min(kBatchSize, kPageRows - begin);
        auto& infos = batches[b];
        infos.reset_with_capacity(count);
        memcpy(infos.nulls_data(), nulls.data() + begin, count);
        for (size_t i = 0; i < count; i++) {
            infos.num_nulls += nulls[begin + i];
            infos.num_ranges += (i == 0 || nulls[begin + i] != nulls[begin + i - 1]);
        }
    }

    const EncodingInfo* encoding = nullptr;
    CHECK(EncodingInfo::get(PT, tparquet::Encoding::PLAIN, &encoding).ok());
    std::unique_ptr<Decoder> decoder;
    CHECK(encoding->create_decoder(&decoder).ok());
    auto dst = NullableColumn::create(FixedLengthColumn<T>::create(), NullColumn::create());
    dst->reserve(kBatchSize);

    for (auto _ : state) {
        CHECK(decoder->set_data(Slice(page.data() + misalign, page.size() - misalign)).ok());
        for (size_t b = 0; b < batches.size(); b++) {
            size_t count = std::min(kBatchSize, kPageRows - b * kBatchSize);
            dst->reset_column();
            Status st;
            if (path == GENERIC) {
                // qualified call skips the virtual override: the pre-change code path
                st = decoder->Decoder::next_batch_with_nulls(count, batches[b], ColumnContentType::VALUE, dst.get(),
                                                             nullptr);
            } else {
                st = decoder->next_batch_with_nulls(count, batches[b], ColumnContentType::VALUE, dst.get(), nullptr);
            }
            CHECK(st.ok()) << st;
            benchmark::DoNotOptimize(dst->size());
        }
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kPageRows);
    state.SetLabel(path == GENERIC ? "generic" : "batch");
}

// BOOLEAN PLAIN is bit packed, so the page comes from the real encoder.
static void BM_PlainBooleanWithNulls(benchmark::State& state) {
    const auto path = static_cast<DecodePath>(state.range(0));
    const int null_pct = static_cast<int>(state.range(1));
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> pct(0, 99);
    std::vector<uint8_t> nulls(kPageRows);
    std::vector<uint8_t> values;
    for (auto& n : nulls) {
        n = pct(rng) < null_pct;
        if (!n) values.push_back(rng() & 1);
    }
    const EncodingInfo* encoding = nullptr;
    CHECK(EncodingInfo::get(tparquet::Type::BOOLEAN, tparquet::Encoding::PLAIN, &encoding).ok());
    std::unique_ptr<Encoder> encoder;
    CHECK(encoding->create_encoder(&encoder).ok());
    CHECK(encoder->append(values.data(), values.size()).ok());
    std::string page = encoder->build().to_string();

    std::vector<NullInfos> batches((kPageRows + kBatchSize - 1) / kBatchSize);
    for (size_t b = 0; b < batches.size(); b++) {
        size_t begin = b * kBatchSize;
        size_t count = std::min(kBatchSize, kPageRows - begin);
        auto& infos = batches[b];
        infos.reset_with_capacity(count);
        memcpy(infos.nulls_data(), nulls.data() + begin, count);
        for (size_t i = 0; i < count; i++) {
            infos.num_nulls += nulls[begin + i];
            infos.num_ranges += (i == 0 || nulls[begin + i] != nulls[begin + i - 1]);
        }
    }
    std::unique_ptr<Decoder> decoder;
    CHECK(encoding->create_decoder(&decoder).ok());
    auto dst = NullableColumn::create(BooleanColumn::create(), NullColumn::create());
    for (auto _ : state) {
        CHECK(decoder->set_data(Slice(page)).ok());
        for (size_t b = 0; b < batches.size(); b++) {
            size_t count = std::min(kBatchSize, kPageRows - b * kBatchSize);
            dst->reset_column();
            Status st = path == GENERIC ? decoder->Decoder::next_batch_with_nulls(
                                                  count, batches[b], ColumnContentType::VALUE, dst.get(), nullptr)
                                        : decoder->next_batch_with_nulls(count, batches[b], ColumnContentType::VALUE,
                                                                         dst.get(), nullptr);
            CHECK(st.ok()) << st;
            benchmark::DoNotOptimize(dst->size());
        }
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kPageRows);
    state.SetLabel(path == GENERIC ? "generic" : "batch");
}

static void NullArgs(benchmark::internal::Benchmark* b) {
    b->ArgNames({"path", "null_pct", "misalign"});
    for (int misalign : {0, 1}) {
        for (int null_pct : {1, 5, 20, 50, 90}) {
            for (int path : {GENERIC, BATCH}) {
                b->Args({path, null_pct, misalign});
            }
        }
    }
    b->Unit(benchmark::kMicrosecond);
}

BENCHMARK_TEMPLATE(BM_PlainWithNulls, tparquet::Type::INT32)->Apply(NullArgs);
BENCHMARK_TEMPLATE(BM_PlainWithNulls, tparquet::Type::INT64)->Apply(NullArgs);
BENCHMARK_TEMPLATE(BM_PlainWithNulls, tparquet::Type::DOUBLE)->Apply(NullArgs);
BENCHMARK(BM_PlainBooleanWithNulls)
        ->ArgNames({"path", "null_pct"})
        ->ArgsProduct({{GENERIC, BATCH}, {1, 5, 20, 50, 90}})
        ->Unit(benchmark::kMicrosecond);

} // namespace starrocks::parquet

BENCHMARK_MAIN();
