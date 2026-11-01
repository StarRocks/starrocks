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

#include <algorithm>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "column/chunk_factory.h"
#include "column/column.h"
#include "column/nullable_column.h"
#include "storage/rowset/bitshuffle_page.h"
#include "storage/rowset/options.h"
#include "storage/rowset/storage_page_decoder.h"

namespace starrocks {

namespace {

constexpr uint32_t kPageRows = 16384;

// One encoded bitshuffle page plus the decoder over it. Kept alive for the whole benchmark.
template <LogicalType Type>
struct PageFixture {
    using CppType = StorageCppType<Type>;

    OwnedSlice owned;
    Slice encoded;
    std::unique_ptr<std::vector<uint8_t>> decoded_page;
    std::unique_ptr<BitShufflePageDecoder<Type>> decoder;
    std::vector<uint8_t> null_flags;

    Status init() {
        std::mt19937 rng(42);
        auto values = std::make_unique<CppType[]>(kPageRows);
        for (size_t i = 0; i < kPageRows; ++i) {
            if constexpr (std::is_same_v<CppType, bool>) {
                values[i] = static_cast<bool>(rng() & 1);
            } else {
                values[i] = static_cast<CppType>(rng());
            }
        }

        null_flags.resize(kPageRows);
        for (size_t i = 0; i < kPageRows; ++i) {
            null_flags[i] = (i % 10 == 0 ? 1 : 0);
        }

        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        BitshufflePageBuilder<Type> builder(options);
        const size_t added = builder.add(reinterpret_cast<const uint8_t*>(values.get()), kPageRows);
        if (added != kPageRows) {
            return Status::InternalError("page builder did not accept all rows");
        }
        owned = builder.finish()->build();
        encoded = owned.slice();

        PageFooterPB footer;
        footer.set_type(DATA_PAGE);
        footer.mutable_data_page_footer()->set_nullmap_size(0);
        RETURN_IF_ERROR(StoragePageDecoder::decode_page(&footer, 0, BIT_SHUFFLE, &decoded_page, &encoded));

        decoder = std::make_unique<BitShufflePageDecoder<Type>>(encoded);
        return decoder->init();
    }
};

// Ascending, unique rowids drawn from a fixed seed so every run and every build sees the same input.
std::vector<rowid_t> make_rowids(size_t selectivity_pct) {
    const size_t k = static_cast<size_t>(kPageRows) * selectivity_pct / 100;
    std::vector<rowid_t> all(kPageRows);
    std::iota(all.begin(), all.end(), 0);
    std::mt19937 rng(42);
    std::shuffle(all.begin(), all.end(), rng);
    all.resize(k);
    std::sort(all.begin(), all.end());
    return all;
}

// column_mode:
//   0: non-nullable destination column
//   1: nullable destination column, null-free page (ParsedPageV2 passes outer NullableColumn* directly)
//   2: nullable destination column, page with nullmap (ParsedPageV2 unwraps nc->data_column_raw_ptr() + null_flags)
template <LogicalType Type>
void run_read_by_rowids(benchmark::State& state, int column_mode, size_t selectivity_pct) {
    const bool nullable = (column_mode != 0);
    const bool has_nullmap = (column_mode == 2);
    PageFixture<Type> page;
    if (Status st = page.init(); !st.ok()) {
        state.SkipWithError(st.to_string().c_str());
        return;
    }
    const std::vector<rowid_t> rowids = make_rowids(selectivity_pct);

    // Self-check against the sequential decode path, which this benchmark does not exercise.
    {
        auto ref = ChunkFactory::column_from_field_type(Type, nullable);
        size_t n = kPageRows;
        BitShufflePageDecoder<Type> seq(page.encoded);
        Column* target_ref = nullable ? down_cast<NullableColumn*>(ref.get())->data_column_raw_ptr() : ref.get();
        if (Status st = seq.init(); !st.ok() || !(st = seq.next_batch(&n, target_ref)).ok() || n != kPageRows) {
            state.SkipWithError("reference decode failed");
            return;
        }
        if (nullable) {
            auto* nc_ref = down_cast<NullableColumn*>(ref.get());
            if (has_nullmap) {
                nc_ref->null_column_raw_ptr()->append_numbers(page.null_flags.data(), page.null_flags.size());
                nc_ref->update_has_null();
            } else {
                nc_ref->null_column_data().resize(kPageRows, 0);
            }
        }

        auto out = ChunkFactory::column_from_field_type(Type, nullable);
        size_t count = rowids.size();
        Column* target_out = has_nullmap ? down_cast<NullableColumn*>(out.get())->data_column_raw_ptr() : out.get();
        if (Status st = page.decoder->read_by_rowids(0, rowids.data(), &count, target_out);
            !st.ok() || count != rowids.size() || target_out->size() != rowids.size()) {
            state.SkipWithError("read_by_rowids failed or returned a short count");
            return;
        }
        if (has_nullmap) {
            std::vector<uint8_t> null_flags;
            null_flags.reserve(count);
            for (size_t i = 0; i < count; i++) {
                null_flags.emplace_back(page.null_flags[rowids[i]]);
            }
            auto* nc_out = down_cast<NullableColumn*>(out.get());
            nc_out->null_column_raw_ptr()->append_numbers(null_flags.data(), null_flags.size());
            nc_out->update_has_null();
        }
        for (size_t i = 0; i < rowids.size(); i++) {
            if (!out->equals(i, *ref, rowids[i])) {
                state.SkipWithError("decoded value mismatch");
                return;
            }
        }
    }

    // Reuse a reserved destination so the loop measures decode work and the decoder's own
    // allocations, not column growth.
    auto column = ChunkFactory::column_from_field_type(Type, nullable);
    column->reserve(rowids.size());
    for (auto _ : state) {
        column->resize(0);
        size_t count = rowids.size();
        if (has_nullmap) {
            auto* nc = down_cast<NullableColumn*>(column.get());
            Status st = page.decoder->read_by_rowids(0, rowids.data(), &count, nc->data_column_raw_ptr());
            std::vector<uint8_t> null_flags;
            null_flags.reserve(count);
            for (size_t i = 0; i < count; i++) {
                null_flags.emplace_back(page.null_flags[rowids[i]]);
            }
            nc->null_column_raw_ptr()->append_numbers(null_flags.data(), null_flags.size());
            nc->update_has_null();
            benchmark::DoNotOptimize(st);
            benchmark::DoNotOptimize(count);
            benchmark::ClobberMemory();
        } else {
            Status st = page.decoder->read_by_rowids(0, rowids.data(), &count, column.get());
            benchmark::DoNotOptimize(st);
            benchmark::DoNotOptimize(count);
            benchmark::ClobberMemory();
        }
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(rowids.size()));
}

template <LogicalType Type>
void register_type(const char* type_name) {
    for (int column_mode : {0, 1, 2}) {
        for (int pct : {1, 10, 50}) {
            const std::string name = std::string("BM_BitShuffleReadByRowids/") + type_name + "/" +
                                     std::to_string(column_mode) + "/" + std::to_string(pct);
            benchmark::RegisterBenchmark(name.c_str(), [column_mode, pct](benchmark::State& state) {
                run_read_by_rowids<Type>(state, column_mode, static_cast<size_t>(pct));
            });
        }
    }
}

[[maybe_unused]] const bool registered = [] {
    register_type<TYPE_INT>("INT");
    register_type<TYPE_BIGINT>("BIGINT");
    register_type<TYPE_DATE>("DATE");
    register_type<TYPE_DATETIME>("DATETIME");
    register_type<TYPE_BOOLEAN>("BOOLEAN");
    return true;
}();

} // namespace

} // namespace starrocks
