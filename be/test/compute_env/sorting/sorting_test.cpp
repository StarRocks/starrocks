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

#include "column/sorting/sorting.h"

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/sorting/sort_permute.h"
#include "common/config_exec_fwd.h"
#include "compute_env/sorting/merge.h"
#include "compute_env/sorting/sort_cursor.h"
#include "compute_env/sorting/sorted_chunks_merger.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "runtime/runtime_state.h"
#include "types/type_descriptor.h"

namespace starrocks {
namespace {

static MutableColumnPtr build_int_column(const std::vector<int32_t>& values) {
    MutableColumnPtr column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false);
    for (int32_t value : values) {
        column->append_datum(Datum(value));
    }
    return column;
}

static void clear_exprs(std::vector<ExprContext*>& exprs) {
    for (ExprContext* ctx : exprs) {
        delete ctx;
    }
    exprs.clear();
}

static std::shared_ptr<RuntimeState> create_runtime_state() {
    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.batch_size = config::vector_chunk_size;
    TQueryGlobals query_globals;
    auto runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
    runtime_state->init_instance_mem_tracker();
    return runtime_state;
}

class SortingCoreTest : public testing::Test {
protected:
    void SetUp() override {
        _runtime_state = create_runtime_state();
        _exprs.emplace_back(std::make_unique<ColumnRef>(TypeDescriptor(TYPE_INT), 0));
        _sort_exprs.emplace_back(new ExprContext(_exprs.back().get()));
        ASSERT_OK(ExprExecutor::prepare(_sort_exprs, _runtime_state.get()));
        ASSERT_OK(ExprExecutor::open(_sort_exprs, _runtime_state.get()));
    }

    void TearDown() override { clear_exprs(_sort_exprs); }

    ChunkUniquePtr make_chunk(const std::vector<int32_t>& values) {
        return std::make_unique<Chunk>(Columns{build_int_column(values)}, Chunk::SlotHashMap{{0, 0}});
    }

    std::shared_ptr<RuntimeState> _runtime_state;
    std::vector<std::unique_ptr<ColumnRef>> _exprs;
    std::vector<ExprContext*> _sort_exprs;
};

TEST_F(SortingCoreTest, simple_cursor_materializes_sort_columns) {
    auto input = make_chunk({1, 3, 5});
    bool emitted = false;

    ChunkProvider provider = [&input, &emitted](ChunkUniquePtr* output, bool* eos) mutable {
        if (output == nullptr || eos == nullptr) {
            return true;
        }
        if (emitted) {
            *output = nullptr;
            *eos = true;
            return false;
        }
        *output = std::move(input);
        *eos = false;
        emitted = true;
        return true;
    };

    SimpleChunkSortCursor cursor(std::move(provider), &_sort_exprs);
    ASSERT_TRUE(cursor.is_data_ready());

    auto [chunk, sort_columns] = cursor.try_get_next();
    ASSERT_NE(nullptr, chunk);
    ASSERT_EQ(3, chunk->num_rows());
    ASSERT_EQ(1, sort_columns.size());
    ASSERT_EQ(3, sort_columns[0]->size());
    EXPECT_EQ(1, sort_columns[0]->get(0).get_int32());
    EXPECT_EQ(3, sort_columns[0]->get(1).get_int32());
    EXPECT_EQ(5, sort_columns[0]->get(2).get_int32());
}

TEST_F(SortingCoreTest, merge_sorted_chunks) {
    std::vector<ChunkUniquePtr> input_chunks;
    input_chunks.emplace_back(make_chunk({-2074, -1691, -1400, -969, -767, -725}));
    input_chunks.emplace_back(make_chunk({-680, -571, -568}));
    input_chunks.emplace_back(make_chunk({-2118, -2065, -1328, -1103, -1099, -1093}));
    input_chunks.emplace_back(make_chunk({-950, -807, -604}));

    SortDescs sort_desc(std::vector<int>{1}, std::vector<int>{-1});
    SortedRuns output;
    ASSERT_OK(merge_sorted_chunks(sort_desc, &_sort_exprs, input_chunks, &output));
    ASSERT_TRUE(output.is_sorted(sort_desc));
    ASSERT_EQ(18, output.num_rows());
}

// Regression test for https://github.com/StarRocks/starrocks/issues/77374, on the path a spilled sort takes.
//
// A spilled sort sorts every mem table with `sort_and_tie_columns` (spill::OrderedMemTable::_do_sort) and merges
// the restored blocks with a CascadeChunkMerger (spill::OrderedInputStream), so the two phases must agree on
// where a NULL nested in an ARRAY goes, exactly like the non-spilled full sort does.
class SpilledArrayNestedNullSortTest : public testing::Test {
protected:
    // The rows of issue #77374, `id` in column 0 and `arr` in column 1
    static constexpr int kNumRows = 8;

    void SetUp() override {
        _runtime_state = create_runtime_state();
        _exprs.emplace_back(
                std::make_unique<ColumnRef>(TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT)), 1));
        _sort_exprs.emplace_back(new ExprContext(_exprs.back().get()));
        ASSERT_OK(ExprExecutor::prepare(_sort_exprs, _runtime_state.get()));
        ASSERT_OK(ExprExecutor::open(_sort_exprs, _runtime_state.get()));
    }

    void TearDown() override { clear_exprs(_sort_exprs); }

    static ChunkUniquePtr build_chunk(int32_t first_id, size_t num_rows) {
        static const std::vector<std::vector<std::optional<int32_t>>> arrays = {
                {std::nullopt}, {5}, {4}, {3}, {std::nullopt, 2}, {7}, {std::nullopt, 9}, {8}};

        auto id_column = Int32Column::create();
        auto elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
        auto offsets = UInt32Column::create();
        offsets->append(0);
        uint32_t offset = 0;
        for (size_t i = 0; i < num_rows; i++) {
            int32_t id = first_id + static_cast<int32_t>(i);
            id_column->append_datum(Datum(id));
            for (const auto& element : arrays[id - 1]) {
                if (element.has_value()) {
                    elements->append_datum(Datum(element.value()));
                } else {
                    elements->append_nulls(1);
                }
                offset++;
            }
            offsets->append(offset);
        }
        auto array_column = ArrayColumn::create(std::move(elements), std::move(offsets));

        Chunk::SlotHashMap slot_map{{0, 0}, {1, 1}};
        return std::make_unique<Chunk>(Columns{std::move(id_column), std::move(array_column)}, slot_map);
    }

    // The way a mem table is sorted before it is spilled
    static ChunkUniquePtr sort_chunk(const SortDescs& sort_desc, ChunkUniquePtr chunk) {
        std::atomic<bool> cancel{false};
        Columns key_columns{chunk->get_column_by_index(1)};
        Permutation perm;
        CHECK_OK(sort_and_tie_columns(cancel, key_columns, sort_desc, &perm));

        ChunkUniquePtr sorted = chunk->clone_empty_with_slot();
        ChunkPtr input = std::move(chunk);
        materialize_by_permutation(sorted.get(), {input}, perm);
        return sorted;
    }

    static std::vector<int32_t> collect_ids(const Chunk& chunk) {
        std::vector<int32_t> ids;
        const auto& id_column = chunk.get_column_by_index(0);
        for (size_t i = 0; i < chunk.num_rows(); i++) {
            ids.push_back(id_column->get(i).get_int32());
        }
        return ids;
    }

    // The way the restored blocks are merged back together
    std::vector<int32_t> merge_sorted_chunks(const SortDescs& sort_desc, std::vector<ChunkUniquePtr> chunks) {
        std::vector<ChunkProvider> providers;
        auto pending = std::make_shared<std::vector<ChunkUniquePtr>>(std::move(chunks));
        for (size_t i = 0; i < pending->size(); i++) {
            providers.emplace_back([pending, i](ChunkUniquePtr* output, bool* eos) {
                if (output == nullptr || eos == nullptr) {
                    return true;
                }
                if ((*pending)[i] == nullptr) {
                    *eos = true;
                    return false;
                }
                *output = std::move((*pending)[i]);
                *eos = false;
                return true;
            });
        }

        CascadeChunkMerger merger(_runtime_state.get());
        CHECK_OK(merger.init(providers, &_sort_exprs, sort_desc));

        std::vector<int32_t> ids;
        std::atomic<bool> eos{false};
        while (!eos) {
            ChunkUniquePtr chunk;
            bool should_exit = false;
            CHECK_OK(merger.get_next(&chunk, &eos, &should_exit));
            if (chunk != nullptr && !chunk->is_empty()) {
                auto part = collect_ids(*chunk);
                ids.insert(ids.end(), part.begin(), part.end());
            }
        }
        return ids;
    }

    void check(const SortDescs& sort_desc, const std::vector<int32_t>& expected) {
        // A single mem table that never spills
        ASSERT_EQ(expected, collect_ids(*sort_chunk(sort_desc, build_chunk(1, kNumRows))));

        // Four spilled mem tables restored and merged back
        std::vector<ChunkUniquePtr> spilled;
        for (int32_t first_id = 1; first_id <= kNumRows; first_id += 2) {
            spilled.emplace_back(sort_chunk(sort_desc, build_chunk(first_id, 2)));
        }
        ASSERT_EQ(expected, merge_sorted_chunks(sort_desc, std::move(spilled)));
    }

    std::shared_ptr<RuntimeState> _runtime_state;
    std::vector<std::unique_ptr<ColumnRef>> _exprs;
    std::vector<ExprContext*> _sort_exprs;
};

TEST_F(SpilledArrayNestedNullSortTest, asc_null_first) {
    check(SortDescs(std::vector<int>{1}, std::vector<int>{-1}), {1, 5, 7, 4, 3, 2, 6, 8});
}

TEST_F(SpilledArrayNestedNullSortTest, desc_null_last) {
    // The reverse of the ascending order: a NULL element is the smallest, so it is the last one
    check(SortDescs(std::vector<int>{-1}, std::vector<int>{-1}), {8, 6, 2, 3, 4, 7, 5, 1});
}

TEST_F(SpilledArrayNestedNullSortTest, desc_null_first) {
    check(SortDescs(std::vector<int>{-1}, std::vector<int>{1}), {7, 5, 1, 8, 6, 2, 3, 4});
}

} // namespace
} // namespace starrocks
