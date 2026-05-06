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

#include <gtest/gtest.h>

#include <memory>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "common/config_exec_fwd.h"
#include "exec/sorting/merge.h"
#include "exec/sorting/sort_cursor.h"
#include "exec/sorting/sorting.h"
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

} // namespace
} // namespace starrocks
