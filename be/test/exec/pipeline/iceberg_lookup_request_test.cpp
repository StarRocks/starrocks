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

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "exec/pipeline/lookup_request.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

class IcebergLookUpTaskTest : public testing::Test {
protected:
    ChunkPtr payload(std::initializer_list<int64_t> values) {
        auto column = Int64Column::create();
        for (auto value : values) column->append(value);
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(std::move(column), 10);
        return chunk;
    }

    ColumnPtr positions(std::initializer_list<uint32_t> values) {
        auto column = UInt32Column::create();
        for (auto value : values) column->append(value);
        return column;
    }

    RuntimeState state{TQueryGlobals()};
    IcebergLookUpTask task{std::make_shared<LookUpTaskContext>()};
};

TEST_F(IcebergLookUpTaskTest, RestoreDuplicatedRowsAndOriginalOrder) {
    // Sorted locators correspond to original rows 2, 0 (duplicate), 3, 1.
    auto result = task._restore_row_order(&state, payload({10, 20, 30}), positions({2, 0, 3, 1}), {0, 2, 3, 4});
    ASSERT_TRUE(result.ok()) << result.status();
    auto column = result.value()->get_column_by_slot_id(10);
    ASSERT_EQ(4, column->size());
    EXPECT_EQ(10, column->get(0).get_int64());
    EXPECT_EQ(30, column->get(1).get_int64());
    EXPECT_EQ(10, column->get(2).get_int64());
    EXPECT_EQ(20, column->get(3).get_int64());
}

TEST_F(IcebergLookUpTaskTest, RejectMissingRowsBeforeReplication) {
    // Three distinct positions were requested, but only two payload rows arrived.
    auto result = task._restore_row_order(&state, payload({10, 20}), positions({2, 0, 3, 1}), {0, 2, 3, 4});
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_internal_error());
}

TEST_F(IcebergLookUpTaskTest, RejectExtraRowsBeforeReplication) {
    auto result = task._restore_row_order(&state, payload({10, 20, 30, 40}), positions({2, 0, 3, 1}), {0, 2, 3, 4});
    ASSERT_FALSE(result.ok());
}

TEST_F(IcebergLookUpTaskTest, RestoreUniqueRowsAndRejectCardinalityMismatch) {
    auto result = task._restore_row_order(&state, payload({10, 20}), positions({1, 0}), {});
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(20, result.value()->get_column_by_slot_id(10)->get(0).get_int64());
    EXPECT_EQ(10, result.value()->get_column_by_slot_id(10)->get(1).get_int64());
    EXPECT_FALSE(task._restore_row_order(&state, payload({10}), positions({1, 0}), {}).ok());
    EXPECT_FALSE(task._restore_row_order(&state, nullptr, positions({0}), {}).ok());
}

} // namespace starrocks::pipeline
