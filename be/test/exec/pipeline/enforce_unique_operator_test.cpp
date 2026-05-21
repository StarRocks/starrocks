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

#include "exec/pipeline/enforce_unique_operator.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

// Helper that builds a two-column chunk:
//   col 0 — nullable BinaryColumn  (file_path)
//   col 1 — nullable FixedLengthColumn<int64_t> (row_position)
//
// Pass an empty string "" as file_path to represent a NULL value for that row.
// Pass INT64_MIN as row_position to represent a NULL value for that row.
static ChunkPtr build_chunk(const std::vector<std::string>& files, const std::vector<int64_t>& positions,
                            bool nullable = true) {
    DCHECK_EQ(files.size(), positions.size());

    auto chunk = std::make_shared<Chunk>();

    if (nullable) {
        auto file_data = BinaryColumn::create();
        auto file_null = NullColumn::create();
        for (const auto& f : files) {
            if (f.empty()) {
                file_data->append_default(); // placeholder value
                file_null->append(1);        // NULL
            } else {
                file_data->append(f);
                file_null->append(0);
            }
        }
        chunk->append_column(NullableColumn::create(std::move(file_data), std::move(file_null)), 0);

        auto pos_data = FixedLengthColumn<int64_t>::create();
        auto pos_null = NullColumn::create();
        for (auto p : positions) {
            if (p == INT64_MIN) {
                pos_data->append_default(); // placeholder
                pos_null->append(1);        // NULL
            } else {
                pos_data->append(p);
                pos_null->append(0);
            }
        }
        chunk->append_column(NullableColumn::create(std::move(pos_data), std::move(pos_null)), 1);
    } else {
        auto file_col = BinaryColumn::create();
        for (const auto& f : files) {
            file_col->append(f);
        }
        chunk->append_column(std::move(file_col), 0);

        auto pos_col = FixedLengthColumn<int64_t>::create();
        for (auto p : positions) {
            pos_col->append(p);
        }
        chunk->append_column(std::move(pos_col), 1);
    }

    return chunk;
}

class EnforceUniqueOperatorTest : public ::testing::Test {
protected:
    void SetUp() override {}

    EnforceUniqueOperatorFactory make_factory() {
        return EnforceUniqueOperatorFactory(/*id=*/0, /*plan_node_id=*/0, std::vector<int32_t>{0, 1});
    }

    RuntimeState _state;
};

// All rows have distinct (file_path, row_position) pairs — push must succeed.
TEST_F(EnforceUniqueOperatorTest, NoDuplicates) {
    auto factory = make_factory();
    auto op = factory.create(/*dop=*/1, /*driver_seq=*/0);

    auto chunk = build_chunk({"file1.parquet", "file1.parquet", "file2.parquet"}, {0, 1, 0});
    ASSERT_OK(op->push_chunk(&_state, chunk));

    ASSERT_TRUE(op->has_output());
    ASSIGN_OR_ABORT(auto pulled, op->pull_chunk(&_state));
    ASSERT_NE(pulled, nullptr);
    EXPECT_EQ(pulled->num_rows(), 3);
}

// Two rows share the same (file_path, row_position) — push must return an error.
TEST_F(EnforceUniqueOperatorTest, DuplicateDetected) {
    auto factory = make_factory();
    auto op = factory.create(1, 0);

    auto chunk = build_chunk({"file1.parquet", "file1.parquet"}, {0, 0});
    auto st = op->push_chunk(&_state, chunk);
    EXPECT_FALSE(st.ok());
}

// Rows with NULL keys must be skipped — no duplicate check applied for them.
TEST_F(EnforceUniqueOperatorTest, NullKeysSkipped) {
    auto factory = make_factory();
    auto op = factory.create(1, 0);

    // Row 0: (file1.parquet, 0)   — valid key, inserted
    // Row 1: (NULL, NULL)         — skipped
    // Row 2: (NULL, NULL)         — skipped (would be a "duplicate" but must be ignored)
    auto chunk = build_chunk({"file1.parquet", "", ""}, {0, INT64_MIN, INT64_MIN});
    ASSERT_OK(op->push_chunk(&_state, chunk));

    ASSERT_TRUE(op->has_output());
    ASSIGN_OR_ABORT(auto pulled, op->pull_chunk(&_state));
    ASSERT_NE(pulled, nullptr);
    EXPECT_EQ(pulled->num_rows(), 3);
}

// A duplicate that spans two separate chunks must still be detected.
TEST_F(EnforceUniqueOperatorTest, CrossChunkDuplicate) {
    auto factory = make_factory();
    auto op = factory.create(1, 0);

    // First chunk — (file1.parquet, 0) is accepted.
    auto chunk1 = build_chunk({"file1.parquet"}, {0});
    ASSERT_OK(op->push_chunk(&_state, chunk1));

    // Pull so that the operator is ready for the next input.
    ASSERT_TRUE(op->has_output());
    ASSIGN_OR_ABORT(auto pulled1, op->pull_chunk(&_state));
    ASSERT_NE(pulled1, nullptr);

    // Second chunk — same (file1.parquet, 0) must now trigger a duplicate error.
    auto chunk2 = build_chunk({"file1.parquet"}, {0});
    auto st = op->push_chunk(&_state, chunk2);
    EXPECT_FALSE(st.ok());
}

// An empty chunk must be accepted without error.
TEST_F(EnforceUniqueOperatorTest, EmptyChunk) {
    auto factory = make_factory();
    auto op = factory.create(1, 0);

    auto chunk = build_chunk({}, {});
    ASSERT_OK(op->push_chunk(&_state, chunk));

    // Empty chunk is not stored as output — need_input should still be true.
    EXPECT_FALSE(op->has_output());
    EXPECT_TRUE(op->need_input());
}

// Non-nullable columns must also work (operator handles both paths).
TEST_F(EnforceUniqueOperatorTest, NonNullableColumnsDuplicate) {
    auto factory = make_factory();
    auto op = factory.create(1, 0);

    auto chunk = build_chunk({"file1.parquet", "file1.parquet"}, {5, 5}, /*nullable=*/false);
    auto st = op->push_chunk(&_state, chunk);
    EXPECT_FALSE(st.ok());
}

// Non-nullable columns with unique rows must pass.
TEST_F(EnforceUniqueOperatorTest, NonNullableColumnsNoDuplicates) {
    auto factory = make_factory();
    auto op = factory.create(1, 0);

    auto chunk = build_chunk({"file1.parquet", "file2.parquet"}, {0, 0}, /*nullable=*/false);
    ASSERT_OK(op->push_chunk(&_state, chunk));

    ASSERT_TRUE(op->has_output());
    ASSIGN_OR_ABORT(auto pulled, op->pull_chunk(&_state));
    ASSERT_NE(pulled, nullptr);
    EXPECT_EQ(pulled->num_rows(), 2);
}

// set_finishing transitions the operator to the finished state.
TEST_F(EnforceUniqueOperatorTest, SetFinishing) {
    auto factory = make_factory();
    auto op = factory.create(1, 0);

    EXPECT_FALSE(op->is_finished());
    ASSERT_OK(op->set_finishing(&_state));
    // No pending output, so operator should be finished.
    EXPECT_TRUE(op->is_finished());
}

} // namespace starrocks::pipeline
