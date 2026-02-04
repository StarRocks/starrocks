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

#include "exec/pipeline/limit_operator.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

class AutoIncChunkBuilder {
public:
    AutoIncChunkBuilder(size_t chunk_size = 4096) : _chunk_size(chunk_size) {}

    ChunkPtr get_next() {
        ChunkPtr chunk = std::make_shared<Chunk>();
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_BIGINT), false);
        for (size_t i = 0; i < _chunk_size; i++) {
            col->append_datum(Datum(_next_value++));
        }
        chunk->append_column(std::move(col), 0);
        return chunk;
    }
    size_t _next_value = 0;
    size_t _chunk_size;
};

class LimitOperatorTest : public ::testing::Test {
public:
    void SetUp() override { dummy_runtime_state.set_chunk_size(chunk_size); }

    void TearDown() override {}

protected:
    RuntimeState dummy_runtime_state;
    int limit = 6000;
    size_t chunk_size = 4096;
    AutoIncChunkBuilder builder{chunk_size};
};

TEST_F(LimitOperatorTest, test_limit_chunk_in_place) {
    LimitOperatorFactory factory(1, 1, limit, true /*limit_chunk_in_place*/);
    auto limit_op = factory.create(1, 1);

    auto first_pushed_chunk = builder.get_next();
    EXPECT_TRUE(limit_op->push_chunk(&dummy_runtime_state, first_pushed_chunk).ok());
    auto first_pulled_chunk_or_status = limit_op->pull_chunk(&dummy_runtime_state);
    ASSERT_OK(first_pulled_chunk_or_status.status());
    auto first_pulled_chunk = first_pulled_chunk_or_status.value();
    EXPECT_TRUE(first_pushed_chunk == first_pulled_chunk);
    EXPECT_TRUE(first_pulled_chunk->num_rows() == chunk_size);

    auto second_pushed_chunk = builder.get_next();
    EXPECT_TRUE(limit_op->push_chunk(&dummy_runtime_state, second_pushed_chunk).ok());
    auto second_pulled_chunk_or_status = limit_op->pull_chunk(&dummy_runtime_state);
    ASSERT_OK(second_pulled_chunk_or_status.status());
    auto second_pulled_chunk = second_pulled_chunk_or_status.value();
    EXPECT_TRUE(second_pushed_chunk == second_pulled_chunk);
    EXPECT_TRUE(second_pulled_chunk->num_rows() == limit - chunk_size);
}

TEST_F(LimitOperatorTest, test_limit_chunk_clone_on_update) {
    LimitOperatorFactory factory(1, 1, limit, false /*limit_chunk_in_place*/);
    auto limit_op = factory.create(1, 1);

    auto first_pushed_chunk = builder.get_next();
    EXPECT_TRUE(limit_op->push_chunk(&dummy_runtime_state, first_pushed_chunk).ok());
    auto first_pulled_chunk_or_status = limit_op->pull_chunk(&dummy_runtime_state);
    ASSERT_OK(first_pulled_chunk_or_status.status());
    auto first_pulled_chunk = first_pulled_chunk_or_status.value();
    EXPECT_TRUE(first_pushed_chunk == first_pulled_chunk);
    EXPECT_TRUE(first_pulled_chunk->num_rows() == chunk_size);

    auto second_pushed_chunk = builder.get_next();
    EXPECT_TRUE(limit_op->push_chunk(&dummy_runtime_state, second_pushed_chunk).ok());
    auto second_pulled_chunk_or_status = limit_op->pull_chunk(&dummy_runtime_state);
    ASSERT_OK(second_pulled_chunk_or_status.status());
    auto second_pulled_chunk = second_pulled_chunk_or_status.value();
    EXPECT_TRUE(second_pushed_chunk != second_pulled_chunk);
    EXPECT_TRUE(second_pushed_chunk->num_rows() == chunk_size);
    EXPECT_TRUE(second_pulled_chunk->num_rows() == limit - chunk_size);
}

} // namespace starrocks::pipeline