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

#include "exec/pipeline/exchange/multi_cast_local_exchange.h"

#include <gtest/gtest.h>

#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "testutil/sync_point.h"
#include "types/logical_type.h"
#include "util/runtime_profile.h"

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

class InMemoryMultiCastLocalExchangerTest : public ::testing::Test {
public:
    void SetUp() override { dummy_runtime_state.set_chunk_size(4096); }

    void TearDown() override {}

protected:
    RuntimeState dummy_runtime_state;
    RuntimeProfile dummy_runtime_profile{"dummy"};
};

TEST_F(InMemoryMultiCastLocalExchangerTest, test_push_pop) {
    const int32_t consumer_number = 2;
    InMemoryMultiCastLocalExchanger exchanger(&dummy_runtime_state, consumer_number);
    exchanger.open_sink_operator();
    for (int32_t i = 0; i < consumer_number; i++) {
        exchanger.open_source_operator(i);
    }

    AutoIncChunkBuilder builder;
    for (int i = 1; i <= 3; i++) {
        ASSERT_TRUE(exchanger.can_push_chunk());
        ASSERT_OK(exchanger.push_chunk(builder.get_next(), 0));
        ASSERT_EQ(exchanger._current_accumulated_row_size, 4096 * i);
    }
    ASSERT_EQ(exchanger._fast_accumulated_row_size, 0);
    ASSERT_EQ(exchanger._head->accumulated_row_size, 0);
    // too many unconsumed data, can not push any more data.
    ASSERT_FALSE(exchanger.can_push_chunk());

    // pull data by consumer 0, _fast_accumulated_row_size shoule be updated
    ASSERT_TRUE(exchanger.can_pull_chunk(0));
    ASSERT_OK(exchanger.pull_chunk(&dummy_runtime_state, 0));
    ASSERT_EQ(exchanger._fast_accumulated_row_size, 4096);
    ASSERT_EQ(exchanger._head->accumulated_row_size, 0);

    // after the fatest consumer pull data, the exchanger can push data again.
    ASSERT_TRUE(exchanger.can_push_chunk());

    // pull data by consumer 1, the head cell should be advanced.
    ASSERT_TRUE(exchanger.can_pull_chunk(1));
    ASSERT_OK(exchanger.pull_chunk(&dummy_runtime_state, 1));
    ASSERT_EQ(exchanger._fast_accumulated_row_size, 4096);
    ASSERT_EQ(exchanger._head->accumulated_row_size, 4096);

    // pull the res data
    for (int i = 1; i < 3; i++) {
        // after the fatest consumer pull data, _fast_accumulated_row_size should be updated
        ASSERT_TRUE(exchanger.can_pull_chunk(0));
        ASSERT_OK(exchanger.pull_chunk(&dummy_runtime_state, 0));
        ASSERT_EQ(exchanger._fast_accumulated_row_size, 4096 * (i + 1));
        ASSERT_EQ(exchanger._head->accumulated_row_size, 4096 * i);

        // after all consumer pull data, head should be advanced
        ASSERT_TRUE(exchanger.can_pull_chunk(1));
        ASSERT_OK(exchanger.pull_chunk(&dummy_runtime_state, 1));
        ASSERT_EQ(exchanger._fast_accumulated_row_size, 4096 * (i + 1));
        ASSERT_EQ(exchanger._head->accumulated_row_size, 4096 * (i + 1));
    }

    // no new data, consumer can't pull chunk
    ASSERT_FALSE(exchanger.can_pull_chunk(0));

    // close sink, consumer can pull chunk and get eof
    exchanger.close_sink_operator();
    ASSERT_TRUE(exchanger.can_pull_chunk(0));
    auto st = exchanger.pull_chunk(&dummy_runtime_state, 0);
    ASSERT_TRUE(st.status().is_end_of_file());

    for (int i = 0; i < consumer_number; i++) {
        exchanger.close_source_operator(i);
    }
}

} // namespace starrocks::pipeline