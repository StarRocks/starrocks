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

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/except_hash_set.h"
#include "exec/intersect_hash_set.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks {

// Regression coverage for the >4GB serialized-key overflow guards in the set operators
// (except / intersect). It fabricates a single-row chunk whose key column reports a serialized
// size beyond the uint32 (4GB) limit -- via a fake offset, without allocating 4GB -- and drives
// the guarded methods to hit the fail-fast throw / InternalError branches.
class SetHashSetOverflowTest : public ::testing::Test {
protected:
    void SetUp() override {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = 4096;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
        _runtime_state->init_instance_mem_tracker();
    }

    void make_overflow_input(ChunkPtr* chunk, std::vector<ExprContext*>* exprs) {
        auto fake = LargeBinaryColumn::create();
        // One logical element whose length exceeds the uint32 serialize-size limit.
        fake->get_offset().push_back(5ULL * 1024 * 1024 * 1024);

        *chunk = std::make_shared<Chunk>();
        (*chunk)->append_column(std::move(fake), 0);

        auto* col_ref = _pool.add(new ColumnRef(TypeDescriptor(TYPE_VARCHAR), 0));
        auto* ctx = _pool.add(new ExprContext(col_ref));
        ASSERT_TRUE(ctx->prepare(_runtime_state.get()).ok());
        ASSERT_TRUE(ctx->open(_runtime_state.get()).ok());
        exprs->push_back(ctx);
    }

    std::shared_ptr<RuntimeState> _runtime_state;
    ObjectPool _pool;
};

TEST_F(SetHashSetOverflowTest, ExceptBuildSetThrowsOnOverflow) {
    ExceptHashSerializeSet hash_set;
    ASSERT_TRUE(hash_set.init(_runtime_state.get()).ok());
    ExceptBufferState buffer_state;
    ASSERT_TRUE(buffer_state.init(_runtime_state.get()).ok());
    MemPool pool;

    ChunkPtr chunk;
    std::vector<ExprContext*> exprs;
    make_overflow_input(&chunk, &exprs);

    bool threw = false;
    try {
        hash_set.build_set(_runtime_state.get(), chunk, exprs, &pool, &buffer_state);
    } catch (const std::runtime_error& e) {
        threw = true;
        EXPECT_NE(std::string(e.what()).find("exceeds the 4GB"), std::string::npos);
    }
    EXPECT_TRUE(threw);
}

TEST_F(SetHashSetOverflowTest, ExceptEraseDuplicateRowReturnsErrorOnOverflow) {
    ExceptHashSerializeSet hash_set;
    ASSERT_TRUE(hash_set.init(_runtime_state.get()).ok());
    ExceptBufferState buffer_state;
    ASSERT_TRUE(buffer_state.init(_runtime_state.get()).ok());

    ChunkPtr chunk;
    std::vector<ExprContext*> exprs;
    make_overflow_input(&chunk, &exprs);

    auto st = hash_set.erase_duplicate_row(_runtime_state.get(), chunk, exprs, &buffer_state);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("exceeds the 4GB"), std::string::npos);
}

TEST_F(SetHashSetOverflowTest, IntersectBuildSetThrowsOnOverflow) {
    IntersectHashSerializeSet hash_set;
    ASSERT_TRUE(hash_set.init(_runtime_state.get()).ok());
    MemPool pool;

    ChunkPtr chunk;
    std::vector<ExprContext*> exprs;
    make_overflow_input(&chunk, &exprs);

    bool threw = false;
    try {
        hash_set.build_set(_runtime_state.get(), chunk, exprs, &pool);
    } catch (const std::runtime_error& e) {
        threw = true;
        EXPECT_NE(std::string(e.what()).find("exceeds the 4GB"), std::string::npos);
    }
    EXPECT_TRUE(threw);
}

TEST_F(SetHashSetOverflowTest, IntersectRefineIntersectRowReturnsErrorOnOverflow) {
    IntersectHashSerializeSet hash_set;
    ASSERT_TRUE(hash_set.init(_runtime_state.get()).ok());

    ChunkPtr chunk;
    std::vector<ExprContext*> exprs;
    make_overflow_input(&chunk, &exprs);

    auto st = hash_set.refine_intersect_row(_runtime_state.get(), chunk, exprs, 1);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("exceeds the 4GB"), std::string::npos);
}

} // namespace starrocks
