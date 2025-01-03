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

#include "runtime/http_result_writer.h"

#include <gtest/gtest.h>

#include <memory>

#include "column/vectorized_fwd.h"
#include "exprs/column_ref.h"
#include "runtime/buffer_control_block.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "testutil/assert.h"
#include "util/uid_util.h"

namespace starrocks {

static ChunkPtr create_chunk(SchemaPtr schema, int col0value, int col1value) {
    std::vector<int> c0v{col0value};
    std::vector<int> c1v{col1value};
    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(c0v.data(), c0v.size() * sizeof(int));
    c1->append_numbers(c1v.data(), c1v.size() * sizeof(int));
    auto chunk = std::shared_ptr<Chunk>(new Chunk({c0, c1}, schema));
    chunk->set_slot_id_to_index(0, 0);
    chunk->set_slot_id_to_index(1, 1);
    return chunk;
}

TEST(HttpResultWriterTest, BasicJsonFormat) {
    // Mock all the necessary data structures
    RuntimeState dummy_state;
    SchemaPtr schema(new Schema());
    auto c0_field = std::make_shared<Field>(0, "c0", TYPE_INT, true);
    auto c1_field = std::make_shared<Field>(1, "c1", TYPE_INT, true);
    schema->append(c0_field);
    schema->append(c1_field);
    auto c0ref = std::make_unique<ColumnRef>(TypeDescriptor(TYPE_INT), 0);
    auto c1ref = std::make_unique<ColumnRef>(TypeDescriptor(TYPE_INT), 1);
    std::vector<std::unique_ptr<ExprContext>> managed_expr_ctxs;
    std::vector<ExprContext*> expr_ctxs;
    // create c0/c1 ExprContext
    managed_expr_ctxs.emplace_back(std::make_unique<ExprContext>(c0ref.get()));
    expr_ctxs.push_back(managed_expr_ctxs.back().get());
    managed_expr_ctxs.emplace_back(std::make_unique<ExprContext>(c1ref.get()));
    expr_ctxs.push_back(managed_expr_ctxs.back().get());
    ASSERT_OK(Expr::prepare(expr_ctxs, &dummy_state));
    ASSERT_OK(Expr::open(expr_ctxs, &dummy_state));

    TUniqueId uuid(generate_uuid());
    RuntimeProfile dummy_profile{"dummy"};
    auto sinker = std::make_unique<BufferControlBlock>(uuid, 100 * 1024);

    // create the writer
    HttpResultWriter writer(sinker.get(), expr_ctxs, &dummy_profile, TResultSinkFormatType::type::JSON);
    writer.init(&dummy_state);

    // generate a chunk with [1,1], check the output json
    {
        auto chunk = create_chunk(schema, 1, 1);
        auto result = writer.process_chunk(chunk.get());
        EXPECT_TRUE(result.ok()) << result.status();
        auto resultPtrs = std::move(result.value());
        EXPECT_EQ(1L, resultPtrs.size());
        EXPECT_EQ(1L, resultPtrs[0]->result_batch.rows.size());
        EXPECT_EQ("{\"data\":[1,1]}\n", resultPtrs[0]->result_batch.rows[0]);
    }
    // generate a chunk with [2,1], check the output json
    {
        auto chunk = create_chunk(schema, 2, 1);
        auto result = writer.process_chunk(chunk.get());
        EXPECT_TRUE(result.ok()) << result.status();
        auto resultPtrs = std::move(result.value());
        EXPECT_EQ(1L, resultPtrs.size());
        EXPECT_EQ(1L, resultPtrs[0]->result_batch.rows.size());
        EXPECT_EQ("{\"data\":[2,1]}\n", resultPtrs[0]->result_batch.rows[0]);
    }
    // generate a chunk with reuse of the result columns
    // `SELECT count(*) as A, count(*) as B FROM t`
    {
        std::vector<int> c0v{10};
        auto c0 = Int32Column::create();
        c0->append_numbers(c0v.data(), c0v.size() * sizeof(int));
        auto chunk = std::shared_ptr<Chunk>(new Chunk({c0, c0}, schema));
        chunk->set_slot_id_to_index(0, 0);
        chunk->set_slot_id_to_index(1, 1);
        auto result = writer.process_chunk(chunk.get());
        EXPECT_TRUE(result.ok()) << result.status();
        auto resultPtrs = std::move(result.value());
        EXPECT_EQ(1L, resultPtrs.size());
        EXPECT_EQ(1L, resultPtrs[0]->result_batch.rows.size());
        EXPECT_EQ("{\"data\":[10,10]}\n", resultPtrs[0]->result_batch.rows[0]);
    }
}
} // namespace starrocks
