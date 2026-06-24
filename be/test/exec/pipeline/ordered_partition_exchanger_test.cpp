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
#include <vector>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum.h"
#include "exec/chunk_buffer_memory_manager.h"
#include "exec/pipeline/exchange/local_exchange.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/query_context.h"
#include "exprs/expr.h"
#include "gen_cpp/Exprs_types.h"
#include "gutil/casts.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "testutil/column_test_helper.h"
#include "testutil/exprs_test_helper.h"
#include "types/logical_type.h"

namespace starrocks::pipeline {

// The partition key lives in slot id 1, matching the SlotRef partition expr below.
static constexpr SlotId kPartitionSlotId = 1;

class OrderedPartitionExchangerTest : public ::testing::Test {
public:
    void SetUp() override {
        _exec_env = ExecEnv::GetInstance();

        _query_context = std::make_shared<QueryContext>();
        _query_context->set_exec_env(_exec_env);
        _query_context->init_mem_tracker(-1, GlobalEnv::GetInstance()->process_mem_tracker());

        TQueryOptions query_options;
        query_options.batch_size = 4096;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(_fragment_id, query_options, query_globals, _exec_env);
        _runtime_state->set_query_ctx(_query_context.get());
        _runtime_state->init_instance_mem_tracker();

        _memory_manager = std::make_shared<ChunkBufferMemoryManager>(_dop, 1024 * 1024);
        _source_op_factory = std::make_unique<LocalExchangeSourceOperatorFactory>(0, 1, _memory_manager);
        _source_op_factory->set_runtime_state(_runtime_state.get());
        for (size_t i = 0; i < _dop; i++) {
            _sources.emplace_back(_source_op_factory->create(_dop, i));
        }

        _exchanger = std::make_unique<OrderedPartitionExchanger>(_memory_manager, _source_op_factory.get(),
                                                                 _make_partition_exprs());
        ASSERT_OK(_exchanger->prepare(_runtime_state.get()));
    }

    void TearDown() override { _exchanger->close(_runtime_state.get()); }

protected:
    LocalExchangeSourceOperator* _source(size_t i) const {
        return down_cast<LocalExchangeSourceOperator*>(_sources[i].get());
    }

    // A single SlotRef(slot 1, INT) partition expression.
    std::vector<ExprContext*> _make_partition_exprs() {
        std::vector<TExpr> t_exprs{
                ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(kPartitionSlotId, /*is_nullable=*/false)};
        std::vector<ExprContext*> exprs;
        CHECK(Expr::create_expr_trees(&_object_pool, t_exprs, &exprs, nullptr).ok());
        return exprs;
    }

    // Build a chunk whose only column (slot 1) holds the given partition-key values.
    static ChunkPtr _make_chunk(const std::vector<int32_t>& keys) {
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnTestHelper::build_column<int32_t>(keys), kPartitionSlotId);
        return chunk;
    }

    // Drain a source channel, returning the partition-key values of every row routed to it, in order.
    std::vector<int32_t> _drain(size_t channel) const {
        std::vector<int32_t> keys;
        while (_source(channel)->has_output()) {
            auto pulled = _source(channel)->pull_chunk(_runtime_state.get());
            CHECK(pulled.ok());
            ChunkPtr chunk = std::move(pulled.value());
            if (chunk == nullptr) {
                break;
            }
            const auto& col = chunk->get_column_by_index(0);
            for (size_t i = 0; i < chunk->num_rows(); ++i) {
                keys.push_back(col->get(i).get_int32());
            }
        }
        return keys;
    }

    std::shared_ptr<ChunkBufferMemoryManager> _memory_manager;
    std::unique_ptr<LocalExchangeSourceOperatorFactory> _source_op_factory;
    std::unique_ptr<OrderedPartitionExchanger> _exchanger;
    TUniqueId _fragment_id;
    ExecEnv* _exec_env = nullptr;
    std::shared_ptr<QueryContext> _query_context;
    std::shared_ptr<RuntimeState> _runtime_state;
    std::vector<OperatorPtr> _sources;
    ObjectPool _object_pool;
    size_t _dop = 3;
};

// Regression for the heap-use-after-free fixed alongside this test: once a chunk is handed downstream, it
// may be mutated on another thread (AnalyticSinkOperator appends window result columns, reallocating the
// chunk's column vector). The exchanger must retain only an owned copy of the previous boundary key, never
// a reference into the handed-off chunk. We deterministically reproduce that hazard by appending a column
// to the already-accepted chunk between two accept() calls; under ASAN this would trip on
// `_previous_chunk->num_rows()` before the fix.
TEST_F(OrderedPartitionExchangerTest, same_partition_continues_after_prev_chunk_appended) {
    auto chunk_a = _make_chunk({7, 7, 7, 7});
    ASSERT_OK(_exchanger->accept(chunk_a, 0));

    // Simulate the downstream operator mutating the already-handed-off chunk: appending a column
    // reallocates chunk_a's `_columns` vector and frees the old storage.
    chunk_a->append_column(ColumnTestHelper::build_column<int32_t>({0, 0, 0, 0}), /*slot_id=*/2);

    // chunk_b's first row continues chunk_a's last partition (7), so it must stay on the same channel.
    auto chunk_b = _make_chunk({7, 7, 7, 7});
    ASSERT_OK(_exchanger->accept(chunk_b, 0));

    // Both chunks land on channel 0 (the first chunk's min-loaded channel); the join is preserved.
    EXPECT_EQ(_drain(0), (std::vector<int32_t>{7, 7, 7, 7, 7, 7, 7, 7}));
    EXPECT_TRUE(_drain(1).empty());
    EXPECT_TRUE(_drain(2).empty());
}

// Same hazard via the other downstream mutation: the analytic LIMIT path calls Chunk::set_num_rows(), which
// resizes every column in place -- including the partition-key column the exchanger used to alias. Caching
// the previous row count (rather than cloning the boundary row) would index past the shrunk column here;
// the owned single-row copy must remain valid and the routing decision must still be correct.
TEST_F(OrderedPartitionExchangerTest, same_partition_continues_after_prev_chunk_truncated) {
    auto chunk_a = _make_chunk({7, 7, 7, 7});
    ASSERT_OK(_exchanger->accept(chunk_a, 0));

    // Simulate the LIMIT path shrinking the already-handed-off chunk in place.
    chunk_a->set_num_rows(2);

    // chunk_b continues chunk_a's partition (7); the boundary key copy (7) must still drive the decision.
    auto chunk_b = _make_chunk({7, 7, 7, 7});
    ASSERT_OK(_exchanger->accept(chunk_b, 0));

    EXPECT_EQ(_drain(0), (std::vector<int32_t>{7, 7, 7, 7, 7, 7}));
    EXPECT_TRUE(_drain(1).empty());
    EXPECT_TRUE(_drain(2).empty());
}

// An empty chunk must be dropped: it carries no rows and no partition boundary, and accepting it must not
// underflow the boundary-row indexing (cur_num_rows - 1). It also must not disturb routing of later chunks.
TEST_F(OrderedPartitionExchangerTest, empty_chunk_is_dropped) {
    ASSERT_OK(_exchanger->accept(_make_chunk({}), 0)); // empty, before any boundary is recorded
    ASSERT_OK(_exchanger->accept(_make_chunk({7, 7, 7, 7}), 0));
    ASSERT_OK(_exchanger->accept(_make_chunk({}), 0));     // empty, after a boundary exists
    ASSERT_OK(_exchanger->accept(_make_chunk({7, 7}), 0)); // continues the same partition

    EXPECT_EQ(_drain(0), (std::vector<int32_t>{7, 7, 7, 7, 7, 7}));
    EXPECT_TRUE(_drain(1).empty());
    EXPECT_TRUE(_drain(2).empty());
}

// The first row of the next chunk starts a brand-new partition, so it is routed to the
// least-loaded channel rather than continuing on the previous channel.
TEST_F(OrderedPartitionExchangerTest, new_partition_goes_to_min_channel) {
    ASSERT_OK(_exchanger->accept(_make_chunk({7, 7, 7, 7}), 0));
    ASSERT_OK(_exchanger->accept(_make_chunk({9, 9, 9, 9}), 0));

    EXPECT_EQ(_drain(0), (std::vector<int32_t>{7, 7, 7, 7}));
    EXPECT_EQ(_drain(1), (std::vector<int32_t>{9, 9, 9, 9}));
    EXPECT_TRUE(_drain(2).empty());
}

// A chunk that begins in the previous partition but switches mid-chunk is split: the leading rows
// continue on the previous channel, the trailing rows of the new partition go to the min channel.
TEST_F(OrderedPartitionExchangerTest, chunk_split_across_partition_boundary) {
    ASSERT_OK(_exchanger->accept(_make_chunk({7, 7, 7, 7}), 0));
    ASSERT_OK(_exchanger->accept(_make_chunk({7, 7, 9, 9}), 0));

    // channel 0: chunk_a (4 rows of 7) + first split part (2 rows of 7).
    EXPECT_EQ(_drain(0), (std::vector<int32_t>{7, 7, 7, 7, 7, 7}));
    // channel 1 (least loaded at split time): the trailing new-partition rows.
    EXPECT_EQ(_drain(1), (std::vector<int32_t>{9, 9}));
    EXPECT_TRUE(_drain(2).empty());
}

} // namespace starrocks::pipeline