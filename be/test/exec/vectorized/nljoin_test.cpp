// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <gtest/gtest.h>

#include "exec/pipeline/crossjoin/cross_join_context.h"
#include "exec/pipeline/crossjoin/cross_join_left_operator.h"
#include "exec/pipeline/crossjoin/nljoin_probe_operator.h"
#include "runtime/descriptors.h"
#include "testutil/assert.h"

namespace starrocks::pipeline {

class NLJoinTest : public ::testing::Test {
public:
    void SetUp() override {
        _expr_col1 = std::make_unique<vectorized::ColumnRef>(TypeDescriptor(TYPE_INT), 0);
        _expr_col2 = std::make_unique<vectorized::ColumnRef>(TypeDescriptor(TYPE_INT), 1);
    }

    std::shared_ptr<RuntimeState> _create_runtime_state() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = 10;
        TQueryGlobals query_globals;
        auto runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
        runtime_state->init_instance_mem_tracker();
        return runtime_state;
    }

    ChunkPtr build_input_chunk() { return nullptr; }

private:
    std::unique_ptr<vectorized::ColumnRef> _expr_col1;
    std::unique_ptr<vectorized::ColumnRef> _expr_col2;
};

TEST_F(NLJoinTest, inner_join) {
    TJoinOp::type join_op = TJoinOp::INNER_JOIN;
    std::vector<ExprContext*> conjuncts;
    std::vector<SlotDescriptor*> slots;
    size_t probe_column_count = 1;
    size_t build_column_count = 1;
    CrossJoinContextParams params;
    auto cross_join_context = std::make_shared<CrossJoinContext>(params);
    auto runtime_state = _create_runtime_state();
    NLJoinProbeOperator nljoin(nullptr, 1, 1, 0, join_op, conjuncts, slots, probe_column_count, build_column_count,
                               cross_join_context);

    nljoin.prepare(runtime_state.get());
    ASSERT_TRUE(nljoin.need_input());
    ChunkPtr input_chunk = build_input_chunk();
    nljoin.push_chunk(runtime_state.get(), input_chunk);

    ChunkPtr output_chunk;
    while (nljoin.has_output()) {
        auto maybe_chunk = nljoin.pull_chunk(runtime_state.get());
        ASSERT_TRUE(maybe_chunk.ok());
        if (!output_chunk) {
            std::swap(output_chunk, maybe_chunk.value());
        } else {
            output_chunk->append(*maybe_chunk.value());
        }
    }
    ASSERT_EQ(10, output_chunk->num_rows());
}

TEST_F(NLJoinTest, left_join) {
    // TODO
}

TEST_F(NLJoinTest, right_join) {
    // TODO
}

} // namespace starrocks::pipeline