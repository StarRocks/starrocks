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

#include "exec/pipeline/nljoin/spillable_nljoin_build_operator.h"

#include <gtest/gtest.h>

#include <memory>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "compute_env/spill/options.h"
#include "exec/pipeline/nljoin/nljoin_context.h"

// The test object library is compiled with -fno-access-control, so the tests can reach the
// private members (_spill_strategy, _is_finished, NLJoinContext::_input_channel) directly.
// This covers the operator-side AUTO-spill wiring that needs no spiller or driver scheduling:
// the spill-strategy state machine and the revocable-memory report on the NO_SPILL path.

namespace starrocks::pipeline {

namespace {

constexpr size_t kChunkSize = 4096;

ChunkPtr make_chunk(size_t num_rows) {
    auto chunk = std::make_shared<Chunk>();
    auto column = Int32Column::create();
    column->append_default(num_rows);
    chunk->append_column(std::move(column), 0);
    return chunk;
}

std::shared_ptr<NLJoinContext> make_context() {
    NLJoinContextParams params;
    params.plan_node_id = 1;
    params.rf_hub = nullptr;
    return std::make_shared<NLJoinContext>(std::move(params));
}

} // namespace

class SpillableNLJoinBuildOperatorTest : public testing::Test {
protected:
    void SetUp() override {
        _ctx = make_context();
        // The factory only serves as the Operator base's OperatorRuntimeAccess; its
        // spill options stay unset because the tests never go through factory->create()
        _factory = std::make_unique<SpillableNLJoinBuildOperatorFactory>(0, 1, _ctx);
        _op = std::make_unique<SpillableNLJoinBuildOperator>(_factory.get(), /*id=*/0, /*plan_node_id=*/1,
                                                             /*driver_sequence=*/0, _ctx,
                                                             "spillable_nestloop_join_build");
        // prepare() is not called (it would need a RuntimeState and a spiller), so
        // install the input channel the way NLJoinContext::incr_builder would
        _ctx->_input_channel.emplace_back(std::make_unique<NJJoinBuildInputChannel>(kChunkSize));
    }

    std::shared_ptr<NLJoinContext> _ctx;
    std::unique_ptr<SpillableNLJoinBuildOperatorFactory> _factory;
    std::unique_ptr<SpillableNLJoinBuildOperator> _op;
};

// Regression guard: _spill_strategy used to be an uninitialized member, so a freshly
// created operator could start in an arbitrary strategy. It must start in NO_SPILL.
TEST_F(SpillableNLJoinBuildOperatorTest, SpillStrategyInitializedToNoSpill) {
    EXPECT_EQ(spill::SpillStrategy::NO_SPILL, _op->_spill_strategy);
    EXPECT_TRUE(_op->spillable());
}

TEST_F(SpillableNLJoinBuildOperatorTest, SetExecuteModeFlipsToSpillAll) {
    _op->set_execute_mode(1);
    EXPECT_EQ(spill::SpillStrategy::SPILL_ALL, _op->_spill_strategy);
    // Idempotent on repeated calls
    _op->set_execute_mode(1);
    EXPECT_EQ(spill::SpillStrategy::SPILL_ALL, _op->_spill_strategy);
}

TEST_F(SpillableNLJoinBuildOperatorTest, SetExecuteModeIgnoredAfterFinished) {
    _op->_is_finished = true;
    _op->set_execute_mode(1);
    EXPECT_EQ(spill::SpillStrategy::NO_SPILL, _op->_spill_strategy);
}

// The AUTO-spill decision loop reads revocable_mem_bytes, so the NO_SPILL push path must
// keep it in sync with the input channel's buffered memory.
TEST_F(SpillableNLJoinBuildOperatorTest, PushChunkReportsRevocableMemBytes) {
    EXPECT_EQ(0u, _op->revocable_mem_bytes());

    // The NO_SPILL path never touches RuntimeState or the spill channel
    ASSERT_TRUE(_op->push_chunk(nullptr, make_chunk(100)).ok());
    EXPECT_GT(_op->revocable_mem_bytes(), 0u);
    EXPECT_EQ(_ctx->input_channel(0).memory_usage(), _op->revocable_mem_bytes());

    ASSERT_TRUE(_op->push_chunk(nullptr, make_chunk(kChunkSize)).ok());
    EXPECT_EQ(_ctx->input_channel(0).memory_usage(), _op->revocable_mem_bytes());
}

} // namespace starrocks::pipeline
