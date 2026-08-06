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
#include "compute_env/spill/operator_mem_resource_manager.h"
#include "compute_env/spill/options.h"
#include "exec/pipeline/nljoin/nljoin_context.h"
#include "exec/pipeline/spill_process_channel.h"

// The test object library is built with -fno-access-control, so private members are reachable.
// Scope: operator-side AUTO-spill wiring that needs no spiller or driver scheduling.

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
        // Only used as the Operator base's OperatorRuntimeAccess; spill options stay unset
        _factory = std::make_unique<SpillableNLJoinBuildOperatorFactory>(0, 1, _ctx);
        _op = std::make_unique<SpillableNLJoinBuildOperator>(_factory.get(), /*id=*/0, /*plan_node_id=*/1,
                                                             /*driver_sequence=*/0, _ctx,
                                                             "spillable_nestloop_join_build");
        // prepare() needs a RuntimeState and a spiller, so install the channel by hand
        _ctx->_input_channel.emplace_back(std::make_unique<NJJoinBuildInputChannel>(kChunkSize));
    }

    std::shared_ptr<NLJoinContext> _ctx;
    std::unique_ptr<SpillableNLJoinBuildOperatorFactory> _factory;
    std::unique_ptr<SpillableNLJoinBuildOperator> _op;
};

// Regression: _spill_strategy used to be uninitialized, so it could start in any strategy.
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

// Regression for a production SIGSEGV: the two pipelines close in nondeterministic order, so
// is_finished() could run after SpillProcessChannel::close() moved the channel's spiller away.
TEST_F(SpillableNLJoinBuildOperatorTest, IsFinishedSurvivesNullSpiller) {
    // set_spiller() is never called here, so the operator's own handle stays null
    auto channel = std::make_shared<SpillProcessChannel>();
    _op->set_channel(channel);
    ASSERT_TRUE(_op->_spiller == nullptr);

    EXPECT_FALSE(_op->is_finished());
    EXPECT_EQ(_op->NLJoinBuildOperator::is_finished(), _op->is_finished());

    // close() moves out the channel's spiller; the operator reads its own handle, so nothing changes
    channel->close();
    ASSERT_TRUE(_op->_spiller == nullptr);
    EXPECT_FALSE(_op->is_finished());
    EXPECT_EQ(_op->NLJoinBuildOperator::is_finished(), _op->is_finished());
}

TEST_F(SpillableNLJoinBuildOperatorTest, NeedInputSurvivesNullSpiller) {
    auto channel = std::make_shared<SpillProcessChannel>();
    channel->close();
    _op->set_channel(channel);

    // Flip to SPILL_ALL so need_input() takes the spiller-dependent branch
    _op->set_execute_mode(spill::MEM_RESOURCE_LOW_MEMORY);
    ASSERT_EQ(spill::SpillStrategy::SPILL_ALL, _op->_spill_strategy);

    // No spiller handle -> cannot accept data, and must not crash
    EXPECT_FALSE(_op->need_input());
    EXPECT_FALSE(_op->is_finished());
}

TEST_F(SpillableNLJoinBuildOperatorTest, NeedInputUsesBaseWhenNoSpill) {
    // No spiller on the channel: an unguarded touch on the NO_SPILL path would crash here
    _op->set_channel(std::make_shared<SpillProcessChannel>());
    ASSERT_EQ(spill::SpillStrategy::NO_SPILL, _op->_spill_strategy);

    EXPECT_TRUE(_op->need_input());
    EXPECT_EQ(_op->NLJoinBuildOperator::need_input(), _op->need_input());
}

// The AUTO-spill decision reads revocable_mem_bytes, so the NO_SPILL path must keep it fresh.
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
