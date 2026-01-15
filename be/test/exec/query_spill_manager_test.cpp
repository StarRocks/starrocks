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

#include "exec/spill/query_spill_manager.h"

#include <gtest/gtest.h>

#include "exec/pipeline/operator.h"
#include "exec/spill/operator_mem_resource_manager.h"
#include "runtime/runtime_state.h"
#include "util/uid_util.h"

namespace starrocks::spill {
using namespace starrocks::pipeline;

class MockedDummyOperatrator final : public pipeline::Operator {
public:
    MockedDummyOperatrator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                           bool spillable, bool releaseable)
            : Operator(factory, id, "mocked_dummy_operator", plan_node_id, false, driver_sequence),
              _spillable(spillable),
              _releaseable(releaseable) {}
    bool spillable() const override { return _spillable; }
    bool releaseable() const override { return _releaseable; }
    // dummy functions:
    bool has_output() const override { return false; }
    bool need_input() const override { return true; }
    bool is_finished() const override { return false; }

    Status prepare(RuntimeState* state) override { return Status::OK(); }
    void close(RuntimeState* state) override {}
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }

private:
    bool _spillable = false;
    bool _releaseable = false;
};

class MockedDummyOperatorFactory final : public pipeline::OperatorFactory {
public:
    MockedDummyOperatorFactory(int32_t id, int32_t plan_node_id, RuntimeState* state, bool spillable = false,
                               bool releaseable = false)
            : pipeline::OperatorFactory(id, "mocked_dummy_operator_factory", plan_node_id),
              _spillable(spillable),
              _releaseable(releaseable) {
        set_runtime_state(state);
    }
    ~MockedDummyOperatorFactory() override = default;
    Status prepare(RuntimeState* state) override { return Status::OK(); }
    void close(RuntimeState* state) override {}
    pipeline::OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<MockedDummyOperatrator>(this, _id, _plan_node_id, driver_sequence, _spillable,
                                                        _releaseable);
    }

private:
    bool _spillable = false;
    bool _releaseable = false;
};

class QuerySpillManagerTest : public ::testing::Test {
public:
    void SetUp() override { dummy_query_id = generate_uuid(); }

    void TearDown() override {}

protected:
    RuntimeState _dummy_state;
    TUniqueId dummy_query_id;
    GlobalSpillManager _global_mgr;
};

TEST_F(QuerySpillManagerTest, test_inc) {
    QuerySpillManager query_spill_manager_1(dummy_query_id, &_global_mgr);

    auto factory_1 = std::make_unique<MockedDummyOperatorFactory>(1, 1, &_dummy_state, true, true);
    auto op = factory_1->create(1, 1);

    OperatorMemoryResourceManager op_mem_res_mgr;
    op_mem_res_mgr.prepare(op.get(), &query_spill_manager_1);
    EXPECT_EQ(_global_mgr.spillable_operators(), 1);

    auto factory_2 = std::make_unique<MockedDummyOperatorFactory>(1, 1, &_dummy_state, false, true);
    auto op2 = factory_2->create(1, 1);
    OperatorMemoryResourceManager op_mem_res_mgr_2;
    op_mem_res_mgr_2.prepare(op2.get(), nullptr);
    EXPECT_EQ(_global_mgr.spillable_operators(), 1);
}

} // namespace starrocks::spill
