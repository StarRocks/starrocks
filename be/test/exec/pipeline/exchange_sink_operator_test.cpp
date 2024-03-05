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

#include "exec/pipeline/exchange/exchange_sink_operator.h"

#include <gmock/gmock.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include "exec/pipeline/query_context.h"
#include "testutil/assert.h"

namespace starrocks::pipeline {
class ExchangeSinkOperatorTest : public testing::Test {
protected:
    void SetUp() override {
        TExecPlanFragmentParams _request;

        const auto& params = _request.params;
        const auto& query_id = params.query_id;
        const auto& fragment_id = params.fragment_instance_id;

        pipeline::QueryContext* _query_ctx;

        ExecEnv* _exec_env = ExecEnv::GetInstance();

        _query_ctx = _exec_env->query_context_mgr()->get_or_register(query_id);
        _query_ctx->set_query_id(query_id);
        _query_ctx->init_mem_tracker(GlobalEnv::GetInstance()->query_pool_mem_tracker()->limit(),
                                     GlobalEnv::GetInstance()->query_pool_mem_tracker());

        _fragment_context = _query_ctx->fragment_mgr()->get_or_register(fragment_id);
        _fragment_context->set_query_id(query_id);
        _fragment_context->set_fragment_instance_id(fragment_id);
        _request.query_options.__set_connector_sink_shuffle_buffer_size_mb(32);
        auto rs = std::make_unique<RuntimeState>(_request.params.query_id, _request.params.fragment_instance_id,
                                                 _request.query_options, _request.query_globals, _exec_env);
        rs->set_func_version(0);
        _fragment_context->set_runtime_state(std::move(rs));
        _runtime_state = _fragment_context->runtime_state();
    }

    void TearDown() override {}

    RuntimeState* _runtime_state;
    FragmentContext* _fragment_context;
    ObjectPool _pool;
};

class MockSinkBuffer : public SinkBuffer {
public:
    MockSinkBuffer(FragmentContext* fragment_ctx, const std::vector<TPlanFragmentDestination>& destinations,
                   bool is_dest_merge)
            : SinkBuffer(fragment_ctx, destinations, is_dest_merge) {}

    MOCK_METHOD(Status, add_request, (TransmitChunkInfo & request));
    MOCK_METHOD(bool, is_full, ());
    MOCK_METHOD(void, set_finishing, ());
    MOCK_METHOD(bool, is_finished, ());
    MOCK_METHOD(void, update_profile, (RuntimeProfile * profile));
    MOCK_METHOD(void, cancel_one_sinker, (RuntimeState* const state));
    MOCK_METHOD(void, incr_sinker, (RuntimeState * state));
    MOCK_METHOD(bool, connector_sink_need_scaling, (int64_t buffer_size, int i));
};

using ::testing::Return;
using ::testing::ByMove;
using ::testing::_;

TEST_F(ExchangeSinkOperatorTest, test_push_chunk) {
    std::vector<TPlanFragmentDestination> destinations;
    TPlanFragmentDestination destination;
    destination.__set_fragment_instance_id(_fragment_context->fragment_instance_id());
    destination.__set_pipeline_driver_sequence(0);
    TNetworkAddress network_address;
    network_address.__set_hostname("127.0.0.1");
    network_address.__set_port(9050);
    destination.__set_brpc_server(network_address);
    destinations.emplace_back(destination);
    std::vector<ExprContext*> partition_expr_ctxs;
    std::vector<int32_t> output_columns;
    // auto mock_sink_buffer = std::make_shared<MockSinkBuffer>(_fragment_context, destinations, false);
    auto mock_sink_buffer = std::make_shared<SinkBuffer>(_fragment_context, destinations, false);
    ASSERT_EQ(mock_sink_buffer->connector_sink_need_scaling(0, 0), false);
    PlanNodeId id = 2;
    auto exchange_sink_operator_factory = std::make_shared<ExchangeSinkOperatorFactory>(
            0, 2, std::move(mock_sink_buffer), TPartitionType::RANDOM_SCALE, destinations, false, 0, 0, id,
            std::move(partition_expr_ctxs), false, false, _fragment_context, output_columns);
    exchange_sink_operator_factory->set_runtime_state(_runtime_state);
    auto exchange_sink_operator = exchange_sink_operator_factory->create(0, 0);
    auto chunk = std::make_shared<Chunk>();
    EXPECT_OK(exchange_sink_operator->push_chunk(_runtime_state, chunk));
}
} // namespace starrocks::pipeline