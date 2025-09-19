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

#include "column/datum.h"
#include "exec/pipeline/exchange/exchange_sink_operator.h"
#include "exec/pipeline/fragment_context.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Partitions_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/data_stream_recvr.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks::pipeline {

class ExchangeBucketAwareTest : public ::testing::Test {
public:
    void SetUp() override {
        BackendOptions::set_localhost("0.0.0.0");

        _exec_env = ExecEnv::GetInstance();

        _query_context = std::make_shared<QueryContext>();
        _query_context->set_exec_env(_exec_env);
        _query_context->init_mem_tracker(-1, GlobalEnv::GetInstance()->process_mem_tracker());

        TQueryOptions query_options;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(_fragment_id, query_options, query_globals, _exec_env);
        _runtime_state->set_query_ctx(_query_context.get());
        _runtime_state->init_instance_mem_tracker();

        _fragment_context = std::make_shared<pipeline::FragmentContext>();
        _fragment_context->set_fragment_instance_id(_fragment_id);
        _fragment_context->set_runtime_state(std::shared_ptr<RuntimeState>{_runtime_state});
        _runtime_state->set_fragment_ctx(_fragment_context.get());

        TNetworkAddress address;
        address.__set_hostname(BackendOptions::get_local_ip());
        address.__set_port(config::brpc_port);
        _destination.__set_fragment_instance_id(_fragment_id);
        _destination.__set_brpc_server(address);
    }

    void TearDown() override {
        _recvr->close();
        _exec_env->stream_mgr()->close();
        _query_context->set_exec_env(nullptr);
    }

protected:
    TUniqueId _fragment_id;
    ExecEnv* _exec_env;
    std::shared_ptr<QueryContext> _query_context;
    std::shared_ptr<RuntimeState> _runtime_state;
    std::shared_ptr<pipeline::FragmentContext> _fragment_context;

    std::shared_ptr<SinkBuffer> _sink_buffer;
    std::shared_ptr<DataStreamRecvr> _recvr;
    std::shared_ptr<RuntimeProfile> _runtime_profile;
    std::shared_ptr<ExchangeSinkOperatorFactory> _exchange_sink_factory;

    TPlanFragmentDestination _destination;
    int32_t _dest_node_id = 0;
    ObjectPool _object_pool;
};

TEST_F(ExchangeBucketAwareTest, test_exchange_bucket_aware) {
    std::vector<TExprNode> nodes;
    TExprNode node1;
    node1.node_type = TExprNodeType::SLOT_REF;
    node1.type = gen_type_desc(TPrimitiveType::INT);
    node1.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = 1;
    t_slot_ref.tuple_id = 0;
    node1.__set_slot_ref(t_slot_ref);
    node1.is_nullable = true;
    nodes.emplace_back(node1);

    TExpr t_expr;
    t_expr.nodes = nodes;

    std::vector<TExpr> t_conjuncts;
    t_conjuncts.emplace_back(t_expr);
    std::vector<ExprContext*> partition_exprs;
    Expr::create_expr_trees(&_object_pool, t_conjuncts, &partition_exprs, nullptr);
    Expr::prepare(partition_exprs, _runtime_state.get());
    Expr::open(partition_exprs, _runtime_state.get());

    TBucketProperty bucket_prperty = TBucketProperty();
    bucket_prperty.bucket_func = TBucketFunction::MURMUR3_X86_32;
    bucket_prperty.bucket_num = 2;
    auto bucket_properies = std::vector<TBucketProperty>();
    bucket_properies.emplace_back(bucket_prperty);

    // destinations
    std::vector<TPlanFragmentDestination> destinations;
    destinations.assign(3, _destination);

    _sink_buffer = std::make_shared<SinkBuffer>(_fragment_context.get(), destinations, /*is_dest_merge*/ false);

    _exchange_sink_factory = std::make_shared<ExchangeSinkOperatorFactory>(
            0, 0, _sink_buffer, TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED, destinations,
            /*is_pipeline_level_shuffle*/ true,
            /*dest_dop*/ 2,
            /*sender_id*/ 0, _dest_node_id, /*partition_exprs*/ partition_exprs,
            /*enable_exchange_pass_through*/ true, /*enable_exchange_perf*/ false, _fragment_context.get(),
            /*output_columns*/ std::vector<int32_t>(), bucket_properies);
    _exchange_sink_factory->set_runtime_state(_runtime_state.get());

    RowDescriptor input_row_desc;
    _recvr = _exec_env->stream_mgr()->create_recvr(_runtime_state.get(), input_row_desc, _fragment_id, 0, 3,
                                                   config::exchg_node_buffer_size_bytes, _dest_node_id,
                                                   std::make_shared<QueryStatisticsRecvr>(),
                                                   /*is_pipeline*/ true, 2, /*keep_order*/ false);
    std::stringstream ss;
    ss << "exchange (id=" << _dest_node_id << ")";
    auto runtime_profile = std::make_shared<RuntimeProfile>(ss.str());
    runtime_profile->set_metadata(_dest_node_id);
    _recvr->bind_profile(0, runtime_profile);
    _recvr->bind_profile(1, runtime_profile);

    auto exchange_sink = _exchange_sink_factory->create(2, 0);
    ASSERT_OK(exchange_sink->prepare(_runtime_state.get()));

    auto chunk = std::make_shared<Chunk>();
    auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
    col->append_datum(Datum(1));
    col->append_datum(Datum(2));
    col->append_datum(Datum(3));
    col->append_nulls(1);
    col->append_datum(Datum(4));

    std::vector<uint32_t> hash_values(col->size());
    col->murmur_hash3_x86_32(hash_values.data(), 0, col->size());
    int pipe0 = 0;
    for (auto hash : hash_values) {
        if (HashUtil::xorshift32(hash) % 2 == 0) {
            pipe0 += 1;
        }
    }

    chunk->append_column(std::move(col), 1);

    ASSERT_OK(exchange_sink->push_chunk(_runtime_state.get(), chunk));
    ASSERT_OK(exchange_sink->set_finishing(_runtime_state.get()));

    int row_num = 0;

    std::unique_ptr<Chunk> received_chunk = nullptr;
    do {
        std::ignore = _recvr->get_chunk_for_pipeline(&received_chunk, 0);
        if (received_chunk != nullptr) {
            row_num += received_chunk->num_rows();
        }
    } while (received_chunk);

    ASSERT_EQ(pipe0, row_num);

    do {
        std::ignore = _recvr->get_chunk_for_pipeline(&received_chunk, 1);
        if (received_chunk != nullptr) {
            row_num += received_chunk->num_rows();
        }
    } while (received_chunk);

    ASSERT_EQ(chunk->num_rows(), row_num);

    exchange_sink->close(_runtime_state.get());
}

} // namespace starrocks::pipeline