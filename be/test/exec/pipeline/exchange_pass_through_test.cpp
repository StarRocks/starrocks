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

class ExchangePassThroughTest : public ::testing::Test {
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
        TPlanFragmentDestination destination;
        destination.__set_fragment_instance_id(_fragment_id);
        destination.__set_brpc_server(address);
        _destinations = {destination};

        _sink_buffer = std::make_shared<SinkBuffer>(_fragment_context.get(), _destinations, /*is_dest_merge*/ false);

        RowDescriptor input_row_desc;
        _recvr = _exec_env->stream_mgr()->create_recvr(
                _runtime_state.get(), input_row_desc, _fragment_id, 0, 1, config::exchg_node_buffer_size_bytes,
                _dest_node_id, std::make_shared<QueryStatisticsRecvr>(),
                /*is_pipeline*/ true, _degree_of_parallelism, /*keep_order*/ false);
        std::stringstream ss;
        ss << "exchange (id=" << _dest_node_id << ")";
        auto runtime_profile = std::make_shared<RuntimeProfile>(ss.str());
        runtime_profile->set_metadata(_dest_node_id);
        _recvr->bind_profile(0, runtime_profile);

        _exchange_sink_factory = std::make_shared<ExchangeSinkOperatorFactory>(
                0, 0, _sink_buffer, TPartitionType::UNPARTITIONED, _destinations, /*is_pipeline_level_shuffle*/ false,
                /*dest_dop*/ 1,
                /*sender_id*/ 0, _dest_node_id, /*partition_exprs*/ std::vector<ExprContext*>(),
                /*enable_exchange_pass_through*/ true, /*enable_exchange_perf*/ false, _fragment_context.get(),
                /*output_columns*/ std::vector<int32_t>(), std::vector<TBucketProperty>());
        _exchange_sink_factory->set_runtime_state(_runtime_state.get());
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

    std::vector<TPlanFragmentDestination> _destinations;
    int32_t _dest_node_id = 0;
    int32_t _degree_of_parallelism = 1;

    size_t _chunk_size = 4096;
    AutoIncChunkBuilder _chunk_builder{_chunk_size};
};

TEST_F(ExchangePassThroughTest, test_exchange_pass_through) {
    int32_t driver_sequence = 0;
    auto exchange_sink = _exchange_sink_factory->create(_degree_of_parallelism, driver_sequence);
    exchange_sink->prepare(_runtime_state.get());

    size_t sent_bytes = 0;
    size_t chunk_bytes = _chunk_builder._chunk_size * 8;
    // data is batched up to max_transmit_batched_bytes. Until then no data is actually sent.
    while (sent_bytes + chunk_bytes < config::max_transmit_batched_bytes) {
        sent_bytes += chunk_bytes;
        exchange_sink->push_chunk(_runtime_state.get(), _chunk_builder.get_next());
        std::unique_ptr<Chunk> received_chunk = nullptr;
        std::ignore = _recvr->get_chunk_for_pipeline(&received_chunk, driver_sequence);
        EXPECT_TRUE(received_chunk == nullptr);
    }

    // once the sent bytes exceeds max_transmit_batched_bytes, the data is sent.
    exchange_sink->push_chunk(_runtime_state.get(), _chunk_builder.get_next());
    std::unique_ptr<Chunk> received_chunk = nullptr;
    std::ignore = _recvr->get_chunk_for_pipeline(&received_chunk, driver_sequence);
    EXPECT_TRUE(received_chunk != nullptr);

    // sending chunks without consuming leads to a full sink buffer.
    while (!_sink_buffer->is_full()) {
        exchange_sink->push_chunk(_runtime_state.get(), _chunk_builder.get_next());
    }

    // receiver ready to consume the data.
    EXPECT_TRUE(_recvr->has_output_for_pipeline(driver_sequence));

    // consuming chunks on the reciever side automatically relieves pressure on the sink side.
    do {
        std::ignore = _recvr->get_chunk_for_pipeline(&received_chunk, driver_sequence);
    } while (received_chunk != nullptr);
    EXPECT_FALSE(_sink_buffer->is_full());

    exchange_sink->close(_runtime_state.get());
}

TEST_F(ExchangePassThroughTest, recv_closed_1) {
    int32_t driver_sequence = 0;
    auto exchange_sink = _exchange_sink_factory->create(_degree_of_parallelism, driver_sequence);
    ASSERT_OK(exchange_sink->prepare(_runtime_state.get()));

    while (!_sink_buffer->is_full()) {
        ASSERT_OK(exchange_sink->push_chunk(_runtime_state.get(), _chunk_builder.get_next()));
    }

    ASSERT_OK(exchange_sink->set_finishing(_runtime_state.get()));

    std::thread thr([recvr = _recvr]() { recvr->close(); });
    thr.join();
    exchange_sink->close(_runtime_state.get());
}

TEST_F(ExchangePassThroughTest, recv_closed_2) {
    int32_t driver_sequence = 0;
    auto exchange_sink = _exchange_sink_factory->create(_degree_of_parallelism, driver_sequence);
    ASSERT_OK(exchange_sink->prepare(_runtime_state.get()));

    while (!_sink_buffer->is_full()) {
        ASSERT_OK(exchange_sink->push_chunk(_runtime_state.get(), _chunk_builder.get_next()));
    }

    std::thread thr([recvr = _recvr]() { recvr->close(); });
    thr.join();

    ASSERT_OK(exchange_sink->set_finishing(_runtime_state.get()));
    exchange_sink->close(_runtime_state.get());
}

} // namespace starrocks::pipeline