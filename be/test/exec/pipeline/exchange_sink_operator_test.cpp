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

#include <gtest/gtest.h>

#include "base/compression/block_compression.h"
#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_network_fwd.h"
#include "common/system/backend_options.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/query_context.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Partitions_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/data.pb.h"
#include "gutil/casts.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/column_test_helper.h"

namespace starrocks::pipeline {

// Mock codec that always reports exceed_max_input_size for any non-zero payload.
class AlwaysOverflowCodec final : public BlockCompressionCodec {
public:
    AlwaysOverflowCodec() : BlockCompressionCodec(LZ4) {}

    Status compress(const Slice& input, Slice* output, bool use_compression_buffer, size_t uncompressed_size,
                    faststring* compressed_body1, raw::RawString* compressed_body2,
                    const BlockCompressionOptions& /*options*/) const override {
        return Status::NotSupported("mock");
    }

    Status decompress(const Slice& input, Slice* output) const override { return Status::NotSupported("mock"); }

    size_t max_compressed_len(size_t len) const override { return len; }

    bool exceed_max_input_size(size_t len) const override { return len > 0; }

    size_t max_input_size() const override { return 0; }
};

class ExchangeSinkOperatorTest : public ::testing::Test {
public:
    void SetUp() override {
        BackendOptions::set_localhost("0.0.0.0");

        _exec_env = ExecEnv::GetInstance();

        _query_context = std::make_shared<QueryContext>();
        _query_context->set_query_execution_services(&_exec_env->query_execution_services());
        _query_context->init_mem_tracker(-1, GlobalEnv::GetInstance()->process_mem_tracker());

        TQueryOptions query_options;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(_fragment_id, query_options, query_globals,
                                                        &_exec_env->query_execution_services(), _exec_env);
        _runtime_state->set_query_ctx(_query_context.get());
        _runtime_state->init_instance_mem_tracker();

        _fragment_context = std::make_shared<FragmentContext>();
        _fragment_context->set_fragment_instance_id(_fragment_id);
        _fragment_context->set_runtime_state(std::shared_ptr{_runtime_state});
        _runtime_state->set_fragment_ctx(_fragment_context.get());
        _runtime_state->set_fragment_dict_state(_fragment_context->dict_state());

        TNetworkAddress address;
        address.__set_hostname(BackendOptions::get_local_ip());
        address.__set_port(config::brpc_port);
        // Set lo=-1 so Channel::init skips brpc stub creation (no brpc infra in test env).
        TUniqueId dest_fragment_id;
        dest_fragment_id.__set_lo(-1);
        dest_fragment_id.__set_hi(0);
        _destination.__set_fragment_instance_id(dest_fragment_id);
        _destination.__set_brpc_server(address);

        _destinations = {_destination};
        _sink_buffer = std::make_shared<SinkBuffer>(_fragment_context.get(), _destinations, /*is_dest_merge*/ false);

        _factory = std::make_shared<ExchangeSinkOperatorFactory>(
                0, 0, _sink_buffer, TPartitionType::UNPARTITIONED, _destinations,
                /*is_pipeline_level_shuffle*/ false, /*num_shuffles_per_channel*/ 1,
                /*sender_id*/ 0, /*dest_node_id*/ 0, /*partition_exprs*/ std::vector<ExprContext*>(),
                /*enable_exchange_pass_through*/ false, /*enable_exchange_perf*/ false, _fragment_context.get(),
                /*output_columns*/ std::vector<int32_t>(),
                /*bucket_properties*/ std::vector<TBucketProperty>());
        _factory->set_runtime_state(_runtime_state.get());
    }

    void TearDown() override { _query_context->set_query_execution_services(nullptr); }

    // Build a minimal single-column INT chunk.
    static ChunkPtr make_chunk() {
        auto chunk = std::make_shared<Chunk>();
        auto col = ColumnTestHelper::build_column<int32_t>({1, 2, 3});
        chunk->append_column(std::move(col), 0);
        return chunk;
    }

protected:
    TUniqueId _fragment_id;
    ExecEnv* _exec_env = nullptr;
    std::shared_ptr<QueryContext> _query_context;
    std::shared_ptr<RuntimeState> _runtime_state;
    std::shared_ptr<FragmentContext> _fragment_context;
    std::vector<TPlanFragmentDestination> _destinations;
    std::shared_ptr<SinkBuffer> _sink_buffer;
    std::shared_ptr<ExchangeSinkOperatorFactory> _factory;
    TPlanFragmentDestination _destination;

    AlwaysOverflowCodec _overflow_codec;
};

// When enable_rpc_compress_overflow_skip=true and the codec reports overflow,
// serialize_chunk should succeed (skip compression rather than error).
TEST_F(ExchangeSinkOperatorTest, serialize_chunk_overflow_skip_enabled) {
    auto op = _factory->create(1, 0);
    ASSERT_OK(op->prepare_local_state(_runtime_state.get()));

    auto* sink = down_cast<ExchangeSinkOperator*>(op.get());
    sink->set_compress_codec_for_testing(&_overflow_codec);

    bool prev = config::enable_rpc_compress_overflow_skip;
    DeferOp restore([&] { config::enable_rpc_compress_overflow_skip = prev; });
    config::enable_rpc_compress_overflow_skip = true;

    ChunkPB chunk_pb;
    bool is_first = true;
    EXPECT_OK(sink->serialize_chunk(make_chunk().get(), &chunk_pb, &is_first));
    // No compression applied: compress_type stays at default (NO_COMPRESSION).
    EXPECT_EQ(chunk_pb.compress_type(), CompressionTypePB::NO_COMPRESSION);

    op->close(_runtime_state.get());
}

// When enable_rpc_compress_overflow_skip=false and the codec reports overflow,
// serialize_chunk should return InternalError.
TEST_F(ExchangeSinkOperatorTest, serialize_chunk_overflow_skip_disabled) {
    auto op = _factory->create(1, 0);
    ASSERT_OK(op->prepare_local_state(_runtime_state.get()));

    auto* sink = down_cast<ExchangeSinkOperator*>(op.get());
    sink->set_compress_codec_for_testing(&_overflow_codec);

    bool prev = config::enable_rpc_compress_overflow_skip;
    DeferOp restore([&] { config::enable_rpc_compress_overflow_skip = prev; });
    config::enable_rpc_compress_overflow_skip = false;

    ChunkPB chunk_pb;
    bool is_first = true;
    auto st = sink->serialize_chunk(make_chunk().get(), &chunk_pb, &is_first);
    EXPECT_TRUE(st.is_internal_error()) << st.to_string();

    op->close(_runtime_state.get());
}

} // namespace starrocks::pipeline
