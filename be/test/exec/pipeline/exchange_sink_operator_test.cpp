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

#include <brpc/server.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "base/compression/block_compression.h"
#include "base/concurrency/countdown_latch.h"
#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/flat_json/json_flattener.h"
#include "column/json_column.h"
#include "common/brpc/internal_service_recoverable_stub.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_network_fwd.h"
#include "common/runtime_profile.h"
#include "common/system/backend_options.h"
#include "exec/exec_env.h"
#include "exec/pipeline/exchange/sink_buffer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/query_context.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Partitions_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/data.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"
#include "testutil/column_test_helper.h"
#include "testutil/exprs_test_helper.h"
#include "types/json_value.h"

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
        _query_context->init_mem_tracker(-1, RuntimeEnv::GetInstance()->process_mem_tracker());

        TQueryOptions query_options;
        // Use a large query timeout so an in-flight RPC does not complete on its own during the
        // cancellation test; the test relies on cancel_one_sinker() to abort it promptly instead.
        query_options.__set_query_timeout(300);
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(_fragment_id, query_options, query_globals,
                                                        &_exec_env->query_execution_services(), _exec_env);
        _query_context->attach_to_runtime_state(_runtime_state.get());
        _runtime_state->init_instance_mem_tracker();

        _fragment_context = std::make_shared<FragmentContext>();
        _fragment_context->set_fragment_instance_id(_fragment_id);
        _fragment_context->set_runtime_state(std::shared_ptr{_runtime_state});
        _runtime_state->set_fragment_ctx(_fragment_context.get(), &_fragment_context->fragment_runtime_state());
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
    // The largest serialized chunk is recorded for the profile.
    auto* peak_serialized = sink->unique_metrics()->get_counter("PeakSerializedChunkBytes");
    ASSERT_NE(peak_serialized, nullptr);
    EXPECT_EQ(COUNTER_VALUE(peak_serialized), static_cast<int64_t>(chunk_pb.uncompressed_size()));
    EXPECT_GT(COUNTER_VALUE(peak_serialized), 0);

    op->close(_runtime_state.get());
}

// Scaffolding for the hash-shuffle tests: a HASH_PARTITIONED sink with pipeline-level shuffle to one
// real destination (the fixture destination uses lo=-1, which the hash-shuffle path treats as an unused
// bucket-shuffle pseudo destination and skips), partitioned on the INT column at slot 0.
class ShuffleSinkHarness {
public:
    ShuffleSinkHarness(RuntimeState* state, FragmentContext* fragment_ctx,
                       const TPlanFragmentDestination& fixture_destination, int64_t dest_lo, int32_t dest_dop,
                       bool enable_exchange_perf)
            : _state(state), _fragment_ctx(fragment_ctx) {
        TExprNode node;
        node.node_type = TExprNodeType::SLOT_REF;
        node.type = gen_type_desc(TPrimitiveType::INT);
        node.num_children = 0;
        TSlotRef slot_ref;
        slot_ref.slot_id = 0;
        slot_ref.tuple_id = 0;
        node.__set_slot_ref(slot_ref);
        node.is_nullable = false;
        TExpr t_expr;
        t_expr.nodes.emplace_back(node);
        CHECK(ExprFactory::create_expr_trees(&_pool, {t_expr}, &_partition_exprs, nullptr).ok());
        CHECK(ExprExecutor::prepare(_partition_exprs, _state).ok());
        CHECK(ExprExecutor::open(_partition_exprs, _state).ok());

        TUniqueId dest_id;
        dest_id.__set_hi(0);
        dest_id.__set_lo(dest_lo);
        TPlanFragmentDestination destination = fixture_destination;
        destination.__set_fragment_instance_id(dest_id);
        _destinations = {destination};
        _sink_buffer = std::make_shared<SinkBuffer>(_fragment_ctx, _destinations, /*is_dest_merge*/ false);

        // Channel::init() resolves the query's pass-through chunk buffer, which the fragment normally registers.
        _fragment_ctx->prepare_pass_through_chunk_buffer();

        _factory = std::make_shared<ExchangeSinkOperatorFactory>(
                0, 0, _sink_buffer, TPartitionType::HASH_PARTITIONED, _destinations,
                /*is_pipeline_level_shuffle*/ true, /*num_shuffles_per_channel*/ dest_dop,
                /*sender_id*/ 0, /*dest_node_id*/ 0, _partition_exprs,
                /*enable_exchange_pass_through*/ false, enable_exchange_perf, _fragment_ctx,
                /*output_columns*/ std::vector<int32_t>(),
                /*bucket_properties*/ std::vector<TBucketProperty>());
        _factory->set_runtime_state(_state);
        _op = _factory->create(1, 0);
        CHECK(_op->prepare(_state).ok());
        CHECK(_op->prepare_local_state(_state).ok());
    }

    ~ShuffleSinkHarness() {
        // Do not call set_finishing(): it would send an EOS rpc to a destination nobody listens on.
        _op->close(_state);
        _fragment_ctx->destroy_pass_through_chunk_buffer();
        ExprExecutor::close(_partition_exprs, _state);
    }

    ExchangeSinkOperator* sink() { return down_cast<ExchangeSinkOperator*>(_op.get()); }
    RuntimeProfile::Counter* counter(const std::string& name) { return sink()->unique_metrics()->get_counter(name); }
    RuntimeProfile::HighWaterMarkCounter* peak_counter() {
        return dynamic_cast<RuntimeProfile::HighWaterMarkCounter*>(counter("ShuffleChunkBufferPeakMemoryUsage"));
    }

private:
    RuntimeState* _state;
    FragmentContext* _fragment_ctx;
    ObjectPool _pool;
    std::vector<ExprContext*> _partition_exprs;
    std::vector<TPlanFragmentDestination> _destinations;
    std::shared_ptr<SinkBuffer> _sink_buffer;
    std::shared_ptr<ExchangeSinkOperatorFactory> _factory;
    OperatorPtr _op;
};

// INT partition column + one payload column, `num_rows` rows. The JSON payload lives outside the column
// container (reference memory); the VARCHAR payload lives inside it (container memory). The JSON payload is
// made much larger than a JsonValue object so that reference memory clearly dominates the container.
static ChunkPtr make_payload_chunk(size_t num_rows, bool json_payload) {
    auto ints = Int32Column::create();
    auto chunk = std::make_shared<Chunk>();
    for (size_t i = 0; i < num_rows; i++) {
        ints->append(static_cast<int32_t>(i));
    }
    chunk->append_column(std::move(ints), 0);
    const std::string payload(json_payload ? 4000 : 400, 'x');
    if (json_payload) {
        auto jsons = JsonColumn::create();
        for (size_t i = 0; i < num_rows; i++) {
            auto json = JsonValue::parse("{\"key\": \"" + payload + "\", \"n\": 1}");
            CHECK(json.ok());
            jsons->append(std::move(json.value()));
        }
        chunk->append_column(std::move(jsons), 1);
    } else {
        auto strings = BinaryColumn::create();
        for (size_t i = 0; i < num_rows; i++) {
            strings->append(Slice(payload));
        }
        chunk->append_column(std::move(strings), 1);
    }
    return chunk;
}

// Hash shuffle with pipeline-level shuffle buffers rows in one partial chunk per (channel, driver_sequence).
// The profile must report how many of those partial chunks were allocated and the memory they retain.
TEST_F(ExchangeSinkOperatorTest, shuffle_chunk_buffer_counters) {
    const int32_t dest_dop = 2;
    ShuffleSinkHarness harness(_runtime_state.get(), _fragment_context.get(), _destination, /*dest_lo*/ 1, dest_dop,
                               /*enable_exchange_perf*/ false);
    auto* sink = harness.sink();
    auto* buffer_num = harness.counter("ShuffleChunkBufferNum");
    auto* buffer_peak_mem = harness.counter("ShuffleChunkBufferPeakMemoryUsage");
    ASSERT_NE(buffer_num, nullptr);
    ASSERT_NE(buffer_peak_mem, nullptr);
    EXPECT_EQ(COUNTER_VALUE(buffer_num), 0);
    EXPECT_EQ(COUNTER_VALUE(buffer_peak_mem), 0);

    // 3 rows never fill a 4096-row chunk, so they stay buffered in the partial chunks.
    ASSERT_OK(sink->push_chunk(_runtime_state.get(), make_chunk()));

    // One channel x dest_dop partial chunks at most, at least one of them received rows.
    EXPECT_GE(COUNTER_VALUE(buffer_num), 1);
    EXPECT_LE(COUNTER_VALUE(buffer_num), dest_dop);
    EXPECT_GT(COUNTER_VALUE(buffer_peak_mem), 0);
    EXPECT_EQ(sink->num_buffered_shuffle_rows_for_testing(), 3);

    // Pushing the same rows again lands in the same partial chunks: no new allocation, peak is monotonic.
    const int64_t num_before = COUNTER_VALUE(buffer_num);
    const int64_t peak_before = COUNTER_VALUE(buffer_peak_mem);
    ASSERT_OK(sink->push_chunk(_runtime_state.get(), make_chunk()));
    EXPECT_EQ(COUNTER_VALUE(buffer_num), num_before);
    EXPECT_GE(COUNTER_VALUE(buffer_peak_mem), peak_before);
    EXPECT_EQ(sink->num_buffered_shuffle_rows_for_testing(), 6);
}

// When a partial chunk fills up it is serialized and reset with set_num_rows(0): the containers keep
// their capacity but the out-of-line objects (JSON here) are destroyed. The accounted memory must
// follow that, i.e. stay bounded by a couple of full chunks no matter how many chunks flow through.
TEST_F(ExchangeSinkOperatorTest, shuffle_chunk_buffer_counters_follow_reset) {
    const int32_t dest_dop = 2;
    ShuffleSinkHarness harness(_runtime_state.get(), _fragment_context.get(), _destination, /*dest_lo*/ 2, dest_dop,
                               /*enable_exchange_perf*/ false);
    auto* sink = harness.sink();
    const size_t chunk_size = _runtime_state->chunk_size();
    auto reference = make_payload_chunk(chunk_size, /*json_payload*/ true);
    ASSERT_GT(reference->reference_memory_usage(), 0);
    // Two partial chunks (dest_dop) that may each hold up to one full chunk, plus capacity-doubling slack.
    const int64_t bound = static_cast<int64_t>(reference->memory_usage()) * dest_dop * 2;

    auto* buffer_num = harness.counter("ShuffleChunkBufferNum");
    auto* peak_counter = harness.peak_counter();
    ASSERT_NE(buffer_num, nullptr);
    ASSERT_NE(peak_counter, nullptr);

    // Enough pushes for every partial chunk to fill and reset several times; without the reset
    // accounting the current value would grow linearly with the number of pushes and pass `bound`.
    const int num_pushes = 16;
    for (int i = 0; i < num_pushes; i++) {
        ASSERT_OK(sink->push_chunk(_runtime_state.get(), make_payload_chunk(chunk_size, /*json_payload*/ true)));
    }
    EXPECT_EQ(COUNTER_VALUE(buffer_num), dest_dop);
    EXPECT_GT(COUNTER_CURRENT_VALUE(peak_counter), 0);
    EXPECT_LE(COUNTER_CURRENT_VALUE(peak_counter), bound);
    EXPECT_LE(COUNTER_VALUE(peak_counter), bound);
    EXPECT_GE(COUNTER_VALUE(peak_counter), COUNTER_CURRENT_VALUE(peak_counter));
    // Far below the default memory limit: nothing was flushed because of memory.
    EXPECT_EQ(COUNTER_VALUE(harness.counter("ShuffleBufferFlushCount")), 0);
}

// INT partition column + a JSON column holding `num_rows` copies of `json`, flattened on `paths` unless `paths`
// is empty.
static ChunkPtr make_json_chunk(size_t num_rows, const std::string& json, const std::vector<std::string>& paths,
                                const std::vector<LogicalType>& types) {
    auto ints = Int32Column::create();
    auto plain = JsonColumn::create();
    for (size_t i = 0; i < num_rows; i++) {
        ints->append(static_cast<int32_t>(i));
        auto value = JsonValue::parse(json);
        CHECK(value.ok());
        plain->append(std::move(value.value()));
    }
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(ints), 0);
    if (paths.empty()) {
        chunk->append_column(std::move(plain), 1);
        return chunk;
    }
    JsonFlattener flattener(paths, types, false);
    flattener.flatten(plain.get());
    auto flat = JsonColumn::create();
    flat->set_flat_columns(paths, types, flattener.mutable_result());
    CHECK(flat->is_flat_json());
    chunk->append_column(std::move(flat), 1);
    return chunk;
}

// Appending flat JSON of another schema degrades a buffered partial chunk to plain JSON. With numeric flat fields
// the rows already buffered move from the flat sub-columns (container memory only) to JSON values whose payload is
// reference memory, so the accounted memory must count that payload for the old rows, not only for the new ones.
TEST_F(ExchangeSinkOperatorTest, shuffle_chunk_buffer_counters_follow_flat_json_degrade) {
    // Two destination drivers: with a single shuffle the sink sends every chunk as is and buffers nothing.
    ShuffleSinkHarness harness(_runtime_state.get(), _fragment_context.get(), _destination, /*dest_lo*/ 5,
                               /*dest_dop*/ 2, /*enable_exchange_perf*/ false);
    auto* sink = harness.sink();
    auto* peak_counter = harness.peak_counter();
    ASSERT_NE(peak_counter, nullptr);

    std::vector<std::string> paths;
    std::string wide_json = "{";
    for (int i = 0; i < 40; i++) {
        paths.emplace_back("k" + std::to_string(i));
        wide_json += (i == 0 ? "\"" : ", \"") + paths.back() + "\": " + std::to_string(1000000007L * (i + 1));
    }
    wide_json += "}";
    const std::vector<LogicalType> types(paths.size(), TYPE_BIGINT);
    const size_t num_buffered = 1000;
    ASSERT_OK(sink->push_chunk(_runtime_state.get(), make_json_chunk(num_buffered, wide_json, paths, types)));

    // The same rows as plain JSON: the payload every buffered row holds once its partial chunk degrades.
    const auto plain_payload_bytes =
            static_cast<int64_t>(make_json_chunk(num_buffered, wide_json, {}, {})->reference_memory_usage());
    ASSERT_GT(plain_payload_bytes, static_cast<int64_t>(num_buffered * 200));

    ASSERT_OK(sink->push_chunk(_runtime_state.get(), make_json_chunk(10, "{\"n\": 2}", {"n"}, {TYPE_BIGINT})));
    // Still below chunk_size and the memory limit: every row stays buffered, now as plain JSON.
    EXPECT_EQ(COUNTER_VALUE(harness.counter("ShuffleBufferFlushCount")), 0);
    EXPECT_GE(COUNTER_CURRENT_VALUE(peak_counter), plain_payload_bytes);
}

// Drives `num_pushes` full chunks through a sink whose memory limit is half of one input chunk, so every
// push crosses the limit and flushes. Checks the invariants shared by both payload kinds and returns
// the harness for payload-specific assertions.
//
// enable_exchange_perf=true makes the (local) channel drop the data instead of sending it, so the flush
// path runs without any RPC; the flushed rows are still counted by ShuffleBufferFlushRows.
static void run_memory_limited_shuffle(ShuffleSinkHarness& harness, RuntimeState* state, bool json_payload,
                                       int num_pushes, int64_t* limit_out) {
    auto* sink = harness.sink();
    const size_t chunk_size = state->chunk_size();
    auto reference = make_payload_chunk(chunk_size, json_payload);
    const int64_t one_chunk_bytes = static_cast<int64_t>(reference->memory_usage());
    const int64_t limit = one_chunk_bytes / 2;
    *limit_out = limit;

    const int64_t prev_limit = config::exchange_sink_buffer_flush_threshold_bytes;
    config::exchange_sink_buffer_flush_threshold_bytes = limit;
    DeferOp restore_limit([&] { config::exchange_sink_buffer_flush_threshold_bytes = prev_limit; });

    auto* peak_counter = harness.peak_counter();
    auto* flush_count = harness.counter("ShuffleBufferFlushCount");
    auto* flush_rows = harness.counter("ShuffleBufferFlushRows");
    ASSERT_NE(peak_counter, nullptr);
    ASSERT_NE(flush_count, nullptr);
    ASSERT_NE(flush_rows, nullptr);

    for (int i = 0; i < num_pushes; i++) {
        ASSERT_OK(sink->push_chunk(state, make_payload_chunk(chunk_size, json_payload)));
        // Every push adds one chunk (> limit), so every push must have flushed ...
        EXPECT_EQ(COUNTER_VALUE(flush_count), i + 1);
        // ... leaving nothing buffered and the retained memory at or below the low watermark.
        EXPECT_EQ(sink->num_buffered_shuffle_rows_for_testing(), 0);
        EXPECT_LE(COUNTER_CURRENT_VALUE(peak_counter), limit / 2);
        // Nothing lost: every pushed row was flushed.
        EXPECT_EQ(static_cast<size_t>(COUNTER_VALUE(flush_rows)), chunk_size * (i + 1));
    }
    // Bounded: the limit plus what one push can add before the check runs.
    EXPECT_LE(COUNTER_VALUE(peak_counter), limit + one_chunk_bytes);
}

// JSON payload: the memory is referenced by the rows, so clearing the chunks is enough to get back under
// the low watermark and the partial chunks keep their capacity — no chunk is released or re-allocated.
TEST_F(ExchangeSinkOperatorTest, shuffle_buffer_flush_keeps_capacity_when_reference_memory_dominates) {
    const int32_t dest_dop = 4;
    ShuffleSinkHarness harness(_runtime_state.get(), _fragment_context.get(), _destination, /*dest_lo*/ 3, dest_dop,
                               /*enable_exchange_perf*/ true);
    int64_t limit = 0;
    run_memory_limited_shuffle(harness, _runtime_state.get(), /*json_payload*/ true, /*num_pushes*/ 8, &limit);
    EXPECT_EQ(COUNTER_VALUE(harness.counter("ShuffleBufferReleasedChunks")), 0);
    EXPECT_EQ(COUNTER_VALUE(harness.counter("ShuffleChunkBufferNum")), dest_dop);
}

// VARCHAR payload: the memory is the containers' capacity, which clearing does not give back, so the
// largest partial chunks must be released to get under the low watermark and are re-allocated later.
TEST_F(ExchangeSinkOperatorTest, shuffle_buffer_flush_releases_capacity_when_container_memory_dominates) {
    const int32_t dest_dop = 4;
    ShuffleSinkHarness harness(_runtime_state.get(), _fragment_context.get(), _destination, /*dest_lo*/ 4, dest_dop,
                               /*enable_exchange_perf*/ true);
    int64_t limit = 0;
    run_memory_limited_shuffle(harness, _runtime_state.get(), /*json_payload*/ false, /*num_pushes*/ 8, &limit);
    EXPECT_GT(COUNTER_VALUE(harness.counter("ShuffleBufferReleasedChunks")), 0);
    EXPECT_GT(COUNTER_VALUE(harness.counter("ShuffleChunkBufferNum")), dest_dop);
}

// With the session switch off, the partial chunks are never flushed because of memory no matter how
// small the BE limit is: everything stays buffered until the chunks fill up.
TEST_F(ExchangeSinkOperatorTest, shuffle_buffer_flush_disabled_by_session_variable) {
    const int32_t dest_dop = 4;
    ShuffleSinkHarness harness(_runtime_state.get(), _fragment_context.get(), _destination, /*dest_lo*/ 5, dest_dop,
                               /*enable_exchange_perf*/ true);
    auto* sink = harness.sink();
    sink->set_enable_shuffle_buffer_flush_for_testing(false);

    const int64_t prev_limit = config::exchange_sink_buffer_flush_threshold_bytes;
    config::exchange_sink_buffer_flush_threshold_bytes = 1;
    DeferOp restore_limit([&] { config::exchange_sink_buffer_flush_threshold_bytes = prev_limit; });

    const size_t chunk_size = _runtime_state->chunk_size();
    const int num_pushes = 3;
    for (int i = 0; i < num_pushes; i++) {
        // Every partial chunk gets ~chunk_size / dest_dop rows per push, so none of them fills up.
        ASSERT_OK(sink->push_chunk(_runtime_state.get(), make_payload_chunk(chunk_size, /*json_payload*/ false)));
    }
    EXPECT_EQ(COUNTER_VALUE(harness.counter("ShuffleBufferFlushCount")), 0);
    EXPECT_EQ(COUNTER_VALUE(harness.counter("ShuffleBufferFlushRows")), 0);
    EXPECT_EQ(sink->num_buffered_shuffle_rows_for_testing(), chunk_size * num_pushes);
}

// A brpc PInternalService whose transmit_chunk blocks until explicitly released, so the client-side
// RPC stays in-flight. This lets us assert that cancel_one_sinker() aborts outstanding RPCs actively
// rather than waiting for them to drain (which would otherwise take until the RPC timeout).
class HangingInternalService : public starrocks::PInternalService {
public:
    using Latch = CountDownLatch;

    void transmit_chunk(google::protobuf::RpcController* /*controller*/,
                        const starrocks::PTransmitChunkParams* /*request*/, starrocks::PTransmitChunkResult* response,
                        google::protobuf::Closure* done) override {
        received.count_down();
        // Block the handler until the test tears down, keeping the client RPC pending.
        release.wait();
        if (response != nullptr) {
            response->mutable_status()->set_status_code(0);
        }
        done->Run();
    }

    Latch received{1};
    Latch release{1};
};

class SinkBufferCancelTest : public ExchangeSinkOperatorTest {
protected:
    // Build a SinkBuffer with a single real remote destination pointing at the given brpc port.
    std::shared_ptr<SinkBuffer> make_remote_sink_buffer(int port, const TUniqueId& dest_id) {
        TNetworkAddress addr;
        addr.__set_hostname("127.0.0.1");
        addr.__set_port(port);

        TPlanFragmentDestination dest;
        dest.__set_fragment_instance_id(dest_id);
        dest.__set_brpc_server(addr);

        std::vector<TPlanFragmentDestination> destinations{dest};
        return std::make_shared<SinkBuffer>(_fragment_context.get(), destinations, /*is_dest_merge*/ false);
    }

    static TUniqueId make_dest_id(int64_t lo) {
        TUniqueId id;
        id.__set_hi(0);
        id.__set_lo(lo);
        return id;
    }

    static TransmitChunkInfo make_request(const TUniqueId& dest_id, int port,
                                          std::shared_ptr<PInternalService_RecoverableStub> stub) {
        TNetworkAddress addr;
        addr.__set_hostname("127.0.0.1");
        addr.__set_port(port);

        auto params = std::make_shared<PTransmitChunkParams>();
        params->set_eos(false);
        return TransmitChunkInfo{dest_id,        std::move(stub),           std::move(params),
                                 butil::IOBuf(), /*request_byte_size*/ 128, addr};
    }
};

// cancel_one_sinker() must actively cancel in-flight RPCs. We launch an RPC against a server that
// never responds, then cancel and assert the buffer reaches the finished state quickly (i.e. the
// failure callback fired with ECANCELED) rather than blocking until the RPC timeout.
TEST_F(SinkBufferCancelTest, cancel_aborts_inflight_rpc) {
    brpc::Server server;
    HangingInternalService service;
    brpc::ServerOptions options;
    options.num_threads = 2;
    ASSERT_EQ(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server.Start(0, &options), 0);
    const int port = server.listen_address().port;
    DeferOp stop_server([&] {
        // Release the blocked handler and shut down the server.
        service.release.count_down();
        server.Stop(0);
        server.Join();
    });

    auto dest_id = make_dest_id(/*lo*/ 987654321);
    auto buffer = make_remote_sink_buffer(port, dest_id);
    buffer->incr_sinker(_runtime_state.get());

    auto stub = std::make_shared<PInternalService_RecoverableStub>(server.listen_address(), "");
    ASSERT_OK(stub->reset_channel());

    auto request = make_request(dest_id, port, stub);
    ASSERT_OK(buffer->add_request(request));

    // Wait until the server has actually received the RPC, guaranteeing it is in-flight.
    ASSERT_TRUE(service.received.wait_for(std::chrono::seconds(10)));
    EXPECT_FALSE(buffer->is_finished());

    // While the RPC is in flight, its memory is still accounted to the sink buffer and the profile
    // reports the in-flight/queued peaks together with the limit that finally blocks the sinker.
    {
        RuntimeProfile profile("sink_buffer");
        buffer->update_profile(&profile);
        auto* peak_mem = profile.get_counter("SinkBufferPeakMemoryUsage");
        auto* mem_limit = profile.get_counter("SinkBufferMemoryLimit");
        auto* peak_in_flight = profile.get_counter("SinkBufferPeakInFlightRpcNum");
        auto* peak_requests = profile.get_counter("SinkBufferPeakRequestNum");
        ASSERT_NE(peak_mem, nullptr);
        ASSERT_NE(mem_limit, nullptr);
        ASSERT_NE(peak_in_flight, nullptr);
        ASSERT_NE(peak_requests, nullptr);
        EXPECT_EQ(COUNTER_VALUE(peak_mem), 128);
        EXPECT_EQ(COUNTER_VALUE(mem_limit), config::sink_buffer_mem_limit_per_driver);
        EXPECT_EQ(COUNTER_VALUE(peak_in_flight), 1);
        EXPECT_EQ(COUNTER_VALUE(peak_requests), 1);
    }

    const auto cancel_start = std::chrono::steady_clock::now();
    buffer->cancel_one_sinker(_runtime_state.get());

    // The in-flight RPC should be aborted promptly. The query timeout is 300s, so if cancellation
    // did not work this loop would time out here and fail rather than hanging for the full timeout.
    const auto deadline = cancel_start + std::chrono::seconds(30);
    while (!buffer->is_finished() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(buffer->is_finished());

    const auto elapsed =
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - cancel_start);
    EXPECT_LT(elapsed.count(), 30) << "cancellation did not abort the in-flight RPC promptly";
}

// cancel_one_sinker() must be safe when there are no in-flight RPCs registered (the swap-and-reset
// runs against a freshly initialized, empty bthread_id_list).
TEST_F(SinkBufferCancelTest, cancel_with_no_inflight_rpc_is_safe) {
    auto dest_id = make_dest_id(/*lo*/ 123456789);
    // Any port works; no RPC is ever sent in this test.
    auto buffer = make_remote_sink_buffer(/*port*/ 1, dest_id);
    buffer->incr_sinker(_runtime_state.get());

    buffer->cancel_one_sinker(_runtime_state.get());
    EXPECT_TRUE(buffer->is_finished());
}

// Records whether a sink notification arrived while the buffer was already finished, which is what
// the event scheduler needs to move a PENDING_FINISH driver forward.
class FinishedNotifyObserver final : public PipelineObserver {
public:
    explicit FinishedNotifyObserver(const SinkBuffer* buffer) : _buffer(buffer) {}

    void source_trigger() override {}
    void sink_trigger() override {
        if (_buffer->is_finished()) {
            notified_finished = true;
        }
    }
    void cancel_trigger() override {}
    void all_trigger() override {}
    void runtime_filter_timeout_trigger() override {}
    std::string debug_string() const override { return "FinishedNotifyObserver"; }

    std::atomic<bool> notified_finished{false};

private:
    const SinkBuffer* _buffer;
};

// Driver 0 cancels the buffer while another sinker is still inside _try_to_send_rpc(). The cancel
// notification sees that sinker as sending, so the buffer becomes finished only when the sinker leaves.
// That transition must be notified too, otherwise driver 0 stays PENDING_FINISH until the query timeout.
TEST_F(SinkBufferCancelTest, notify_when_last_sender_leaves_after_cancel) {
    auto dest_id = make_dest_id(/*lo*/ 135792468);
    // No RPC is sent: the sinker sees _is_finishing before issuing one.
    auto buffer = make_remote_sink_buffer(/*port*/ 1, dest_id);
    buffer->incr_sinker(_runtime_state.get());
    buffer->incr_sinker(_runtime_state.get());

    _runtime_state->set_enable_event_scheduler(true);
    FinishedNotifyObserver observer(buffer.get());
    buffer->attach_observer(_runtime_state.get(), &observer);

    CountDownLatch entered(1);
    CountDownLatch release(1);
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp clear_sync_point([]() {
        SyncPoint::GetInstance()->ClearCallBack("SinkBuffer::_try_to_send_rpc:after_incr_sending");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    SyncPoint::GetInstance()->SetCallBack("SinkBuffer::_try_to_send_rpc:after_incr_sending", [&](void*) {
        entered.count_down();
        release.wait();
    });

    std::thread sender([&]() {
        auto request = make_request(dest_id, /*port*/ 1, /*stub*/ nullptr);
        EXPECT_OK(buffer->add_request(request));
    });
    DeferOp join_sender([&]() {
        release.count_down();
        if (sender.joinable()) {
            sender.join();
        }
    });
    ASSERT_TRUE(entered.wait_for(std::chrono::seconds(10)));

    buffer->cancel_one_sinker(_runtime_state.get());
    EXPECT_FALSE(buffer->is_finished());
    EXPECT_FALSE(observer.notified_finished);

    release.count_down();
    sender.join();

    EXPECT_TRUE(buffer->is_finished());
    EXPECT_TRUE(observer.notified_finished);
}

} // namespace starrocks::pipeline
