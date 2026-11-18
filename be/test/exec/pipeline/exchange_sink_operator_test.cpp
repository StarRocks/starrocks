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

#include "column/binary_column.h"
#include "column/json_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "exec/pipeline/exchange/sink_buffer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/query_context.h"
#include "exprs/expr.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Partitions_types.h"
#include "gen_cpp/Types_types.h"
#include "gutil/casts.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "testutil/assert.h"
#include "testutil/exprs_test_helper.h"
#include "util/defer_op.h"
#include "util/json.h"

namespace starrocks::pipeline {

class ExchangeSinkOperatorTest : public ::testing::Test {
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

        _fragment_context = std::make_shared<FragmentContext>();
        _fragment_context->set_fragment_instance_id(_fragment_id);
        _fragment_context->set_runtime_state(std::shared_ptr<RuntimeState>{_runtime_state});
        _runtime_state->set_fragment_ctx(_fragment_context.get());

        TNetworkAddress address;
        address.__set_hostname(BackendOptions::get_local_ip());
        address.__set_port(config::brpc_port);
        _destination.__set_fragment_instance_id(_fragment_id);
        _destination.__set_brpc_server(address);
    }

protected:
    TUniqueId _fragment_id;
    ExecEnv* _exec_env = nullptr;
    std::shared_ptr<QueryContext> _query_context;
    std::shared_ptr<RuntimeState> _runtime_state;
    std::shared_ptr<FragmentContext> _fragment_context;
    TPlanFragmentDestination _destination;
};

// Scaffolding for the hash-shuffle tests: a HASH_PARTITIONED sink with pipeline-level shuffle to one
// destination with a real instance id (lo=-1 would be treated as an unused bucket-shuffle pseudo
// destination and skipped by the hash-shuffle path), partitioned on the INT column at slot 0.
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
        CHECK(Expr::create_expr_trees(&_pool, {t_expr}, &_partition_exprs, nullptr).ok());
        CHECK(Expr::prepare(_partition_exprs, _state).ok());
        CHECK(Expr::open(_partition_exprs, _state).ok());

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
        Expr::close(_partition_exprs, _state);
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

static ChunkPtr make_small_chunk() {
    auto ints = Int32Column::create();
    ints->append(1);
    ints->append(2);
    ints->append(3);
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(ints), 0);
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
    EXPECT_EQ(buffer_num->value(), 0);
    EXPECT_EQ(buffer_peak_mem->value(), 0);

    // 3 rows never fill a 4096-row chunk, so they stay buffered in the partial chunks.
    ASSERT_OK(sink->push_chunk(_runtime_state.get(), make_small_chunk()));

    // One channel x dest_dop partial chunks at most, at least one of them received rows.
    EXPECT_GE(buffer_num->value(), 1);
    EXPECT_LE(buffer_num->value(), dest_dop);
    EXPECT_GT(buffer_peak_mem->value(), 0);
    EXPECT_EQ(sink->num_buffered_shuffle_rows_for_testing(), 3);

    // Pushing the same rows again lands in the same partial chunks: no new allocation, peak is monotonic.
    const int64_t num_before = buffer_num->value();
    const int64_t peak_before = buffer_peak_mem->value();
    ASSERT_OK(sink->push_chunk(_runtime_state.get(), make_small_chunk()));
    EXPECT_EQ(buffer_num->value(), num_before);
    EXPECT_GE(buffer_peak_mem->value(), peak_before);
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
    EXPECT_EQ(buffer_num->value(), dest_dop);
    EXPECT_GT(peak_counter->current_value(), 0);
    EXPECT_LE(peak_counter->current_value(), bound);
    EXPECT_LE(peak_counter->value(), bound);
    EXPECT_GE(peak_counter->value(), peak_counter->current_value());
    // Far below the default flush threshold: nothing was flushed because of memory.
    EXPECT_EQ(harness.counter("ShuffleBufferFlushCount")->value(), 0);
}

// Drives `num_pushes` full chunks through a sink whose flush threshold is half of one input chunk, so every
// push crosses the threshold and flushes. Checks the invariants shared by both payload kinds.
//
// enable_exchange_perf=true makes the (local) channel drop the data instead of sending it, so the flush
// path runs without any RPC; the flushed rows are still counted by ShuffleBufferFlushRows.
static void run_memory_limited_shuffle(ShuffleSinkHarness& harness, RuntimeState* state, bool json_payload,
                                       int num_pushes) {
    auto* sink = harness.sink();
    const size_t chunk_size = state->chunk_size();
    auto reference = make_payload_chunk(chunk_size, json_payload);
    const int64_t one_chunk_bytes = static_cast<int64_t>(reference->memory_usage());
    const int64_t limit = one_chunk_bytes / 2;

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
        EXPECT_EQ(flush_count->value(), i + 1);
        // ... leaving nothing buffered and the retained memory at or below the low watermark.
        EXPECT_EQ(sink->num_buffered_shuffle_rows_for_testing(), 0);
        EXPECT_LE(peak_counter->current_value(), limit / 2);
        // Nothing lost: every pushed row was flushed.
        EXPECT_EQ(static_cast<size_t>(flush_rows->value()), chunk_size * (i + 1));
    }
    // Bounded: the limit plus what one push can add before the check runs.
    EXPECT_LE(peak_counter->value(), limit + one_chunk_bytes);
}

// JSON payload: the memory is referenced by the rows, so clearing the chunks is enough to get back under
// the low watermark and the partial chunks keep their capacity — no chunk is released or re-allocated.
TEST_F(ExchangeSinkOperatorTest, shuffle_buffer_flush_keeps_capacity_when_reference_memory_dominates) {
    const int32_t dest_dop = 4;
    ShuffleSinkHarness harness(_runtime_state.get(), _fragment_context.get(), _destination, /*dest_lo*/ 3, dest_dop,
                               /*enable_exchange_perf*/ true);
    run_memory_limited_shuffle(harness, _runtime_state.get(), /*json_payload*/ true, /*num_pushes*/ 8);
    EXPECT_EQ(harness.counter("ShuffleBufferReleasedChunks")->value(), 0);
    EXPECT_EQ(harness.counter("ShuffleChunkBufferNum")->value(), dest_dop);
}

// VARCHAR payload: the memory is the containers' capacity, which clearing does not give back, so partial
// chunks must be released to get under the low watermark and are re-allocated later.
TEST_F(ExchangeSinkOperatorTest, shuffle_buffer_flush_releases_capacity_when_container_memory_dominates) {
    const int32_t dest_dop = 4;
    ShuffleSinkHarness harness(_runtime_state.get(), _fragment_context.get(), _destination, /*dest_lo*/ 4, dest_dop,
                               /*enable_exchange_perf*/ true);
    run_memory_limited_shuffle(harness, _runtime_state.get(), /*json_payload*/ false, /*num_pushes*/ 8);
    EXPECT_GT(harness.counter("ShuffleBufferReleasedChunks")->value(), 0);
    EXPECT_GT(harness.counter("ShuffleChunkBufferNum")->value(), dest_dop);
}

// With the session switch off, the partial chunks are never flushed because of memory no matter how
// small the BE threshold is: everything stays buffered until the chunks fill up.
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
    EXPECT_EQ(harness.counter("ShuffleBufferFlushCount")->value(), 0);
    EXPECT_EQ(harness.counter("ShuffleBufferFlushRows")->value(), 0);
    EXPECT_EQ(sink->num_buffered_shuffle_rows_for_testing(), chunk_size * num_pushes);
}

} // namespace starrocks::pipeline
