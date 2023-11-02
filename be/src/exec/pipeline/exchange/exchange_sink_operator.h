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

#pragma once

#include <memory>
#include <utility>

#include "column/column.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/data_sink.h"
#include "exec/pipeline/exchange/shuffler.h"
#include "exec/pipeline/exchange/sink_buffer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "gen_cpp/data.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "serde/protobuf_serde.h"
#include "util/raw_container.h"
#include "util/runtime_profile.h"

namespace butil {
class IOBuf;
}

namespace starrocks {

class BlockCompressionCodec;
class ExprContext;

namespace pipeline {
class SinkBuffer;
class ExchangeSinkOperator final : public Operator {
public:
    ExchangeSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                         const std::shared_ptr<SinkBuffer>& buffer, TPartitionType::type part_type,
                         const std::vector<TPlanFragmentDestination>& destinations, bool is_pipeline_level_shuffle,
                         const int32_t num_shuffles_per_channel, int32_t sender_id, PlanNodeId dest_node_id,
                         const std::vector<ExprContext*>& partition_expr_ctxs, bool enable_exchange_pass_through,
                         bool enable_exchange_perf, FragmentContext* const fragment_ctx,
                         const std::vector<int32_t>& output_columns,
                         const std::vector<int32_t>& _iceberg_bucket_modulus);

    ~ExchangeSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override;

    bool pending_finish() const override;

    Status set_finishing(RuntimeState* state) override;

    Status set_cancelled(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    void update_metrics(RuntimeState* state) override;

    // For the first chunk , serialize the chunk data and meta to ChunkPB both.
    // For other chunk, only serialize the chunk data to ChunkPB.
    Status serialize_chunk(const Chunk* chunk, ChunkPB* dst, bool* is_first_chunk, int num_receivers = 1);

    // Return the physical bytes of attachment.
    int64_t construct_brpc_attachment(const PTransmitChunkParamsPtr& _chunk_request, butil::IOBuf& attachment);

private:
    bool _is_large_chunk(size_t sz) const {
        // ref olap_scan_node.cpp release_large_columns
        return sz > runtime_state()->chunk_size() * 512;
    }

private:
    class Channel;

    static const int32_t DEFAULT_DRIVER_SEQUENCE = 0;

    const std::shared_ptr<SinkBuffer>& _buffer;

    const TPartitionType::type _part_type;

    const std::vector<TPlanFragmentDestination> _destinations;
    // If the pipeline of dest be is ExchangeSourceOperator -> AggregateBlockingSinkOperator(with group by)
    // then we shuffle for different parallelism at sender side(ExchangeSinkOperator) if _is_pipeline_level_shuffle is true
    bool _is_pipeline_level_shuffle;
    // Degree of pipeline level shuffle
    // - If _is_pipeline_level_shuffle is false, it is set to 1.
    // - If _is_pipeline_level_shuffle is true,
    //      - if each channel is bound to a specific driver sequence, it it set to 1.
    //      - otherwise, it is equal with dop of dest pipeline.
    int32_t _num_shuffles_per_channel;
    int32_t _num_shuffles = 0;
    // Channel is bound to the specific driver sequence only for the right exchange of local bucket shuffle join.
    // The shuffle way of right exchange must be identical to the bucket distribution of the left table,
    // so each bucket has a corresponding channel.
    // Because each tablet bucket of the left table is assigned to a scan operator with the specific driver sequence,
    // each channel of the right exchange must be bound to the same driver sequence.
    //
    // For example, assume that there are 5 tablet buckets and pipeline_dop is 3.
    //                                   ┌───────────┐                                                ┌──────────┐
    //                       ┌──────────►│Join Probe │◄───────────┐                  ┌──────────────► │Join Build│◄────────────────┐
    //                       │           └───────────┘            │                  │                └──────────┘                 │
    //                       │                  ▲                 │                  │                       ▲                     │
    //                       │                  │                 │                  │                       │                     │
    //               ┌───────┴──────┐   ┌───────┴──────┐  ┌───────┴──────┐  ┌────────┴───────┐      ┌────────┴───────┐  ┌──────────┴─────┐
    //               │ScanOperator#0│   │ScanOperator#1│  │ScanOperator#2│  │ExchangeSource#0│      │ExchangeSource#1│  │ExchangeSource#2│
    //               └──────────────┘   └──────────────┘  └──────────────┘  └────────────────┘      └────────────────┘  └────────────────┘
    //                                                                               ▲                       ▲                    ▲
    //                                                                               │                       │                    │
    //                                                                          ┌────┴──────┐           ┌────┴──────┐             │
    //                                                                          │           │           │           │             │
    //                                                                    ┌─────┴───┐ ┌─────┴───┐ ┌─────┴───┐ ┌─────┴───┐    ┌────┴────┐
    // Tablet Bucket      0, 2                1, 3                4       │Channel#0│ │Channel#2│ │Channel#1│ │Channel#3│    │Channel#4│
    //                                                                    └─────▲───┘ └─────▲───┘ └────▲────┘ └─────▲───┘    └─────▲───┘
    //                                                                          │           │          │            │              │
    //                                                                          └───────────┴──────────┴──────┬─────┴──────────────┘
    //                                                                                                        │
    //                                                                                                 ┌──────┴───────┐
    //                                                                                                 │ ExchangeSink │
    //                                                                                                 └──────────────┘
    bool _is_channel_bound_driver_sequence = false;

    // Sender instance id, unique within a fragment.
    const int32_t _sender_id;
    const PlanNodeId _dest_node_id;
    int32_t _encode_level = 0;
    // Will set in prepare
    int32_t _be_number = 0;
    phmap::flat_hash_map<int64_t, std::unique_ptr<Channel>> _instance_id2channel;
    std::vector<Channel*> _channels;
    // index list for channels
    // We need a random order of sending channels to avoid rpc blocking at the same time.
    // But we can't change the order in the vector<channel> directly,
    // because the channel is selected based on the hash pattern,
    // so we pick a random order for the index
    std::vector<int> _channel_indices;
    // Index of current channel to send to if _part_type == RANDOM.
    int32_t _curr_random_channel_idx = 0;

    // Only used when broadcast
    PTransmitChunkParamsPtr _chunk_request;
    size_t _current_request_bytes = 0;

    bool _is_first_chunk = true;

    // String to write compressed chunk data in serialize().
    // This is a string so we can swap() with the string in the ChunkPB we're serializing
    // to (we don't compress directly into the ChunkPB in case the compressed data is
    // longer than the uncompressed data).
    raw::RawString _compression_scratch;

    CompressionTypePB _compress_type = CompressionTypePB::NO_COMPRESSION;
    const BlockCompressionCodec* _compress_codec = nullptr;

    RuntimeProfile::Counter* _serialize_chunk_timer = nullptr;
    RuntimeProfile::Counter* _shuffle_hash_timer = nullptr;
    RuntimeProfile::Counter* _shuffle_chunk_append_counter = nullptr;
    RuntimeProfile::Counter* _shuffle_chunk_append_timer = nullptr;
    RuntimeProfile::Counter* _compress_timer = nullptr;
    RuntimeProfile::Counter* _bytes_pass_through_counter = nullptr;
    RuntimeProfile::Counter* _sender_input_bytes_counter = nullptr;
    RuntimeProfile::Counter* _serialized_bytes_counter = nullptr;
    RuntimeProfile::Counter* _compressed_bytes_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _pass_through_buffer_peak_mem_usage = nullptr;

    std::atomic<bool> _is_finished = false;
    std::atomic<bool> _is_cancelled = false;

    // The following fields are for shuffle exchange:
    const std::vector<ExprContext*>& _partition_expr_ctxs; // compute per-row partition values
    Columns _partitions_columns;
    std::vector<uint32_t> _hash_values;
    std::vector<uint32_t> _shuffle_channel_ids;
    std::vector<int> _driver_sequence_per_shuffle;
    // This array record the channel start point in _row_indexes
    // And the last item is the number of rows of the current shuffle chunk.
    // It will easy to get number of rows belong to one channel by doing
    // _channel_row_idx_start_points[i + 1] - _channel_row_idx_start_points[i]
    std::vector<uint32_t> _channel_row_idx_start_points;
    // Record the row indexes for the current shuffle index. Sender will arrange the row indexes
    // according to channels. For example, if there are 3 channels, this _row_indexes will put
    // channel 0's row first, then channel 1's row indexes, then put channel 2's row indexes in
    // the last.
    std::vector<uint32_t> _row_indexes;

    FragmentContext* const _fragment_ctx;

    const std::vector<int32_t>& _output_columns;

    std::vector<int32_t> _iceberg_bucket_modulus;

    std::unique_ptr<Shuffler> _shuffler;

    std::shared_ptr<serde::EncodeContext> _encode_context = nullptr;
};

class ExchangeSinkOperatorFactory final : public OperatorFactory {
public:
    ExchangeSinkOperatorFactory(int32_t id, int32_t plan_node_id, std::shared_ptr<SinkBuffer> buffer,
                                TPartitionType::type part_type,
                                const std::vector<TPlanFragmentDestination>& destinations,
                                bool is_pipeline_level_shuffle, int32_t num_shuffles_per_channel, int32_t sender_id,
                                PlanNodeId dest_node_id, std::vector<ExprContext*> partition_expr_ctxs,
                                bool enable_exchange_pass_through, bool enable_exchange_perf,
                                FragmentContext* const fragment_ctx, std::vector<int32_t> output_columns,
                                std::vector<int32_t> iceberg_bucket_modulus);

    ~ExchangeSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    std::shared_ptr<SinkBuffer> _buffer;
    const TPartitionType::type _part_type;

    const std::vector<TPlanFragmentDestination>& _destinations;
    const bool _is_pipeline_level_shuffle;
    const int32_t _num_shuffles_per_channel;
    int32_t _sender_id;
    const PlanNodeId _dest_node_id;

    // For shuffle exchange
    std::vector<ExprContext*> _partition_expr_ctxs; // compute per-row partition values

    bool _enable_exchange_pass_through;
    bool _enable_exchange_perf;

    FragmentContext* const _fragment_ctx;

    const std::vector<int32_t> _output_columns;

    std::vector<int32_t> _iceberg_bucket_modulus;
};

} // namespace pipeline
} // namespace starrocks
