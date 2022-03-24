// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <utility>

#include "column/column.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/data_sink.h"
#include "exec/pipeline/exchange/sink_buffer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "gen_cpp/data.pb.h"
#include "gen_cpp/internal_service.pb.h"
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
    ExchangeSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                         const std::shared_ptr<SinkBuffer>& buffer, TPartitionType::type part_type,
                         const std::vector<TPlanFragmentDestination>& destinations, bool is_pipeline_level_shuffle,
                         const int32_t num_shuffles, int32_t sender_id, PlanNodeId dest_node_id,
                         const std::vector<ExprContext*>& partition_expr_ctxs, bool enable_exchange_pass_through,
                         FragmentContext* const fragment_ctx);

    ~ExchangeSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override;

    bool pending_finish() const override;

    Status set_finishing(RuntimeState* state) override;

    Status set_cancelled(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

    // For the first chunk , serialize the chunk data and meta to ChunkPB both.
    // For other chunk, only serialize the chunk data to ChunkPB.
    Status serialize_chunk(const vectorized::Chunk* chunk, ChunkPB* dst, bool* is_first_chunk, int num_receivers = 1);

    void construct_brpc_attachment(PTransmitChunkParamsPtr _chunk_request, butil::IOBuf& attachment);

private:
    class Channel;

    static const int32_t DEFAULT_DRIVER_SEQUENCE = 0;

    const std::shared_ptr<SinkBuffer>& _buffer;

    const TPartitionType::type _part_type;

    const std::vector<TPlanFragmentDestination> _destinations;
    // If the pipeline of dest be is ExchangeSourceOperator -> AggregateBlockingSinkOperator(with group by)
    // then we shuffle for different parallelism at sender side(ExchangeSinkOperator) if _is_pipeline_level_shuffle is true
    const bool _is_pipeline_level_shuffle;
    // Degree of pipeline level shuffle
    // If _is_pipeline_level_shuffle is false, it is set to 1
    // If _is_pipeline_level_shuffle is true, it is equal with dop of dest pipeline
    const int32_t _num_shuffles;
    // Sender instance id, unique within a fragment.
    const int32_t _sender_id;
    const PlanNodeId _dest_node_id;

    // Will set in prepare
    int32_t _be_number = 0;

    std::vector<std::shared_ptr<Channel>> _channels;
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
    size_t _request_bytes_threshold = config::max_transmit_batched_bytes;

    bool _is_first_chunk = true;

    // String to write compressed chunk data in serialize().
    // This is a string so we can swap() with the string in the ChunkPB we're serializing
    // to (we don't compress directly into the ChunkPB in case the compressed data is
    // longer than the uncompressed data).
    raw::RawString _compression_scratch;

    CompressionTypePB _compress_type = CompressionTypePB::NO_COMPRESSION;
    const BlockCompressionCodec* _compress_codec = nullptr;

    RuntimeProfile::Counter* _serialize_batch_timer = nullptr;
    RuntimeProfile::Counter* _shuffle_hash_timer = nullptr;
    RuntimeProfile::Counter* _compress_timer = nullptr;
    RuntimeProfile::Counter* _bytes_sent_counter = nullptr;
    RuntimeProfile::Counter* _bytes_pass_through_counter = nullptr;
    RuntimeProfile::Counter* _uncompressed_bytes_counter = nullptr;

    // Throughput per total time spent in sender
    RuntimeProfile::Counter* _overall_throughput = nullptr;
    RuntimeProfile::Counter* _network_timer = nullptr;

    std::atomic<bool> _is_finished = false;
    std::atomic<bool> _is_cancelled = false;

    // The following fields are for shuffle exchange:
    const std::vector<ExprContext*>& _partition_expr_ctxs; // compute per-row partition values
    vectorized::Columns _partitions_columns;
    std::vector<uint32_t> _hash_values;
    std::vector<uint32_t> _channel_ids;
    std::vector<uint32_t> _driver_sequences;
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
};

class ExchangeSinkOperatorFactory final : public OperatorFactory {
public:
    ExchangeSinkOperatorFactory(int32_t id, int32_t plan_node_id, std::shared_ptr<SinkBuffer> buffer,
                                TPartitionType::type part_type,
                                const std::vector<TPlanFragmentDestination>& destinations,
                                bool is_pipeline_level_shuffle, int32_t num_shuffles, int32_t sender_id,
                                PlanNodeId dest_node_id, std::vector<ExprContext*> partition_expr_ctxs,
                                bool enable_exchange_pass_through, FragmentContext* const fragment_ctx);

    ~ExchangeSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    std::shared_ptr<SinkBuffer> _buffer;
    const TPartitionType::type _part_type;

    const std::vector<TPlanFragmentDestination>& _destinations;
    const bool _is_pipeline_level_shuffle;
    const int32_t _num_shuffles;
    int32_t _sender_id;
    const PlanNodeId _dest_node_id;

    // For shuffle exchange
    std::vector<ExprContext*> _partition_expr_ctxs; // compute per-row partition values

    bool _enable_exchange_pass_through;

    FragmentContext* const _fragment_ctx;
};

} // namespace pipeline
} // namespace starrocks
