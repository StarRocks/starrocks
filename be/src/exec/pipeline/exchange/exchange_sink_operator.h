// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

#include "column/column.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/data_sink.h"
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
    ExchangeSinkOperator(int32_t id, int32_t plan_node_id, const std::shared_ptr<SinkBuffer>& buffer,
                         TPartitionType::type part_type, const std::vector<TPlanFragmentDestination>& destinations,
                         int sender_id, PlanNodeId dest_node_id, const std::vector<ExprContext*>& partition_expr_ctxs);

    ~ExchangeSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override;

    void finish(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

    // For the first chunk , serialize the chunk data and meta to ChunkPB both.
    // For other chunk, only serialize the chunk data to ChunkPB.
    Status serialize_chunk(const vectorized::Chunk* chunk, ChunkPB* dst, bool* is_first_chunk, int num_receivers = 1);

    void construct_brpc_attachment(PTransmitChunkParams* _chunk_request, butil::IOBuf* attachment);

    RuntimeProfile* profile() { return _profile; }

private:
    class Channel;

    const std::shared_ptr<SinkBuffer>& _buffer;

    TPartitionType::type _part_type;

    std::vector<TPlanFragmentDestination> _destinations;

    // Sender instance id, unique within a fragment.
    int _sender_id;

    // Will set in prepare
    int _be_number = 0;

    // Identifier of the destination plan node.
    PlanNodeId _dest_node_id;

    std::vector<std::shared_ptr<Channel>> _channels;

    // Only used when broadcast
    PTransmitChunkParams _chunk_request;
    size_t _current_request_bytes = 0;
    size_t _request_bytes_threshold = 0;

    bool _is_first_chunk = true;

    // String to write compressed chunk data in serialize().
    // This is a string so we can swap() with the string in the ChunkPB we're serializing
    // to (we don't compress directly into the ChunkPB in case the compressed data is
    // longer than the uncompressed data).
    raw::RawString _compression_scratch;

    CompressionTypePB _compress_type = CompressionTypePB::NO_COMPRESSION;
    const BlockCompressionCodec* _compress_codec = nullptr;

    // Because we should close all channels even if fail to close some channel.
    // We use a global _close_status to record the error close status.
    // Only sender will change this value, so no need to use lock to protect it.
    Status _close_status;

    RuntimeProfile* _profile; // Allocated from _pool
    RuntimeProfile::Counter* _serialize_batch_timer;
    RuntimeProfile::Counter* _compress_timer{};
    RuntimeProfile::Counter* _bytes_sent_counter;
    RuntimeProfile::Counter* _uncompressed_bytes_counter{};
    RuntimeProfile::Counter* _ignore_rows{};

    RuntimeProfile::Counter* _send_request_timer{};
    RuntimeProfile::Counter* _wait_response_timer{};
    // Throughput per total time spent in sender
    RuntimeProfile::Counter* _overall_throughput{};

    std::atomic<bool> _is_finished{false};
    std::atomic<bool> _is_cancelled{false};

    // The following fields are for shuffle exchange:
    const std::vector<ExprContext*>& _partition_expr_ctxs; // compute per-row partition values
    vectorized::Columns _partitions_columns;
    std::vector<uint32_t> _hash_values;
    // This array record the channel start point in _row_indexes
    // And the last item is the number of rows of the current shuffle chunk.
    // It will easy to get number of rows belong to one channel by doing
    // _channel_row_idx_start_points[i + 1] - _channel_row_idx_start_points[i]
    std::vector<uint16_t> _channel_row_idx_start_points;
    // Record the row indexes for the current shuffle index. Sender will arrange the row indexes
    // according to channels. For example, if there are 3 channels, this _row_indexes will put
    // channel 0's row first, then channel 1's row indexes, then put channel 2's row indexes in
    // the last.
    std::vector<uint32_t> _row_indexes;
};

class ExchangeSinkOperatorFactory final : public OperatorFactory {
public:
    ExchangeSinkOperatorFactory(int32_t id, int32_t plan_node_id, std::shared_ptr<SinkBuffer> buffer,
                                TPartitionType::type part_type,
                                const std::vector<TPlanFragmentDestination>& destinations, int sender_id,
                                PlanNodeId dest_node_id, std::vector<ExprContext*> partition_expr_ctxs)
            : OperatorFactory(id, "exchange_sink", plan_node_id),
              _buffer(std::move(buffer)),
              _part_type(part_type),
              _destinations(destinations),
              _sender_id(sender_id),
              _dest_node_id(dest_node_id),
              _partition_expr_ctxs(std::move(partition_expr_ctxs)) {}

    ~ExchangeSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    Status prepare(RuntimeState* state, MemTracker* mem_tracker) override;

    void close(RuntimeState* state) override;

private:
    std::shared_ptr<SinkBuffer> _buffer;

    TPartitionType::type _part_type;

    const std::vector<TPlanFragmentDestination>& _destinations;

    // Sender instance id, unique within a fragment.
    int _sender_id;

    // Identifier of the destination plan node.
    PlanNodeId _dest_node_id;

    // For shuffle exchange
    std::vector<ExprContext*> _partition_expr_ctxs; // compute per-row partition values
};

} // namespace pipeline
} // namespace starrocks
