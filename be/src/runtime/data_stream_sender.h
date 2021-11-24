// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/data_stream_sender.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef STARROCKS_BE_RUNTIME_DATA_STREAM_SENDER_H
#define STARROCKS_BE_RUNTIME_DATA_STREAM_SENDER_H

#include <string>
#include <vector>

#include "column/column.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/data_sink.h"
#include "gen_cpp/doris_internal_service.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "util/raw_container.h"
#include "util/runtime_profile.h"

namespace butil {
class IOBuf;
}

namespace starrocks {

class ExprContext;
class RowDescriptor;
class TDataStreamSink;
class TNetworkAddress;
class TPlanFragmentDestination;
class PartitionInfo;
class PartRangeKey;
class MemTracker;
class BlockCompressionCodec;

// Single sender of an m:n data stream.
// Row batch data is routed to destinations based on the provided
// partitioning specification.
// *Not* thread-safe.
//
// TODO: capture stats that describe distribution of rows/data volume
// across channels.
class DataStreamSender final : public DataSink {
public:
    // Construct a sender according to the output specification (sink),
    // sending to the given destinations.
    // Per_channel_buffer_size is the buffer size allocated to each channel
    // and is specified in bytes.
    // The RowDescriptor must live until close() is called.
    // NOTE: supported partition types are UNPARTITIONED (broadcast) and HASH_PARTITIONED
    DataStreamSender(ObjectPool* pool, bool is_vectorized, int sender_id, const RowDescriptor& row_desc,
                     const TDataStreamSink& sink, const std::vector<TPlanFragmentDestination>& destinations,
                     int per_channel_buffer_size, bool send_query_statistics_with_every_batch);
    ~DataStreamSender() override;

    Status init(const TDataSink& thrift_sink) override;

    // Must be called before other API calls, and before the codegen'd IR module is
    // compiled (i.e. in an ExecNode's Prepare() function).
    Status prepare(RuntimeState* state) override;

    // Must be called before Send() or Close(), and after the codegen'd IR module is
    // compiled (i.e. in an ExecNode's Open() function).
    Status open(RuntimeState* state) override;

    // Send a chunk into this sink.
    Status send_chunk(RuntimeState* state, vectorized::Chunk* chunk) override;

    // Flush all buffered data and close all existing channels to destination
    // hosts. Further send() calls are illegal after calling close().
    Status close(RuntimeState* state, Status exec_status) override;

    // For the first chunk , serialize the chunk data and meta to ChunkPB both.
    // For other chunk, only serialize the chunk data to ChunkPB.
    Status serialize_chunk(const vectorized::Chunk* chunk, ChunkPB* dst, bool* is_first_chunk, int num_receivers = 1);

    void construct_brpc_attachment(PTransmitChunkParams* _chunk_request, butil::IOBuf* attachment);

    // Return total number of bytes sent in TRowBatch.data. If batches are
    // broadcast to multiple receivers, they are counted once per receiver.
    [[maybe_unused]] int64_t get_num_data_bytes_sent() const;

    RuntimeProfile* profile() override { return _profile; }

    TPartitionType::type get_partition_type() const { return _part_type; }

    std::vector<ExprContext*>& get_partition_exprs() { return _partition_expr_ctxs; }

    int32_t get_destinations_size() const { return _channels.size(); }

    PlanNodeId get_dest_node_id() const { return _dest_node_id; }

private:
    class Channel;

    bool _is_vectorized;

    // Sender instance id, unique within a fragment.
    int _sender_id;
    int _be_number = 0;

    RuntimeState* _state{};
    ObjectPool* _pool;
    const RowDescriptor& _row_desc;

    int _current_channel_idx; // index of current channel to send to if _random == true

    // If true, this sender has been closed. Not valid to call Send() anymore.
    bool _closed{};

    TPartitionType::type _part_type;
    bool _ignore_not_found;

    // Only used when broadcast
    PTransmitChunkParams _chunk_request;
    size_t _current_request_bytes = 0;
    size_t _request_bytes_threshold = 0;

    std::vector<uint32_t> _hash_values;
    vectorized::Columns _partitions_columns;
    bool _is_first_chunk = true;
    // String to write compressed chunk data in serialize().
    // This is a string so we can swap() with the string in the ChunkPB we're serializing
    // to (we don't compress directly into the ChunkPB in case the compressed data is
    // longer than the uncompressed data).
    raw::RawString _compression_scratch;
    // vector query engine data struct

    std::vector<ExprContext*> _partition_expr_ctxs; // compute per-row partition values

    std::vector<Channel*> _channels;
    // index list for channels
    // We need a random order of sending channels to avoid rpc blocking at the same time.
    // But we can't change the order in the vector<channel> directly,
    // because the channel is selected based on the hash pattern,
    // so we pick a random order for the index
    std::vector<int> _channel_indices;
    std::vector<std::shared_ptr<Channel>> _channel_shared_ptrs;

    // map from range value to partition_id
    // sorted in ascending orderi by range for binary search
    std::vector<PartitionInfo*> _partition_infos;

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

    RuntimeProfile::Counter* _shuffle_dispatch_timer{};
    RuntimeProfile::Counter* _shuffle_hash_timer{};

    // Throughput per total time spent in sender
    RuntimeProfile::Counter* _overall_throughput{};

    // Identifier of the destination plan node.
    PlanNodeId _dest_node_id;
};

} // namespace starrocks

#endif
