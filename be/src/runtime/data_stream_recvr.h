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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/data_stream_recvr.h

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

#pragma once

#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "gen_cpp/Types_types.h" // for TUniqueId
#include "runtime/descriptors.h"
#include "runtime/local_pass_through_buffer.h"
#include "runtime/query_statistics.h"
#include "util/runtime_profile.h"

namespace google::protobuf {
class Closure;
} // namespace google::protobuf

namespace starrocks {

class SortedChunksMerger;
class CascadeChunkMerger;

class DataStreamMgr;
class MemTracker;
class RuntimeProfile;
class PTransmitChunkParams;
class SortExecExprs;

// Single receiver of an m:n data stream.
// DataStreamRecvr maintains one or more queues of row batches received by a
// DataStreamMgr from one or more sender fragment instances.
// Receivers are created via DataStreamMgr::CreateRecvr().
// Ownership of a stream recvr is shared between the DataStreamMgr that created it and
// the caller of DataStreamMgr::CreateRecvr() (i.e. the exchange node)
//
// The _is_merging member determines if the recvr merges input streams from different
// sender fragment instances according to a specified sort order.
// If _is_merging = false : Only one batch queue is maintained for row batches from all
// sender fragment instances. These row batches are returned one at a time via
// get_batch().
// If _is_merging is true : One queue is created for the batches from each distinct
// sender. A SortedRunMerger instance must be created via create_merger() prior to
// retrieving any rows from the receiver. Rows are retrieved from the receiver via
// get_next(RowBatch* output_batch, int limit, bool eos). After the final call to
// get_next(), transfer_all_resources() must be called to transfer resources from the input
// batches from each sender to the caller's output batch.
// The receiver sets deep_copy to false on the merger - resources are transferred from
// the input batches from each sender queue to the merger to the output batch by the
// merger itself as it processes each run.
//
// DataStreamRecvr::close() must be called by the caller of CreateRecvr() to remove the
// recvr instance from the tracking structure of its DataStreamMgr in all cases.
class DataStreamRecvr {
public:
    const static int32_t INVALID_DOP_FOR_NON_PIPELINE_LEVEL_SHUFFLE = 0;

public:
    ~DataStreamRecvr();

    Status get_chunk(std::unique_ptr<Chunk>* chunk);
    Status get_chunk_for_pipeline(std::unique_ptr<Chunk>* chunk, const int32_t driver_sequence);

    // Deregister from DataStreamMgr instance, which shares ownership of this instance.
    void close();

    // Create a SortedRunMerger instance to merge rows from multiple sender according to the
    // specified row comparator. Fetches the first batches from the individual sender
    // queues. The exprs used in less_than must have already been prepared and opened.
    Status create_merger(RuntimeState* state, const SortExecExprs* exprs, const std::vector<bool>* is_asc,
                         const std::vector<bool>* is_null_first);
    Status create_merger_for_pipeline(RuntimeState* state, const SortExecExprs* exprs, const std::vector<bool>* is_asc,
                                      const std::vector<bool>* is_null_first);

    // Fill output_batch with the next batch of rows obtained by merging the per-sender
    // input streams. Must only be called if _is_merging is true.
    Status get_next(ChunkPtr* chunk, bool* eos);
    Status get_next_for_pipeline(ChunkPtr* chunk, std::atomic<bool>* eos, bool* should_exit);

    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }
    PlanNodeId dest_node_id() const { return _dest_node_id; }
    const RowDescriptor& row_desc() const { return _row_desc; }

    void add_sub_plan_statistics(const PQueryStatistics& statistics, int sender_id) {
        if (_sub_plan_query_statistics_recvr) {
            _sub_plan_query_statistics_recvr->insert(statistics, sender_id);
        }
    }

    void short_circuit_for_pipeline(const int32_t driver_sequence);

    bool has_output_for_pipeline(const int32_t driver_sequence) const;

    bool is_finished() const;

    bool is_data_ready();

    bool get_encode_level() const { return _encode_level; }

private:
    friend class DataStreamMgr;
    class SenderQueue;
    class NonPipelineSenderQueue;
    class PipelineSenderQueue;

    DataStreamRecvr(DataStreamMgr* stream_mgr, RuntimeState* runtime_state, const RowDescriptor& row_desc,
                    const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int num_senders, bool is_merging,
                    int total_buffer_limit, std::shared_ptr<RuntimeProfile> profile,
                    std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr, bool is_pipeline,
                    int32_t degree_of_parallelism, bool keep_order, PassThroughChunkBuffer* pass_through_chunk_buffer);

    // If receive queue is full, done is enqueue pending, and return with *done is nullptr
    Status add_chunks(const PTransmitChunkParams& request, ::google::protobuf::Closure** done);

    // Indicate that a particular sender is done. Delegated to the appropriate
    // sender queue. Called from DataStreamMgr.
    void remove_sender(int sender_id, int be_number);

    // Empties the sender queues and notifies all waiting consumers of cancellation.
    void cancel_stream();

    // Return true if the addition of a new batch of size 'chunk_size' would exceed the
    // total buffer limit.
    bool exceeds_limit(int chunk_size) { return _num_buffered_bytes + chunk_size > _total_buffer_limit; }

    // DataStreamMgr instance used to create this recvr. (Not owned)
    DataStreamMgr* _mgr;

    // Fragment and node id of the destination exchange node this receiver is used by.
    TUniqueId _fragment_instance_id;
    PlanNodeId _dest_node_id;

    // soft upper limit on the total amount of buffering allowed for this stream across
    // all sender queues. we stop acking incoming data once the amount of buffered data
    // exceeds this value
    size_t _total_buffer_limit;

    // Row schema, copied from the caller of CreateRecvr().
    RowDescriptor _row_desc;

    // True if this reciver merges incoming rows from different senders. Per-sender
    // row batch queues are maintained in this case.
    bool _is_merging;

    // total number of bytes held across all sender queues.
    std::atomic<size_t> _num_buffered_bytes{0};

    // One or more queues of row batches received from senders. If _is_merging is true,
    // there is one SenderQueue for each sender. Otherwise, row batches from all senders
    // are placed in the same SenderQueue. The SenderQueue instances are owned by the
    // receiver and placed in _sender_queue_pool.
    std::vector<SenderQueue*> _sender_queues;

    // SortedChunksMerger merges chunks from different senders.
    std::unique_ptr<SortedChunksMerger> _chunks_merger;
    std::unique_ptr<CascadeChunkMerger> _cascade_merger;

    // Pool of sender queues.
    ObjectPool _sender_queue_pool;

    // Runtime profile storing the counters below.
    std::shared_ptr<RuntimeProfile> _profile;

    // instance profile and mem_tracker
    std::shared_ptr<RuntimeProfile> _instance_profile;
    std::shared_ptr<MemTracker> _query_mem_tracker;
    std::shared_ptr<MemTracker> _instance_mem_tracker;

    // Number of bytes received
    RuntimeProfile::Counter* _bytes_received_counter;
    RuntimeProfile::Counter* _bytes_pass_through_counter;

    // Time series of number of bytes received, samples _bytes_received_counter
    // RuntimeProfile::TimeSeriesCounter* _bytes_received_time_series_counter;
    RuntimeProfile::Counter* _deserialize_chunk_timer;
    RuntimeProfile::Counter* _decompress_chunk_timer;
    RuntimeProfile::Counter* _request_received_counter;

    // Average time of closure stayed in the buffer
    // Formula is: cumulative_time / _degree_of_parallelism, so the estimation may
    // not be that accurate, but enough to expose problems in profile analysis
    RuntimeProfile::Counter* _closure_block_timer;
    RuntimeProfile::Counter* _closure_block_counter;
    RuntimeProfile::Counter* _process_total_timer = nullptr;

    // Total spent for senders putting data in the queue
    // TODO(hcf) remove these two metrics after non-pipeline offlined
    RuntimeProfile::Counter* _sender_total_timer = nullptr;
    RuntimeProfile::Counter* _sender_wait_lock_timer = nullptr;

    RuntimeProfile::Counter* _buffer_unplug_counter = nullptr;

    // Sub plan query statistics receiver.
    std::shared_ptr<QueryStatisticsRecvr> _sub_plan_query_statistics_recvr;
    bool _is_pipeline;
    // Invalid if _is_pipeline is false
    int32_t _degree_of_parallelism;

    // Invalid if _is_pipeline is false
    // Pipeline will send packets out-of-order
    // if _keep_order is set to true, then receiver will keep the order according sequence
    bool _keep_order;
    PassThroughContext _pass_through_context;

    int _encode_level;
};

} // end namespace starrocks
