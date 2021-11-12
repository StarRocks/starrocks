// This file is made available under Elastic License 2.0.
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

#ifndef STARROCKS_BE_SRC_RUNTIME_DATA_STREAM_RECVR_H
#define STARROCKS_BE_SRC_RUNTIME_DATA_STREAM_RECVR_H

#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "gen_cpp/Types_types.h" // for TUniqueId
#include "runtime/descriptors.h"
#include "runtime/query_statistics.h"
#include "util/tuple_row_compare.h"

namespace google {
namespace protobuf {
class Closure;
}
} // namespace google

namespace starrocks {

namespace vectorized {
class SortedChunksMerger;
}

class DataStreamMgr;
class MemTracker;
class RowBatch;
class RuntimeProfile;
class PRowBatch;
class PTransmitChunkParams;

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
    ~DataStreamRecvr();

    Status get_chunk(std::unique_ptr<vectorized::Chunk>* chunk);

    // Deregister from DataStreamMgr instance, which shares ownership of this instance.
    void close();

    // Create a SortedRunMerger instance to merge rows from multiple sender according to the
    // specified row comparator. Fetches the first batches from the individual sender
    // queues. The exprs used in less_than must have already been prepared and opened.
    Status create_merger(const SortExecExprs* exprs, const std::vector<bool>* is_asc,
                         const std::vector<bool>* is_null_first);
    Status create_merger_for_pipeline(const SortExecExprs* exprs, const std::vector<bool>* is_asc,
                                      const std::vector<bool>* is_null_first);

    // Fill output_batch with the next batch of rows obtained by merging the per-sender
    // input streams. Must only be called if _is_merging is true.
    Status get_next(vectorized::ChunkPtr* chunk, bool* eos);
    Status get_next_for_pipeline(vectorized::ChunkPtr* chunk, std::atomic<bool>* eos, bool* should_exit);

    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }
    PlanNodeId dest_node_id() const { return _dest_node_id; }
    const RowDescriptor& row_desc() const { return _row_desc; }

    void add_sub_plan_statistics(const PQueryStatistics& statistics, int sender_id) {
        _sub_plan_query_statistics_recvr->insert(statistics, sender_id);
    }

    bool has_output() const;

    bool is_finished() const;

    bool is_data_ready();

private:
    friend class DataStreamMgr;
    class SenderQueue;

    DataStreamRecvr(DataStreamMgr* stream_mgr, RuntimeState* runtime_state, const RowDescriptor& row_desc,
                    const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int num_senders, bool is_merging,
                    int total_buffer_limit, std::shared_ptr<RuntimeProfile> profile,
                    std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr, bool is_pipeline);

    // If receive queue is full, done is enqueue pending, and return with *done is nullptr
    void add_batch(const PRowBatch& batch, int sender_id, int be_number, int64_t packet_seq,
                   ::google::protobuf::Closure** done);

    // If receive queue is full, done is enqueue pending, and return with *done is nullptr
    Status add_chunks(const PTransmitChunkParams& request, ::google::protobuf::Closure** done);

    // Indicate that a particular sender is done. Delegated to the appropriate
    // sender queue. Called from DataStreamMgr.
    void remove_sender(int sender_id, int be_number);

    // Empties the sender queues and notifies all waiting consumers of cancellation.
    void cancel_stream();

    // Return true if the addition of a new batch of size 'batch_size' would exceed the
    // total buffer limit.
    bool exceeds_limit(int batch_size) { return _num_buffered_bytes + batch_size > _total_buffer_limit; }

    // DataStreamMgr instance used to create this recvr. (Not owned)
    DataStreamMgr* _mgr;

    // Fragment and node id of the destination exchange node this receiver is used by.
    TUniqueId _fragment_instance_id;
    PlanNodeId _dest_node_id;

    // soft upper limit on the total amount of buffering allowed for this stream across
    // all sender queues. we stop acking incoming data once the amount of buffered data
    // exceeds this value
    int _total_buffer_limit;

    // Row schema, copied from the caller of CreateRecvr().
    RowDescriptor _row_desc;

    // True if this reciver merges incoming rows from different senders. Per-sender
    // row batch queues are maintained in this case.
    bool _is_merging;

    // total number of bytes held across all sender queues.
    std::atomic<int> _num_buffered_bytes{0};

    // One or more queues of row batches received from senders. If _is_merging is true,
    // there is one SenderQueue for each sender. Otherwise, row batches from all senders
    // are placed in the same SenderQueue. The SenderQueue instances are owned by the
    // receiver and placed in _sender_queue_pool.
    std::vector<SenderQueue*> _sender_queues;

    // vectorized::SortedChunksMerger merges chunks from different senders.
    std::unique_ptr<vectorized::SortedChunksMerger> _chunks_merger;

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

    // Time series of number of bytes received, samples _bytes_received_counter
    // RuntimeProfile::TimeSeriesCounter* _bytes_received_time_series_counter;
    RuntimeProfile::Counter* _deserialize_chunk_meta_timer;
    RuntimeProfile::Counter* _deserialize_row_batch_timer;
    RuntimeProfile::Counter* _decompress_row_batch_timer;
    RuntimeProfile::Counter* _request_received_counter;

    // Time spent waiting until the first batch arrives across all queues.
    // TODO: Turn this into a wall-clock timer.
    RuntimeProfile::Counter* _first_batch_wait_total_timer;

    // Total spent for senders putting data in the queue
    RuntimeProfile::Counter* _sender_total_timer = nullptr;
    RuntimeProfile::Counter* _sender_wait_lock_timer = nullptr;

    // Total time (summed across all threads) spent waiting for the
    // recv buffer to be drained so that new batches can be
    // added. Remote plan fragments are blocked for the same amount of
    // time.
    RuntimeProfile::Counter* _buffer_full_total_timer;

    // Sub plan query statistics receiver.
    std::shared_ptr<QueryStatisticsRecvr> _sub_plan_query_statistics_recvr;

    // Total time spent waiting for data to arrive in the recv buffer
    RuntimeProfile::Counter* _data_arrival_timer;

    bool _is_pipeline;
};

} // end namespace starrocks

#endif // end STARROCKS_BE_SRC_RUNTIME_DATA_STREAM_RECVR_H
