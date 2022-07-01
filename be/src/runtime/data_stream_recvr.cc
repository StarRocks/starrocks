// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/data_stream_recvr.cc

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

#include "runtime/data_stream_recvr.h"

#include <fmt/format.h>
#include <util/time.h>

#include <condition_variable>
#include <deque>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "column/chunk.h"
#include "exec/sort_exec_exprs.h"
#include "gen_cpp/data.pb.h"
#include "runtime/current_thread.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/sorted_chunks_merger.h"
#include "serde/protobuf_serde.h"
#include "util/compression/block_compression.h"
#include "util/debug_util.h"
#include "util/defer_op.h"
#include "util/faststring.h"
#include "util/logging.h"
#include "util/phmap/phmap.h"
#include "util/runtime_profile.h"

using std::list;
using std::vector;
using std::pair;
using std::make_pair;

namespace starrocks {

using vectorized::ChunkUniquePtr;

// Implements a blocking queue of row batches from one or more senders. One queue
// is maintained per sender if _is_merging is true for the enclosing receiver, otherwise
// rows from all senders are placed in the same queue.
class DataStreamRecvr::SenderQueue {
public:
    SenderQueue(DataStreamRecvr* parent_recvr, int32_t num_senders, int32_t degree_of_parallelism);

    ~SenderQueue() = default;

    // Return the next batch form this sender queue. Sets the returned batch in _cur_batch.
    // A returned batch that is not filled to capacity does *not* indicate
    // end-of-stream.
    // The call blocks until another batch arrives or all senders close
    // their channels. The returned batch is owned by the sender queue. The caller
    // must acquire data from the returned batch before the next call to get_batch().
    Status get_chunk(vectorized::Chunk** chunk);

    // Same as get_chunk, but this version will not wait if there is non buffer chunks
    Status get_chunk_for_pipeline(vectorized::Chunk** chunk, const int32_t driver_sequence);

    // check if data has come, work with try_get_chunk.
    bool has_chunk();

    // Probe for chunks, because _chunk_queue maybe empty when data hasn't come yet.
    // So compute thread should do other works.
    bool try_get_chunk(vectorized::Chunk** chunk);

    // Adds a column chunk to this sender queue if this stream has not been cancelled;
    // blocks if this will make the stream exceed its buffer limit.
    // If the total size of the chunks in this queue would exceed the allowed buffer size,
    // the queue is considered full and the call blocks until a chunk is dequeued.
    Status add_chunks(const PTransmitChunkParams& request, ::google::protobuf::Closure** done, bool is_pipeline);

    // add_chunks_and_keep_order is almost the same like add_chunks except that it didn't
    // notify compute thread to grab chunks, compute thread is notified by pipeline's dispatch thread.
    // Process data in strict accordance with the order of the sequence
    Status add_chunks_and_keep_order(const PTransmitChunkParams& request, ::google::protobuf::Closure** done);

    // Decrement the number of remaining senders for this queue and signal eos ("new data")
    // if the count drops to 0. The number of senders will be 1 for a merging
    // DataStreamRecvr.
    void decrement_senders(int be_number);

    // Set cancellation flag and signal cancellation to receiver and sender. Subsequent
    // incoming batches will be dropped.
    void cancel();

    // Must be called once to cleanup any queued resources.
    void close();

    void clean_buffer_queues();

    void short_circuit_for_pipeline(const int32_t driver_sequence);

    bool has_output_for_pipeline(const int32_t driver_sequence);

    bool is_finished() const;

private:
    struct ChunkItem {
        int64_t chunk_bytes = 0;
        // Invalid if SenderQueue::_is_pipeline_level_shuffle is false
        int32_t driver_sequence = -1;
        ChunkUniquePtr chunk_ptr;
        // When the memory of the ChunkQueue exceeds the limit,
        // we have to hold closure of the request, so as not to let the sender continue to send data.
        // A Request may have multiple Chunks, so only when the last Chunk of the Request is consumed,
        // the callback is closed- >run() Let the sender continue to send data
        google::protobuf::Closure* closure = nullptr;
        // Time in nano of saveing closure
        int64_t enter_time;
    };

    Status _build_chunk_meta(const ChunkPB& pb_chunk);
    Status _deserialize_chunk(const ChunkPB& pchunk, vectorized::Chunk* chunk, faststring* uncompressed_buffer);

    // Receiver of which this queue is a member.
    DataStreamRecvr* _recvr;

    // protects all subsequent data.
    mutable std::mutex _lock;

    // if true, the receiver fragment for this stream got cancelled
    bool _is_cancelled;

    // number of senders which haven't closed the channel yet
    // (if it drops to 0, end-of-stream is true)
    int _num_remaining_senders;

    // signal arrival of new batch or the eos/cancelled condition
    std::condition_variable _data_arrival_cv;

    typedef std::list<ChunkItem> ChunkQueue;
    ChunkQueue _chunk_queue;
    bool _is_pipeline_level_shuffle_init = false;
    bool _is_pipeline_level_shuffle = false;
    std::vector<bool> _has_chunks_per_driver_sequence;
    serde::ProtobufChunkMeta _chunk_meta;

    std::unordered_set<int> _sender_eos_set;          // sender_id
    std::unordered_map<int, int64_t> _packet_seq_map; // be_number => packet_seq

    // distribution of received sequence numbers:
    // part1: { sequence | 1 <= sequence <= _max_processed_sequence }
    // part2: { sequence | seq = _max_processed_sequence + i, i > 1 }
    phmap::flat_hash_map<int, int64_t> _max_processed_sequences;
    // chunk request may be out-of-order, but we have to deal with it in order
    // key of first level is be_number
    // key of second level is request sequence
    phmap::flat_hash_map<int, phmap::flat_hash_map<int64_t, ChunkQueue>> _buffered_chunk_queues;

    std::unordered_set<int32_t> _short_circuit_driver_sequences;
};

DataStreamRecvr::SenderQueue::SenderQueue(DataStreamRecvr* parent_recvr, int32_t num_senders,
                                          int32_t degree_of_parallelism)
        : _recvr(parent_recvr),
          _is_cancelled(false),
          _num_remaining_senders(num_senders),
          _has_chunks_per_driver_sequence(degree_of_parallelism, false) {}

void DataStreamRecvr::SenderQueue::short_circuit_for_pipeline(const int32_t driver_sequence) {
    std::lock_guard<std::mutex> l(_lock);
    _short_circuit_driver_sequences.insert(driver_sequence);

    auto iter = _chunk_queue.begin();
    while (iter != _chunk_queue.end()) {
        if (_is_pipeline_level_shuffle && iter->driver_sequence == driver_sequence) {
            if (iter->closure != nullptr) {
                _recvr->_closure_block_timer->update(MonotonicNanos() - iter->enter_time);
                iter->closure->Run();
            }
            iter = _chunk_queue.erase(iter);
        } else {
            ++iter;
        }
    }
}

bool DataStreamRecvr::SenderQueue::has_output_for_pipeline(const int32_t driver_sequence) {
    // First check without lock to avoid competition
    // This method may be invoked by PipelineDriverPoller and GlobalDriverExecutor simultaneously
    // when some driver_sequences has no chunks to get but the others do, then GlobalDriverExecutor
    // may be starved because PipelineDriverPoller will call this method repeatedly with almost no interval
    // Both false positives and false negatives are allowed here
    {
        if (_is_cancelled) {
            return false;
        }
        if (_is_pipeline_level_shuffle && !_has_chunks_per_driver_sequence[driver_sequence]) {
            return false;
        }
        if (_chunk_queue.empty()) {
            return false;
        }
    }

    // Second check under lock
    {
        std::lock_guard<std::mutex> l(_lock);

        if (_is_cancelled) {
            return false;
        }
        if (_is_pipeline_level_shuffle && !_has_chunks_per_driver_sequence[driver_sequence]) {
            return false;
        }

        if (_chunk_queue.empty()) {
            return false;
        }

        for (auto& item : _chunk_queue) {
            if (!_is_pipeline_level_shuffle || item.driver_sequence == driver_sequence) {
                return true;
            }
        }

        if (_is_pipeline_level_shuffle) {
            _has_chunks_per_driver_sequence[driver_sequence] = false;
        }
        return false;
    }
}

bool DataStreamRecvr::SenderQueue::is_finished() const {
    std::lock_guard<std::mutex> l(_lock);
    return _is_cancelled || (_num_remaining_senders == 0 && _chunk_queue.empty());
}

bool DataStreamRecvr::SenderQueue::has_chunk() {
    std::lock_guard<std::mutex> l(_lock);
    if (_is_cancelled) {
        return true;
    }

    if (_chunk_queue.empty() && _num_remaining_senders > 0) {
        return false;
    }

    return true;
}

// try_get_chunk will only be used when has_chunk return true(explicitly or implicitly).
bool DataStreamRecvr::SenderQueue::try_get_chunk(vectorized::Chunk** chunk) {
    std::lock_guard<std::mutex> l(_lock);
    if (_is_cancelled) {
        return false;
    }

    if (_chunk_queue.empty()) {
        DCHECK_EQ(_num_remaining_senders, 0);
        return false;
    } else {
        *chunk = _chunk_queue.front().chunk_ptr.release();
        _recvr->_num_buffered_bytes -= _chunk_queue.front().chunk_bytes;
        auto* closure = _chunk_queue.front().closure;
        auto enter_time = _chunk_queue.front().enter_time;
        VLOG_ROW << "DataStreamRecvr fetched #rows=" << (*chunk)->num_rows();
        _chunk_queue.pop_front();
        if (closure != nullptr) {
            _recvr->_closure_block_timer->update(MonotonicNanos() - enter_time);
            closure->Run();
        }
        return true;
    }
}

Status DataStreamRecvr::SenderQueue::get_chunk(vectorized::Chunk** chunk) {
    std::unique_lock<std::mutex> l(_lock);
    // wait until something shows up or we know we're done
    while (!_is_cancelled && _chunk_queue.empty() && _num_remaining_senders > 0) {
        VLOG_ROW << "wait arrival fragment_instance_id=" << _recvr->fragment_instance_id()
                 << " node=" << _recvr->dest_node_id();
        _data_arrival_cv.wait(l);
    }

    if (_is_cancelled) {
        return Status::Cancelled("Cancelled SenderQueue::get_chunk");
    }

    if (_chunk_queue.empty()) {
        return Status::OK();
    }

    *chunk = _chunk_queue.front().chunk_ptr.release();
    auto* closure = _chunk_queue.front().closure;
    auto enter_time = _chunk_queue.front().enter_time;

    _recvr->_num_buffered_bytes -= _chunk_queue.front().chunk_bytes;
    VLOG_ROW << "DataStreamRecvr fetched #rows=" << (*chunk)->num_rows();
    _chunk_queue.pop_front();

    if (closure != nullptr) {
        _recvr->_closure_block_timer->update(MonotonicNanos() - enter_time);
        // When the execution thread is blocked and the Chunk queue exceeds the memory limit,
        // the execution thread will hold done and will not return, block brpc from sending packets,
        // and the execution thread will call run() to let brpc continue to send packets,
        // and there will be memory release
#ifndef BE_TEST
        MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(ExecEnv::GetInstance()->process_mem_tracker());
        DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });
#endif

        closure->Run();
    }

    return Status::OK();
}

Status DataStreamRecvr::SenderQueue::get_chunk_for_pipeline(vectorized::Chunk** chunk, const int32_t driver_sequence) {
    std::lock_guard<std::mutex> l(_lock);

    if (_is_cancelled) {
        return Status::Cancelled("Cancelled SenderQueue::get_chunk");
    }

    if (_chunk_queue.empty()) {
        return Status::OK();
    }

    auto iter = _chunk_queue.begin();
    while (iter != _chunk_queue.end()) {
        if (!_is_pipeline_level_shuffle || iter->driver_sequence == driver_sequence) {
            *chunk = iter->chunk_ptr.release();
            auto* closure = iter->closure;
            auto enter_time = iter->enter_time;
            _recvr->_num_buffered_bytes -= iter->chunk_bytes;
            VLOG_ROW << "DataStreamRecvr fetched #rows=" << (*chunk)->num_rows();
            _chunk_queue.erase(iter);

            if (closure != nullptr) {
                _recvr->_closure_block_timer->update(MonotonicNanos() - enter_time);
                // When the execution thread is blocked and the Chunk queue exceeds the memory limit,
                // the execution thread will hold done and will not return, block brpc from sending packets,
                // and the execution thread will call run() to let brpc continue to send packets,
                // and there will be memory release
#ifndef BE_TEST
                MemTracker* prev_tracker =
                        tls_thread_status.set_mem_tracker(ExecEnv::GetInstance()->process_mem_tracker());
                DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });
#endif

                closure->Run();
            }
            return Status::OK();
        }
        ++iter;
    }

    return Status::OK();
}

Status DataStreamRecvr::SenderQueue::_build_chunk_meta(const ChunkPB& pb_chunk) {
    if (UNLIKELY(pb_chunk.is_nulls().empty() || pb_chunk.slot_id_map().empty())) {
        return Status::InternalError("pb_chunk meta could not be empty");
    }

    _chunk_meta.slot_id_to_index.reserve(pb_chunk.slot_id_map().size());
    for (int i = 0; i < pb_chunk.slot_id_map().size(); i += 2) {
        _chunk_meta.slot_id_to_index[pb_chunk.slot_id_map()[i]] = pb_chunk.slot_id_map()[i + 1];
    }

    _chunk_meta.tuple_id_to_index.reserve(pb_chunk.tuple_id_map().size());
    for (int i = 0; i < pb_chunk.tuple_id_map().size(); i += 2) {
        _chunk_meta.tuple_id_to_index[pb_chunk.tuple_id_map()[i]] = pb_chunk.tuple_id_map()[i + 1];
    }

    _chunk_meta.is_nulls.resize(pb_chunk.is_nulls().size());
    for (int i = 0; i < pb_chunk.is_nulls().size(); ++i) {
        _chunk_meta.is_nulls[i] = pb_chunk.is_nulls()[i];
    }

    _chunk_meta.is_consts.resize(pb_chunk.is_consts().size());
    for (int i = 0; i < pb_chunk.is_consts().size(); ++i) {
        _chunk_meta.is_consts[i] = pb_chunk.is_consts()[i];
    }

    size_t column_index = 0;
    _chunk_meta.types.resize(pb_chunk.is_nulls().size());
    for (auto tuple_desc : _recvr->_row_desc.tuple_descriptors()) {
        const std::vector<SlotDescriptor*>& slots = tuple_desc->slots();
        for (const auto& kv : _chunk_meta.slot_id_to_index) {
            //TODO: performance?
            for (auto slot : slots) {
                if (kv.first == slot->id()) {
                    _chunk_meta.types[kv.second] = slot->type();
                    ++column_index;
                    break;
                }
            }
        }
    }
    for (const auto& kv : _chunk_meta.tuple_id_to_index) {
        _chunk_meta.types[kv.second] = TypeDescriptor(PrimitiveType::TYPE_BOOLEAN);
        ++column_index;
    }

    if (UNLIKELY(column_index != _chunk_meta.is_nulls.size())) {
        return Status::InternalError("build chunk meta error");
    }
    return Status::OK();
}

Status DataStreamRecvr::SenderQueue::add_chunks(const PTransmitChunkParams& request, ::google::protobuf::Closure** done,
                                                bool is_pipeline) {
    bool use_pass_through = request.use_pass_through();
    DCHECK(request.chunks_size() > 0 || use_pass_through);
    int32_t be_number = request.be_number();
    int64_t sequence = request.sequence();
    ScopedTimer<MonotonicStopWatch> wait_timer(_recvr->_sender_wait_lock_timer);
    {
        std::lock_guard<std::mutex> l(_lock);
        wait_timer.stop();
        if (_is_cancelled) {
            return Status::OK();
        }
        if (!is_pipeline) {
            // TODO(zc): Do we really need this check?
            auto iter = _packet_seq_map.find(be_number);
            if (iter != _packet_seq_map.end()) {
                if (iter->second >= sequence) {
                    LOG(WARNING) << "packet already exist [cur_packet_id=" << iter->second
                                 << " receive_packet_id=" << sequence << "]";
                    return Status::OK();
                }
                iter->second = sequence;
            } else {
                _packet_seq_map.emplace(be_number, sequence);
            }
        }

        // Following situation will match the following condition.
        // Sender send a packet failed, then close the channel.
        // but closed packet reach first, then the failed packet.
        // Then meet the assert
        // we remove the assert
        // DCHECK_GT(_num_remaining_senders, 0);
        if (_num_remaining_senders <= 0) {
            DCHECK(_sender_eos_set.end() != _sender_eos_set.find(be_number));
            return Status::OK();
        }
        // We only need to build chunk meta on first chunk and not use_pass_through
        // By using pass through, chunks are transmitted in shared memory without ser/deser
        // So there is no need to build chunk meta.
        if (_chunk_meta.types.empty() && !use_pass_through) {
            SCOPED_TIMER(_recvr->_deserialize_chunk_timer);
            auto& pchunk = request.chunks(0);
            RETURN_IF_ERROR(_build_chunk_meta(pchunk));
        }
    }

    ChunkQueue chunks;
    size_t total_chunk_bytes = 0;
    faststring uncompressed_buffer;
    bool prev_is_pipeline_level_shuffle = _is_pipeline_level_shuffle;
    _is_pipeline_level_shuffle =
            _recvr->_is_pipeline && request.has_is_pipeline_level_shuffle() && request.is_pipeline_level_shuffle();
    // _is_pipeline_level_shuffle must be stable after first assignment
    DCHECK(!_is_pipeline_level_shuffle_init || (prev_is_pipeline_level_shuffle == _is_pipeline_level_shuffle));
    _is_pipeline_level_shuffle_init = true;

    if (use_pass_through) {
        ChunkUniquePtrVector swap_chunks;
        std::vector<size_t> swap_bytes;
        _recvr->_pass_through_context.pull_chunks(request.sender_id(), &swap_chunks, &swap_bytes);
        DCHECK(swap_chunks.size() == swap_bytes.size());
        size_t bytes = 0;
        for (size_t i = 0; i < swap_chunks.size(); i++) {
            // The sending and receiving of chunks from _pass_through_context may out of order, and
            // considering the following sequences:
            // 1. add chunk_1 to _pass_through_context and send request_1
            // 2. add chunk_2 to _pass_through_context and send request_2
            // 3. receive request_1 and get both chunk_1 and chunk_2
            // 4. receive request_2 and get nothing
            // So one receiving may receive two or more chunks, and we need to use the chunk's driver_sequence
            // but not the request's driver_sequence
            ChunkItem item{static_cast<int64_t>(swap_bytes[i]), swap_chunks[i].second, std::move(swap_chunks[i].first),
                           nullptr};
            chunks.emplace_back(std::move(item));
            bytes += swap_bytes[i];
        }
        total_chunk_bytes += bytes;
        COUNTER_UPDATE(_recvr->_bytes_pass_through_counter, total_chunk_bytes);

    } else {
        for (auto i = 0; i < request.chunks().size(); ++i) {
            auto& pchunk = request.chunks().Get(i);
            auto driver_sequence = _is_pipeline_level_shuffle ? request.driver_sequences(i) : -1;
            int64_t chunk_bytes = pchunk.data().size();
            ChunkUniquePtr chunk = std::make_unique<vectorized::Chunk>();
            RETURN_IF_ERROR(_deserialize_chunk(pchunk, chunk.get(), &uncompressed_buffer));
            ChunkItem item{chunk_bytes, driver_sequence, std::move(chunk), nullptr};
            chunks.emplace_back(std::move(item));
            total_chunk_bytes += chunk_bytes;
        }
        COUNTER_UPDATE(_recvr->_bytes_received_counter, total_chunk_bytes);
    }

    wait_timer.start();
    {
        std::lock_guard<std::mutex> l(_lock);
        wait_timer.stop();

        // _is_cancelled may be modified after checking _is_cancelled above,
        // because lock is release temporarily when deserializing chunk.
        if (_is_cancelled) {
            return Status::OK();
        }

        const auto original_size = _chunk_queue.size();
        for (auto& item : chunks) {
            // This chunks may contains different driver_sequence
            if (_is_pipeline_level_shuffle) {
                // Some pipelines may be short-circuit, so we just drop the chunk we received
                if (_short_circuit_driver_sequences.find(item.driver_sequence) !=
                    _short_circuit_driver_sequences.end()) {
                    continue;
                }
                _has_chunks_per_driver_sequence[item.driver_sequence] = true;
            }
            _chunk_queue.emplace_back(std::move(item));
        }
        bool has_new_chunks = _chunk_queue.size() > original_size;
        if (has_new_chunks && done != nullptr && _recvr->exceeds_limit(total_chunk_bytes)) {
            _chunk_queue.back().closure = *done;
            _chunk_queue.back().enter_time = MonotonicNanos();
            *done = nullptr;
        }

        _recvr->_num_buffered_bytes += total_chunk_bytes;
    }
    _data_arrival_cv.notify_one();
    return Status::OK();
}

Status DataStreamRecvr::SenderQueue::add_chunks_and_keep_order(const PTransmitChunkParams& request,
                                                               ::google::protobuf::Closure** done) {
    bool use_pass_through = request.use_pass_through();
    DCHECK(request.chunks_size() > 0 || use_pass_through);
    const int32_t be_number = request.be_number();
    const int32_t sequence = request.sequence();

    {
        std::lock_guard<std::mutex> l(_lock);
        if (_is_cancelled) {
            return Status::OK();
        }

        if (_max_processed_sequences.find(be_number) == _max_processed_sequences.end()) {
            _max_processed_sequences[be_number] = -1;
        }

        if (_buffered_chunk_queues.find(be_number) == _buffered_chunk_queues.end()) {
            _buffered_chunk_queues[be_number] = phmap::flat_hash_map<int64_t, ChunkQueue>();
        }
    }

    ScopedTimer<MonotonicStopWatch> wait_timer(_recvr->_sender_wait_lock_timer);
    {
        std::lock_guard<std::mutex> l(_lock);
        wait_timer.stop();
        if (_is_cancelled) {
            return Status::OK();
        }

        // Following situation will match the following condition.
        // Sender send a packet failed, then close the channel.
        // but closed packet reach first, then the failed packet.
        // Then meet the assert
        // we remove the assert
        if (_num_remaining_senders <= 0) {
            DCHECK(_sender_eos_set.end() != _sender_eos_set.find(be_number));
            return Status::OK();
        }
        // We only need to build chunk meta on first chunk and not use_pass_through
        // By using pass through, chunks are transmitted in shared memory without ser/deser
        // So there is no need to build chunk meta.
        if (_chunk_meta.types.empty() && !use_pass_through) {
            SCOPED_TIMER(_recvr->_deserialize_chunk_timer);
            auto& pchunk = request.chunks(0);
            RETURN_IF_ERROR(_build_chunk_meta(pchunk));
        }
    }

    size_t total_chunk_bytes = 0;
    faststring uncompressed_buffer;
    ChunkQueue local_chunk_queue;
    bool prev_is_pipeline_level_shuffle = _is_pipeline_level_shuffle;
    _is_pipeline_level_shuffle =
            _recvr->_is_pipeline && request.has_is_pipeline_level_shuffle() && request.is_pipeline_level_shuffle();
    // _is_pipeline_level_shuffle must be stable after first assignment
    DCHECK(!_is_pipeline_level_shuffle_init || (prev_is_pipeline_level_shuffle == _is_pipeline_level_shuffle));
    _is_pipeline_level_shuffle_init = true;

    if (use_pass_through) {
        ChunkUniquePtrVector swap_chunks;
        std::vector<size_t> swap_bytes;
        _recvr->_pass_through_context.pull_chunks(request.sender_id(), &swap_chunks, &swap_bytes);
        DCHECK(swap_chunks.size() == swap_bytes.size());
        size_t bytes = 0;
        for (size_t i = 0; i < swap_chunks.size(); i++) {
            // The sending and receiving of chunks from _pass_through_context may out of order, and
            // considering the following sequences:
            // 1. add chunk_1 to _pass_through_context and send request_1
            // 2. add chunk_2 to _pass_through_context and send request_2
            // 3. receive request_1 and get both chunk_1 and chunk_2
            // 4. receive request_2 and get nothing
            // So one receiving may receive two or more chunks, and we need to use the chunk's driver_sequence
            // but not the request's driver_sequence
            ChunkItem item{static_cast<int64_t>(swap_bytes[i]), swap_chunks[i].second, std::move(swap_chunks[i].first),
                           nullptr};
            local_chunk_queue.emplace_back(std::move(item));
            bytes += swap_bytes[i];
        }
        total_chunk_bytes += bytes;
        COUNTER_UPDATE(_recvr->_bytes_pass_through_counter, total_chunk_bytes);
    } else {
        for (auto i = 0; i < request.chunks().size(); ++i) {
            auto& pchunk = request.chunks().Get(i);
            auto driver_sequence = _is_pipeline_level_shuffle ? request.driver_sequences(i) : -1;
            int64_t chunk_bytes = pchunk.data().size();
            ChunkUniquePtr chunk = std::make_unique<vectorized::Chunk>();
            RETURN_IF_ERROR(_deserialize_chunk(pchunk, chunk.get(), &uncompressed_buffer));

            ChunkItem item{chunk_bytes, driver_sequence, std::move(chunk), nullptr};

            // TODO(zc): review this chunk_bytes
            local_chunk_queue.emplace_back(std::move(item));

            total_chunk_bytes += chunk_bytes;
        }
        COUNTER_UPDATE(_recvr->_bytes_received_counter, total_chunk_bytes);
    }

    wait_timer.start();
    {
        std::lock_guard<std::mutex> l(_lock);
        wait_timer.stop();

        // _is_cancelled may be modified after checking _is_cancelled above,
        // because lock is release temporarily when deserializing chunk.
        if (_is_cancelled) {
            return Status::OK();
        }

        auto& chunk_queues = _buffered_chunk_queues[be_number];

        if (!local_chunk_queue.empty() && done != nullptr && _recvr->exceeds_limit(total_chunk_bytes)) {
            local_chunk_queue.back().closure = *done;
            local_chunk_queue.back().enter_time = MonotonicNanos();
            *done = nullptr;
        }

        // The queue in chunk_queues cannot be changed, so it must be
        // assigned to chunk_queues after local_chunk_queue is initialized
        // Otherwise, other threads may see the intermediate state because
        // the initialization of local_chunk_queue is beyond mutex
        chunk_queues[sequence] = std::move(local_chunk_queue);

        phmap::flat_hash_map<int64_t, ChunkQueue>::iterator it;
        int64_t& max_processed_sequence = _max_processed_sequences[be_number];

        // max_processed_sequence + 1 means the first unprocessed sequence
        while ((it = chunk_queues.find(max_processed_sequence + 1)) != chunk_queues.end()) {
            ChunkQueue& unprocessed_chunk_queue = (*it).second;

            // Now, all the packets with sequance <= unprocessed_sequence have been received
            // so chunks of unprocessed_sequence can be flushed to ready queue
            for (auto& item : unprocessed_chunk_queue) {
                // This chunks may contains different driver_sequence when enable local pass through
                // So we use driver_sequence of each chunk instead of request's driver_sequence
                if (_is_pipeline_level_shuffle) {
                    // Some pipelines may be short-circuit, so we just drop the chunk we received
                    if (_short_circuit_driver_sequences.find(item.driver_sequence) !=
                        _short_circuit_driver_sequences.end()) {
                        // We may buffered closure in last reception, but the branch of the driver_sequence may
                        // become short-circuit now, so we make sure to invoke the closure
                        if (item.closure != nullptr) {
                            _recvr->_closure_block_timer->update(MonotonicNanos() - item.enter_time);
                            item.closure->Run();
                        }
                        continue;
                    }
                    _has_chunks_per_driver_sequence[item.driver_sequence] = true;
                }
                _chunk_queue.emplace_back(std::move(item));
            }

            chunk_queues.erase(it);
            ++max_processed_sequence;
        }

        _recvr->_num_buffered_bytes += total_chunk_bytes;
    }
    return Status::OK();
}

Status DataStreamRecvr::SenderQueue::_deserialize_chunk(const ChunkPB& pchunk, vectorized::Chunk* chunk,
                                                        faststring* uncompressed_buffer) {
    if (pchunk.compress_type() == CompressionTypePB::NO_COMPRESSION) {
        SCOPED_TIMER(_recvr->_deserialize_chunk_timer);
        TRY_CATCH_BAD_ALLOC({
            serde::ProtobufChunkDeserializer des(_chunk_meta);
            StatusOr<vectorized::Chunk> res = des.deserialize(pchunk.data());
            if (!res.ok()) return res.status();
            *chunk = std::move(res).value();
        });
    } else {
        size_t uncompressed_size = 0;
        {
            SCOPED_TIMER(_recvr->_decompress_chunk_timer);
            const BlockCompressionCodec* codec = nullptr;
            RETURN_IF_ERROR(get_block_compression_codec(pchunk.compress_type(), &codec));
            uncompressed_size = pchunk.uncompressed_size();
            TRY_CATCH_BAD_ALLOC(uncompressed_buffer->resize(uncompressed_size));
            Slice output{uncompressed_buffer->data(), uncompressed_size};
            RETURN_IF_ERROR(codec->decompress(pchunk.data(), &output));
        }
        {
            SCOPED_TIMER(_recvr->_deserialize_chunk_timer);
            TRY_CATCH_BAD_ALLOC({
                std::string_view buff(reinterpret_cast<const char*>(uncompressed_buffer->data()), uncompressed_size);
                serde::ProtobufChunkDeserializer des(_chunk_meta);
                StatusOr<vectorized::Chunk> res = des.deserialize(buff);
                if (!res.ok()) return res.status();
                *chunk = std::move(res).value();
            });
        }
    }
    return Status::OK();
}

void DataStreamRecvr::SenderQueue::decrement_senders(int be_number) {
    std::lock_guard<std::mutex> l(_lock);
    if (_sender_eos_set.end() != _sender_eos_set.find(be_number)) {
        return;
    }
    _sender_eos_set.insert(be_number);
    DCHECK_GT(_num_remaining_senders, 0);
    _num_remaining_senders--;
    VLOG_FILE << "decremented senders: fragment_instance_id=" << print_id(_recvr->fragment_instance_id())
              << " node_id=" << _recvr->dest_node_id() << " #senders=" << _num_remaining_senders
              << " be_number=" << be_number;
    if (_num_remaining_senders == 0) {
        _data_arrival_cv.notify_one();
    }
}

void DataStreamRecvr::SenderQueue::cancel() {
    {
        std::lock_guard<std::mutex> l(_lock);
        if (_is_cancelled) {
            return;
        }
        _is_cancelled = true;
        VLOG_QUERY << "cancelled stream: _fragment_instance_id=" << _recvr->fragment_instance_id()
                   << " node_id=" << _recvr->dest_node_id();
    }
    // Wake up all threads waiting to produce/consume batches.  They will all
    // notice that the stream is cancelled and handle it.
    _data_arrival_cv.notify_all();

    {
        std::lock_guard<std::mutex> l(_lock);
        clean_buffer_queues();
    }
}

void DataStreamRecvr::SenderQueue::close() {
    {
        // If _is_cancelled is not set to true, there may be concurrent send
        // which add batch to _batch_queue. The batch added after _batch_queue
        // is clear will be memory leak
        std::lock_guard<std::mutex> l(_lock);
        _is_cancelled = true;

        clean_buffer_queues();
    }
}

void DataStreamRecvr::SenderQueue::clean_buffer_queues() {
    for (auto& item : _chunk_queue) {
        if (item.closure != nullptr) {
            _recvr->_closure_block_timer->update(MonotonicNanos() - item.enter_time);
            item.closure->Run();
        }
    }
    _chunk_queue.clear();
    for (auto& [_, chunk_queues] : _buffered_chunk_queues) {
        for (auto& [_, chunk_queue] : chunk_queues) {
            for (auto& item : chunk_queue) {
                if (item.closure != nullptr) {
                    _recvr->_closure_block_timer->update(MonotonicNanos() - item.enter_time);
                    item.closure->Run();
                }
            }
        }
    }
    _buffered_chunk_queues.clear();
}

Status DataStreamRecvr::create_merger(RuntimeState* state, const SortExecExprs* exprs, const std::vector<bool>* is_asc,
                                      const std::vector<bool>* is_null_first) {
    DCHECK(_is_merging);
    _chunks_merger = std::make_unique<vectorized::SortedChunksMerger>(state, _keep_order);
    vectorized::ChunkSuppliers chunk_suppliers;
    for (SenderQueue* q : _sender_queues) {
        // we use chunk_supplier in non-pipeline.
        auto f = [q](vectorized::Chunk** chunk) -> Status { return q->get_chunk(chunk); };
        chunk_suppliers.emplace_back(std::move(f));
    }
    vectorized::ChunkProbeSuppliers chunk_probe_suppliers;
    for (SenderQueue* q : _sender_queues) {
        // we willn't use chunk_probe_supplier in non-pipeline.
        auto f = [q](vectorized::Chunk** chunk) -> bool { return false; };
        chunk_probe_suppliers.emplace_back(std::move(f));
    }
    vectorized::ChunkHasSuppliers chunk_has_suppliers;
    for (SenderQueue* q : _sender_queues) {
        // we willn't use chunk_has_supplier in non-pipeline.
        auto f = [q]() -> bool { return false; };
        chunk_has_suppliers.emplace_back(std::move(f));
    }

    RETURN_IF_ERROR(_chunks_merger->init(chunk_suppliers, chunk_probe_suppliers, chunk_has_suppliers,
                                         &(exprs->lhs_ordering_expr_ctxs()), is_asc, is_null_first));
    _chunks_merger->set_profile(_profile.get());
    return Status::OK();
}

Status DataStreamRecvr::create_merger_for_pipeline(RuntimeState* state, const SortExecExprs* exprs,
                                                   const std::vector<bool>* is_asc,
                                                   const std::vector<bool>* is_null_first) {
    DCHECK(_is_merging);
    _chunks_merger = std::make_unique<vectorized::SortedChunksMerger>(state, _keep_order);
    vectorized::ChunkSuppliers chunk_suppliers;
    for (SenderQueue* q : _sender_queues) {
        // we willn't use chunk_supplier in pipeline.
        auto f = [q](vectorized::Chunk** chunk) -> Status { return Status::OK(); };
        chunk_suppliers.emplace_back(std::move(f));
    }
    vectorized::ChunkProbeSuppliers chunk_probe_suppliers;
    for (SenderQueue* q : _sender_queues) {
        // we use chunk_probe_supplier in pipeline.
        auto f = [q](vectorized::Chunk** chunk) -> bool { return q->try_get_chunk(chunk); };
        chunk_probe_suppliers.emplace_back(std::move(f));
    }
    vectorized::ChunkHasSuppliers chunk_has_suppliers;
    for (SenderQueue* q : _sender_queues) {
        // we use chunk_has_supplier in pipeline.
        auto f = [q]() -> bool { return q->has_chunk(); };
        chunk_has_suppliers.emplace_back(std::move(f));
    }

    RETURN_IF_ERROR(_chunks_merger->init_for_pipeline(chunk_suppliers, chunk_probe_suppliers, chunk_has_suppliers,
                                                      &(exprs->lhs_ordering_expr_ctxs()), is_asc, is_null_first));
    _chunks_merger->set_profile(_profile.get());
    return Status::OK();
}

DataStreamRecvr::DataStreamRecvr(DataStreamMgr* stream_mgr, RuntimeState* runtime_state, const RowDescriptor& row_desc,
                                 const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int num_senders,
                                 bool is_merging, int total_buffer_limit, std::shared_ptr<RuntimeProfile> profile,
                                 std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr,
                                 bool is_pipeline, int32_t degree_of_parallelism, bool keep_order,
                                 PassThroughChunkBuffer* pass_through_chunk_buffer)
        : _mgr(stream_mgr),
          _fragment_instance_id(fragment_instance_id),
          _dest_node_id(dest_node_id),
          _total_buffer_limit(total_buffer_limit),
          _row_desc(row_desc),
          _is_merging(is_merging),
          _num_buffered_bytes(0),
          _profile(std::move(profile)),
          _instance_profile(runtime_state->runtime_profile_ptr()),
          _query_mem_tracker(runtime_state->query_mem_tracker_ptr()),
          _instance_mem_tracker(runtime_state->instance_mem_tracker_ptr()),
          _sub_plan_query_statistics_recvr(std::move(sub_plan_query_statistics_recvr)),
          _is_pipeline(is_pipeline),
          _degree_of_parallelism(degree_of_parallelism),
          _keep_order(keep_order),
          _pass_through_context(pass_through_chunk_buffer, fragment_instance_id, dest_node_id) {
    // Create one queue per sender if is_merging is true.
    int num_queues = is_merging ? num_senders : 1;
    _sender_queues.reserve(num_queues);
    int num_sender_per_queue = is_merging ? 1 : num_senders;
    for (int i = 0; i < num_queues; ++i) {
        SenderQueue* queue =
                _sender_queue_pool.add(new SenderQueue(this, num_sender_per_queue, _degree_of_parallelism));
        _sender_queues.push_back(queue);
    }

    // Initialize the counters
    _bytes_received_counter = ADD_COUNTER(_profile, "BytesReceived", TUnit::BYTES);
    _bytes_pass_through_counter = ADD_COUNTER(_profile, "BytesPassThrough", TUnit::BYTES);
    _request_received_counter = ADD_COUNTER(_profile, "RequestReceived", TUnit::UNIT);
    _closure_block_timer = ADD_TIMER(_profile, "ClosureBlockTime");
    _deserialize_chunk_timer = ADD_TIMER(_profile, "DeserializeChunkTime");
    _decompress_chunk_timer = ADD_TIMER(_profile, "DecompressChunkTime");
    _process_total_timer = ADD_TIMER(_profile, "ReceiverProcessTotalTime");

    _sender_total_timer = ADD_TIMER(_profile, "SenderTotalTime");
    _sender_wait_lock_timer = ADD_TIMER(_profile, "SenderWaitLockTime");

    _pass_through_context.init();
}

Status DataStreamRecvr::get_next(vectorized::ChunkPtr* chunk, bool* eos) {
    DCHECK(_chunks_merger.get() != nullptr);
    return _chunks_merger->get_next(chunk, eos);
}

Status DataStreamRecvr::get_next_for_pipeline(vectorized::ChunkPtr* chunk, std::atomic<bool>* eos, bool* should_exit) {
    DCHECK(_chunks_merger.get() != nullptr);
    return _chunks_merger->get_next_for_pipeline(chunk, eos, should_exit);
}

bool DataStreamRecvr::is_data_ready() {
    return _chunks_merger->is_data_ready();
}

Status DataStreamRecvr::add_chunks(const PTransmitChunkParams& request, ::google::protobuf::Closure** done) {
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(_instance_mem_tracker.get());
    DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

    SCOPED_TIMER(_process_total_timer);
    SCOPED_TIMER(_sender_total_timer);
    COUNTER_UPDATE(_request_received_counter, 1);
    int use_sender_id = _is_merging ? request.sender_id() : 0;
    // Add all batches to the same queue if _is_merging is false.

    if (_keep_order) {
        DCHECK(_is_pipeline);
        return _sender_queues[use_sender_id]->add_chunks_and_keep_order(request, done);
    } else {
        return _sender_queues[use_sender_id]->add_chunks(request, done, _is_pipeline);
    }
}

void DataStreamRecvr::remove_sender(int sender_id, int be_number) {
    int use_sender_id = _is_merging ? sender_id : 0;
    _sender_queues[use_sender_id]->decrement_senders(be_number);
}

void DataStreamRecvr::cancel_stream() {
    for (auto& _sender_queue : _sender_queues) {
        _sender_queue->cancel();
    }
}

void DataStreamRecvr::close() {
    for (auto& _sender_queue : _sender_queues) {
        _sender_queue->close();
    }
    // Remove this receiver from the DataStreamMgr that created it.
    // TODO: log error msg
    _mgr->deregister_recvr(fragment_instance_id(), dest_node_id());
    _mgr = nullptr;
    _chunks_merger.reset();

    _closure_block_timer->update(_closure_block_timer->value() / _degree_of_parallelism);
}

DataStreamRecvr::~DataStreamRecvr() {
    DCHECK(_mgr == nullptr) << "Must call close()";
}

Status DataStreamRecvr::get_chunk(std::unique_ptr<vectorized::Chunk>* chunk) {
    DCHECK(!_is_merging);
    DCHECK_EQ(_sender_queues.size(), 1);
    vectorized::Chunk* tmp_chunk = nullptr;
    Status status = _sender_queues[0]->get_chunk(&tmp_chunk);
    chunk->reset(tmp_chunk);
    return status;
}

Status DataStreamRecvr::get_chunk_for_pipeline(std::unique_ptr<vectorized::Chunk>* chunk,
                                               const int32_t driver_sequence) {
    DCHECK(!_is_merging);
    DCHECK_EQ(_sender_queues.size(), 1);
    vectorized::Chunk* tmp_chunk = nullptr;
    Status status = _sender_queues[0]->get_chunk_for_pipeline(&tmp_chunk, driver_sequence);
    chunk->reset(tmp_chunk);
    return status;
}

void DataStreamRecvr::short_circuit_for_pipeline(const int32_t driver_sequence) {
    return _sender_queues[0]->short_circuit_for_pipeline(driver_sequence);
}

bool DataStreamRecvr::has_output_for_pipeline(const int32_t driver_sequence) const {
    DCHECK(!_is_merging);
    return _sender_queues[0]->has_output_for_pipeline(driver_sequence);
}

bool DataStreamRecvr::is_finished() const {
    DCHECK(!_is_merging);
    return _sender_queues[0]->is_finished();
}

} // namespace starrocks
