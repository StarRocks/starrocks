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

#include <condition_variable>
#include <utility>

#include "bthread/mutex.h"
#include "column/vectorized_fwd.h"
#include "runtime/data_stream_recvr.h"
#include "serde/protobuf_serde.h"
#include "util/moodycamel/concurrentqueue.h"
#include "util/spinlock.h"

namespace google::protobuf {
class Closure;
} // namespace google::protobuf

namespace starrocks {
class DataStreamRecvr::SenderQueue {
public:
    SenderQueue(DataStreamRecvr* parent_recvr) : _recvr(parent_recvr) {}
    virtual ~SenderQueue() = default;

    // get chunk from this sender queue, driver_sequence is only meaningful in pipeline engine
    virtual Status get_chunk(Chunk** chunk, const int32_t driver_sequence = -1) = 0;

    // check if data has come, work with try_get_chunk.
    virtual bool has_chunk() = 0;

    // Probe for chunks, because _chunk_queue maybe empty when data hasn't come yet.
    virtual bool try_get_chunk(Chunk** chunk) = 0;

    // add chunks to this sender queue if this stream has not been cancelled
    virtual Status add_chunks(const PTransmitChunkParams& request, Metrics& metrics,
                              ::google::protobuf::Closure** done) = 0;

    // add chunks to this sender queue if this stream has not been cancelled
    // Process data in strict accordance with the order of the sequence
    virtual Status add_chunks_and_keep_order(const PTransmitChunkParams& request, Metrics& metrics,
                                             ::google::protobuf::Closure** done) = 0;

    // Decrement the number of remaining senders for this queue
    virtual void decrement_senders(int be_number) = 0;

    virtual void cancel() = 0;

    virtual void close() = 0;

protected:
    Status _build_chunk_meta(const ChunkPB& pb_chunk);

    Status _deserialize_chunk(const ChunkPB& pchunk, Chunk* chunk, Metrics& metrics, faststring* uncompressed_buffer);

    DataStreamRecvr* _recvr = nullptr;

    serde::ProtobufChunkMeta _chunk_meta;
};

class DataStreamRecvr::NonPipelineSenderQueue final : public DataStreamRecvr::SenderQueue {
public:
    NonPipelineSenderQueue(DataStreamRecvr* parent_recvr, int num_senders);
    ~NonPipelineSenderQueue() override = default;

    Status get_chunk(Chunk** chunk, const int32_t driver_sequence = -1) override;

    bool has_chunk() override;

    bool try_get_chunk(Chunk** chunk) override;

    Status add_chunks(const PTransmitChunkParams& request, Metrics& metrics,
                      ::google::protobuf::Closure** done) override;

    Status add_chunks_and_keep_order(const PTransmitChunkParams& request, Metrics& metrics,
                                     ::google::protobuf::Closure** done) override;

    void decrement_senders(int be_number) override;

    void cancel() override;

    void close() override;

private:
    void clean_buffer_queues();

    template <bool keep_order>
    Status add_chunks(const PTransmitChunkParams& request, Metrics& metrics, ::google::protobuf::Closure** done);

    struct ChunkItem {
        int64_t chunk_bytes = 0;
        ChunkUniquePtr chunk_ptr;
        // When the memory of the ChunkQueue exceeds the limit,
        // we have to hold closure of the request, so as not to let the sender continue to send data.
        // A Request may have multiple Chunks, so only when the last Chunk of the Request is consumed,
        // the callback is closed- >run() Let the sender continue to send data
        google::protobuf::Closure* closure = nullptr;
        // Time in nano of saving closure
        int64_t queue_enter_time = -1;
    };

    typedef std::mutex Mutex;
    // protects all subsequent data.
    mutable Mutex _lock;
    // if true, the receiver fragment for this stream got cancelled
    bool _is_cancelled = false;

    // number of senders which haven't closed the channel yet
    // (if it drops to 0, end-of-stream is true)
    int _num_remaining_senders;

    // signal arrival of new batch or the eos/cancelled condition
    std::condition_variable _data_arrival_cv;

    typedef std::list<ChunkItem> ChunkQueue;
    ChunkQueue _chunk_queue;

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
};

// PipelineSenderQueue will be called in the pipeline execution threads.
// In order to avoid thread blocking caused by lock competition,
// we try to use lock-free structures in the relevant interface as much as possible.
// It should be noted that some atomic variables are updated at the same time under the protection of lock,
// which may not be completely consistent when reading without lock, but will eventually be consistent.
// This won't affect the correctness in our usage scenario.
class DataStreamRecvr::PipelineSenderQueue final : public DataStreamRecvr::SenderQueue {
public:
    PipelineSenderQueue(DataStreamRecvr* parent_recvr, int num_senders, int degree_of_parallism);
    ~PipelineSenderQueue() override { close(); }

    Status get_chunk(Chunk** chunk, const int32_t driver_sequence = -1) override;

    bool has_chunk() override;

    bool try_get_chunk(Chunk** chunk) override;

    Status add_chunks(const PTransmitChunkParams& request, Metrics& metrics,
                      ::google::protobuf::Closure** done) override;

    Status add_chunks_and_keep_order(const PTransmitChunkParams& request, Metrics& metrics,
                                     ::google::protobuf::Closure** done) override;

    void decrement_senders(int be_number) override;

    void cancel() override;

    void close() override;

    void short_circuit(const int32_t driver_sequence);

    bool has_output(const int32_t driver_sequence);

    bool is_finished() const;

private:
    struct ChunkItem {
        int64_t chunk_bytes = 0;
        // Invalid if SenderQueue::_is_pipeline_level_shuffle is false
        int32_t driver_sequence = -1;
        // When the memory of the ChunkQueue exceeds the limit,
        // we have to hold closure of the request, so as not to let the sender continue to send data.
        // A Request may have multiple Chunks, so only when the last Chunk of the Request is consumed,
        // the callback is closed->run() Let the sender continue to send data
        google::protobuf::Closure* closure = nullptr;
        // if pass_through is used, the chunk will be stored in chunk_ptr.
        // otherwise, the chunk that has not been deserialized will be stored in pchunk and deserialized lazily during get_chunk
        ChunkUniquePtr chunk_ptr;
        ChunkPB pchunk;
        // Time in nano of saving closure
        int64_t queue_enter_time = -1;

        ChunkItem() = default;

        ChunkItem(int64_t chunk_bytes, int32_t driver_sequence, google::protobuf::Closure* closure,
                  ChunkUniquePtr&& chunk_ptr)
                : chunk_bytes(chunk_bytes),
                  driver_sequence(driver_sequence),
                  closure(closure),
                  chunk_ptr(std::move(chunk_ptr)) {}

        ChunkItem(int64_t chunk_bytes, int32_t driver_sequence, google::protobuf::Closure* closure, ChunkPB pchunk)
                : chunk_bytes(chunk_bytes),
                  driver_sequence(driver_sequence),
                  closure(closure),
                  pchunk(std::move(pchunk)) {}
    };

    typedef std::list<ChunkItem> ChunkList;

    void clean_buffer_queues();

    StatusOr<ChunkList> get_chunks_from_pass_through(const int32_t sender_id, size_t& total_chunk_bytes);

    template <bool need_deserialization>
    StatusOr<ChunkList> get_chunks_from_request(const PTransmitChunkParams& request, Metrics& metrics,
                                                size_t& total_chunk_bytes);

    Status try_to_build_chunk_meta(const PTransmitChunkParams& request, Metrics& metrics);

    template <bool keep_order>
    Status add_chunks(const PTransmitChunkParams& request, Metrics& metrics, ::google::protobuf::Closure** done);

    typedef moodycamel::ConcurrentQueue<ChunkItem> ChunkQueue;

    std::atomic<bool> _is_cancelled{false};
    std::atomic<int> _num_remaining_senders;

    typedef bthread::Mutex Mutex;
    Mutex _lock;

    // if _is_pipeline_level_shuffle=true, we will create a queue for each driver sequence to avoid competition
    // otherwise, we will only use the first item
    std::vector<ChunkQueue> _chunk_queues;
    // _producer_token is used to ensure that the order of dequeueing is the same as enqueueing
    // it is only used when the order needs to be guaranteed
    std::unique_ptr<ChunkQueue::producer_token_t> _producer_token;

    struct ChunkQueueState {
        // Record the number of blocked closure in the queue
        std::atomic_int32_t blocked_closure_num = 0;
        // Record whether the queue is in the unplug state.
        // In the unplug state, has_output will return true directly if there is a chunk in the queue.
        // Otherwise, it will try to batch enough chunks to reduce the scheduling overhead.
        bool unpluging = false;
        std::atomic<bool> is_short_circuited = false;
    };
    std::vector<ChunkQueueState> _chunk_queue_states;

    std::atomic<size_t> _total_chunks{0};
    bool _is_pipeline_level_shuffle = false;

    std::unordered_set<int> _sender_eos_set;

    // distribution of received sequence numbers:
    // part1: { sequence | 1 <= sequence <= _max_processed_sequence }
    // part2: { sequence | seq = _max_processed_sequence + i, i > 1 }
    phmap::flat_hash_map<int, int64_t> _max_processed_sequences;
    // chunk request may be out-of-order, but we have to deal with it in order
    // key of first level is be_number
    // key of second level is request sequence
    phmap::flat_hash_map<int, phmap::flat_hash_map<int64_t, ChunkList>> _buffered_chunk_queues;

    static constexpr size_t kUnplugBufferThreshold = 16;
};

} // namespace starrocks
