// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once
#include "column/vectorized_fwd.h"
#include "runtime/data_stream_recvr.h"
#include "serde/protobuf_serde.h"
#include "util/moodycamel/concurrentqueue.h"
#include "util/spinlock.h"

namespace google::protobuf {
class Closure;
} // namespace google::protobuf

namespace starrocks {
using vectorized::ChunkUniquePtr;

class DataStreamRecvr::SenderQueue {
public:
    SenderQueue(DataStreamRecvr* parent_recvr) : _recvr(parent_recvr) {}
    virtual ~SenderQueue() = default;

    // get chunk from this sender queue, driver_sequence is only meaningful in pipeline engine
    virtual Status get_chunk(vectorized::Chunk** chunk, const int32_t driver_sequence = -1) = 0;

    // check if data has come, work with try_get_chunk.
    virtual bool has_chunk() = 0;

    // Probe for chunks, because _chunk_queue maybe empty when data hasn't come yet.
    virtual bool try_get_chunk(vectorized::Chunk** chunk) = 0;

    // add chunks to this sender queue if this stream has not been cancelled
    virtual Status add_chunks(const PTransmitChunkParams& request, ::google::protobuf::Closure** done) = 0;

    // add chunks to this sender queue if this stream has not been cancelled
    // Process data in strict accordance with the order of the sequence
    virtual Status add_chunks_and_keep_order(const PTransmitChunkParams& request,
                                             ::google::protobuf::Closure** done) = 0;

    // Decrement the number of remaining senders for this queue
    virtual void decrement_senders(int be_number) = 0;

    virtual void cancel() = 0;

    virtual void close() = 0;

protected:
    Status _build_chunk_meta(const ChunkPB& pb_chunk);

    Status _deserialize_chunk(const ChunkPB& pchunk, vectorized::Chunk* chunk, faststring* uncompressed_buffer);

    DataStreamRecvr* _recvr = nullptr;

    serde::ProtobufChunkMeta _chunk_meta;
};

class DataStreamRecvr::NonPipelineSenderQueue : public DataStreamRecvr::SenderQueue {
public:
    NonPipelineSenderQueue(DataStreamRecvr* parent_recvr, int num_senders);
    ~NonPipelineSenderQueue() override = default;

    Status get_chunk(vectorized::Chunk** chunk, const int32_t driver_sequence = -1) override;

    bool has_chunk() override;

    bool try_get_chunk(vectorized::Chunk** chunk) override;

    Status add_chunks(const PTransmitChunkParams& request, ::google::protobuf::Closure** done) override;

    Status add_chunks_and_keep_order(const PTransmitChunkParams& request, ::google::protobuf::Closure** done) override;

    void decrement_senders(int be_number) override;

    void cancel() override;

    void close() override;

private:
    void clean_buffer_queues();

    struct ChunkItem {
        int64_t chunk_bytes = 0;
        ChunkUniquePtr chunk_ptr;
        // When the memory of the ChunkQueue exceeds the limit,
        // we have to hold closure of the request, so as not to let the sender continue to send data.
        // A Request may have multiple Chunks, so only when the last Chunk of the Request is consumed,
        // the callback is closed- >run() Let the sender continue to send data
        google::protobuf::Closure* closure = nullptr;
        // Time in nano of saving closure
        int64_t queue_enter_time;
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
class DataStreamRecvr::PipelineSenderQueue : public DataStreamRecvr::SenderQueue {
public:
    PipelineSenderQueue(DataStreamRecvr* parent_recvr, int num_senders, int degree_of_parallism);
    ~PipelineSenderQueue() override = default;

    Status get_chunk(vectorized::Chunk** chunk, const int32_t driver_sequence = -1) override;

    bool has_chunk() override;

    bool try_get_chunk(vectorized::Chunk** chunk) override;

    Status add_chunks(const PTransmitChunkParams& request, ::google::protobuf::Closure** done) override;

    Status add_chunks_and_keep_order(const PTransmitChunkParams& request, ::google::protobuf::Closure** done) override;

    void decrement_senders(int be_number) override;

    void cancel() override;

    void close() override;

    void short_circuit(const int32_t driver_sequence);

    bool has_output(const int32_t driver_sequence);

    bool is_finished() const;

private:
    void clean_buffer_queues();

    struct ChunkItem {
        int64_t chunk_bytes = 0;
        // Invalid if SenderQueue::_is_pipeline_level_shuffle is false
        int32_t driver_sequence = -1;
        // When the memory of the ChunkQueue exceeds the limit,
        // we have to hold closure of the request, so as not to let the sender continue to send data.
        // A Request may have multiple Chunks, so only when the last Chunk of the Request is consumed,
        // the callback is closed->run() Let the sender continue to send data
        google::protobuf::Closure* closure = nullptr;
        ChunkUniquePtr chunk_ptr;
        // Time in nano of saving closure
        int64_t queue_enter_time;

        inline static ChunkItem create(int64_t chunk_bytes, int32_t driver_sequence, google::protobuf::Closure* closure,
                                       ChunkUniquePtr&& chunk_ptr) {
            ChunkItem result;
            result.chunk_bytes = chunk_bytes;
            result.driver_sequence = driver_sequence;
            result.closure = closure;
            result.chunk_ptr = std::move(chunk_ptr);
            return result;
        }
    };

    typedef moodycamel::ConcurrentQueue<ChunkItem> ChunkQueue;

    std::atomic<bool> _is_cancelled{false};
    std::atomic<int> _num_remaining_senders;

    typedef SpinLock Mutex;
    Mutex _lock;

    // if _is_pipeline_level_shuffle=true, we will create a queue for each driver sequence to avoid competition
    // otherwise, we will only use the first item
    std::vector<ChunkQueue> _chunk_queues;
    // _producer_token is used to ensure that the order of dequeueing is the same as enqueueing
    // it is only used when the order needs to be guaranteed
    std::unique_ptr<ChunkQueue::producer_token_t> _producer_token;
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
    typedef std::list<ChunkItem> ChunkList;
    phmap::flat_hash_map<int, phmap::flat_hash_map<int64_t, ChunkList>> _buffered_chunk_queues;

    std::unordered_set<int32_t> _short_circuit_driver_sequences;
};

} // namespace starrocks