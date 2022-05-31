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
};

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
        ChunkUniquePtr chunk_ptr;
        // When the memory of the ChunkQueue exceeds the limit,
        // we have to hold closure of the request, so as not to let the sender continue to send data.
        // A Request may have multiple Chunks, so only when the last Chunk of the Request is consumed,
        // the callback is closed- >run() Let the sender continue to send data
        google::protobuf::Closure* closure = nullptr;

        ChunkItem() {}
        ChunkItem(int64_t bytes, int32_t seq, ChunkUniquePtr ptr, google::protobuf::Closure* cls)
                : chunk_bytes(bytes), driver_sequence(seq), chunk_ptr(std::move(ptr)), closure(cls) {}

        ChunkItem(ChunkItem&& rhs) {
            chunk_bytes = rhs.chunk_bytes;
            driver_sequence = rhs.driver_sequence;
            chunk_ptr.swap(rhs.chunk_ptr);
            closure = rhs.closure;
            rhs.closure = nullptr;
        }

        ChunkItem& operator=(ChunkItem&& rhs) {
            chunk_bytes = rhs.chunk_bytes;
            driver_sequence = rhs.driver_sequence;
            chunk_ptr.swap(rhs.chunk_ptr);
            closure = rhs.closure;
            rhs.closure = nullptr;
            return *this;
        }
    };
    class ChunkQueue {
    public:
        ChunkQueue() {}
        ~ChunkQueue() {}

        ChunkQueue(const ChunkQueue&) = delete;
        ChunkQueue& operator=(const ChunkQueue&) = delete;

        ChunkQueue(ChunkQueue&& rhs) {
            _chunks.swap(rhs._chunks);
            _size.store(rhs._size);
        }

        ChunkQueue& operator=(ChunkQueue&& rhs) {
            _chunks.swap(rhs._chunks);
            _size.store(rhs._size);
            return *this;
        }

        inline bool empty() const { return _size.load() == 0; }

        inline bool size() const { return _size.load(); }

        void enqueue(ChunkItem&& item) {
            _chunks.enqueue(std::move(item));
            _size += 1;
        }

        bool try_dequeue(ChunkItem& item) {
            if (!_chunks.try_dequeue(item)) {
                return false;
            }
            _size -= 1;
            return true;
        }

    private:
        moodycamel::ConcurrentQueue<ChunkItem> _chunks;
        std::atomic<size_t> _size{0};
    };

    std::atomic<bool> _is_cancelled{false};
    std::atomic<int> _num_remaining_senders;

    typedef SpinLock Mutex;
    Mutex _lock;

    // one queue per driver sequence
    std::vector<ChunkQueue> _chunk_queues;
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