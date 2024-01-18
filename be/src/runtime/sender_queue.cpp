// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "runtime/sender_queue.h"

#include <atomic>

#include "column/chunk.h"
#include "gen_cpp/data.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/current_thread.h"
#include "runtime/data_stream_recvr.h"
#include "runtime/exec_env.h"
#include "util/compression/block_compression.h"
#include "util/faststring.h"
#include "util/logging.h"
#include "util/runtime_profile.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace starrocks {

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
    std::set<SlotId> hit_flags;
    for (auto tuple_desc : _recvr->_row_desc.tuple_descriptors()) {
        const std::vector<SlotDescriptor*>& slots = tuple_desc->slots();
        for (const auto& kv : _chunk_meta.slot_id_to_index) {
            //TODO: performance?
            for (auto slot : slots) {
                if (kv.first == slot->id()) {
                    _chunk_meta.types[kv.second] = slot->type();
                    ++column_index;
                    hit_flags.insert(kv.first);
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
        std::vector<std::pair<SlotId, size_t>> missing_pairs;
        for (const auto& kv : _chunk_meta.slot_id_to_index) {
            if (hit_flags.find(kv.first) == hit_flags.end()) {
                missing_pairs.emplace_back(kv.first, kv.second);
            }
        }
        std::stringstream ss;
        ss << "build chunk meta error";
        ss << ", node_id=" << _recvr->_dest_node_id;
        ss << ", missing pairs: ";
        for (const auto& kv : missing_pairs) {
            ss << "(slot:" << kv.first << ", index:" << kv.second << ") ";
        }
        std::string msg = ss.str();
        return Status::InternalError(msg);
    }
    return Status::OK();
}

Status DataStreamRecvr::SenderQueue::_deserialize_chunk(const ChunkPB& pchunk, vectorized::Chunk* chunk,
                                                        Metrics& metrics, faststring* uncompressed_buffer) {
    if (pchunk.compress_type() == CompressionTypePB::NO_COMPRESSION) {
        SCOPED_TIMER(metrics.deserialize_chunk_timer);
        TRY_CATCH_BAD_ALLOC({
            serde::ProtobufChunkDeserializer des(_chunk_meta, &pchunk, _recvr->get_encode_level());
            ASSIGN_OR_RETURN(*chunk, des.deserialize(pchunk.data()));
        });
    } else {
        size_t uncompressed_size = 0;
        {
            SCOPED_TIMER(metrics.decompress_chunk_timer);
            const BlockCompressionCodec* codec = nullptr;
            RETURN_IF_ERROR(get_block_compression_codec(pchunk.compress_type(), &codec));
            uncompressed_size = pchunk.uncompressed_size();
            TRY_CATCH_BAD_ALLOC(uncompressed_buffer->resize(uncompressed_size));
            Slice output{uncompressed_buffer->data(), uncompressed_size};
            RETURN_IF_ERROR(codec->decompress(pchunk.data(), &output));
        }
        {
            SCOPED_TIMER(metrics.deserialize_chunk_timer);
            TRY_CATCH_BAD_ALLOC({
                std::string_view buff(reinterpret_cast<const char*>(uncompressed_buffer->data()), uncompressed_size);
                serde::ProtobufChunkDeserializer des(_chunk_meta, &pchunk, _recvr->get_encode_level());
                ASSIGN_OR_RETURN(*chunk, des.deserialize(buff));
            });
        }
    }
    return Status::OK();
}

DataStreamRecvr::NonPipelineSenderQueue::NonPipelineSenderQueue(DataStreamRecvr* parent_recvr, int32_t num_senders)
        : SenderQueue(parent_recvr), _num_remaining_senders(num_senders) {}

Status DataStreamRecvr::NonPipelineSenderQueue::get_chunk(vectorized::Chunk** chunk, const int32_t driver_sequence) {
    std::unique_lock<Mutex> l(_lock);
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
    auto queue_enter_time = _chunk_queue.front().queue_enter_time;
    auto& metrics = _recvr->_metrics[0];

    _recvr->_num_buffered_bytes -= _chunk_queue.front().chunk_bytes;
    VLOG_ROW << "DataStreamRecvr fetched #rows=" << (*chunk)->num_rows();
    _chunk_queue.pop_front();

    if (closure != nullptr) {
        COUNTER_UPDATE(metrics.closure_block_timer, MonotonicNanos() - queue_enter_time);
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

bool DataStreamRecvr::NonPipelineSenderQueue::has_chunk() {
    std::lock_guard<Mutex> l(_lock);
    if (_is_cancelled) {
        return true;
    }

    if (_chunk_queue.empty() && _num_remaining_senders > 0) {
        return false;
    }

    return true;
}

// try_get_chunk will only be used when has_chunk return true(explicitly or implicitly).
bool DataStreamRecvr::NonPipelineSenderQueue::try_get_chunk(vectorized::Chunk** chunk) {
    std::lock_guard<Mutex> l(_lock);
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
        auto queue_enter_time = _chunk_queue.front().queue_enter_time;
        auto& metrics = _recvr->get_metrics_round_robin();
        VLOG_ROW << "DataStreamRecvr fetched #rows=" << (*chunk)->num_rows();
        _chunk_queue.pop_front();
        if (closure != nullptr) {
            COUNTER_UPDATE(metrics.closure_block_timer, MonotonicNanos() - queue_enter_time);
            closure->Run();
        }
        return true;
    }
}

Status DataStreamRecvr::NonPipelineSenderQueue::add_chunks(const PTransmitChunkParams& request, Metrics& metrics,
                                                           ::google::protobuf::Closure** done) {
    return add_chunks<false>(request, metrics, done);
}

Status DataStreamRecvr::NonPipelineSenderQueue::add_chunks_and_keep_order(const PTransmitChunkParams& request,
                                                                          Metrics& metrics,
                                                                          ::google::protobuf::Closure** done) {
    return add_chunks<true>(request, metrics, done);
}

template <bool keep_order>
Status DataStreamRecvr::NonPipelineSenderQueue::add_chunks(const PTransmitChunkParams& request, Metrics& metrics,
                                                           ::google::protobuf::Closure** done) {
    DCHECK(request.chunks_size() > 0);
    int32_t be_number = request.be_number();
    int64_t sequence = request.sequence();
    ScopedTimer<MonotonicStopWatch> wait_timer(metrics.wait_lock_timer);
    {
        std::lock_guard<Mutex> l(_lock);
        wait_timer.stop();
        if (_is_cancelled) {
            return Status::OK();
        }
        if (!keep_order) {
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
        } else {
            _max_processed_sequences.lazy_emplace(be_number, [be_number](const auto& ctor) { ctor(be_number, -1); });

            _buffered_chunk_queues.lazy_emplace(be_number, [be_number](const auto& ctor) {
                ctor(be_number, phmap::flat_hash_map<int64_t, ChunkQueue>());
            });
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
        // We only need to build chunk meta on first chunk
        if (_chunk_meta.types.empty()) {
            SCOPED_TIMER(metrics.deserialize_chunk_timer);
            auto& pchunk = request.chunks(0);
            RETURN_IF_ERROR(_build_chunk_meta(pchunk));
        }
    }

    ChunkQueue chunks;
    size_t total_chunk_bytes = 0;
    faststring uncompressed_buffer;

    for (auto i = 0; i < request.chunks().size(); ++i) {
        auto& pchunk = request.chunks().Get(i);
        int64_t chunk_bytes = pchunk.data().size();
        ChunkUniquePtr chunk = std::make_unique<vectorized::Chunk>();
        RETURN_IF_ERROR(_deserialize_chunk(pchunk, chunk.get(), metrics, &uncompressed_buffer));
        ChunkItem item{chunk_bytes, std::move(chunk), nullptr};
        chunks.emplace_back(std::move(item));
        total_chunk_bytes += chunk_bytes;
    }
    COUNTER_UPDATE(metrics.bytes_received_counter, total_chunk_bytes);

    wait_timer.start();
    {
        std::lock_guard<Mutex> l(_lock);
        wait_timer.stop();

        // _is_cancelled may be modified after checking _is_cancelled above,
        // because lock is release temporarily when deserializing chunk.
        if (_is_cancelled) {
            return Status::OK();
        }

        if (!keep_order) {
            const auto original_size = _chunk_queue.size();
            for (auto& item : chunks) {
                _chunk_queue.emplace_back(std::move(item));
            }
            bool has_new_chunks = _chunk_queue.size() > original_size;
            if (has_new_chunks && done != nullptr && _recvr->exceeds_limit(total_chunk_bytes)) {
                _chunk_queue.back().closure = *done;
                _chunk_queue.back().queue_enter_time = MonotonicNanos();
                COUNTER_UPDATE(metrics.closure_block_counter, 1);
                *done = nullptr;
            }
            _recvr->_num_buffered_bytes += total_chunk_bytes;
            _data_arrival_cv.notify_one();
        } else {
            auto& chunk_queues = _buffered_chunk_queues[be_number];

            if (!chunks.empty() && done != nullptr && _recvr->exceeds_limit(total_chunk_bytes)) {
                chunks.back().closure = *done;
                chunks.back().queue_enter_time = MonotonicNanos();
                COUNTER_UPDATE(metrics.closure_block_counter, 1);
                *done = nullptr;
            }

            // The queue in chunk_queues cannot be changed, so it must be
            // assigned to chunk_queues after local_chunk_queue is initialized
            // Otherwise, other threads may see the intermediate state because
            // the initialization of local_chunk_queue is beyond mutex
            chunk_queues[sequence] = std::move(chunks);

            phmap::flat_hash_map<int64_t, ChunkQueue>::iterator it;
            int64_t& max_processed_sequence = _max_processed_sequences[be_number];

            // max_processed_sequence + 1 means the first unprocessed sequence
            while ((it = chunk_queues.find(max_processed_sequence + 1)) != chunk_queues.end()) {
                ChunkQueue& unprocessed_chunk_queue = (*it).second;

                // Now, all the packets with sequance <= unprocessed_sequence have been received
                // so chunks of unprocessed_sequence can be flushed to ready queue
                for (auto& item : unprocessed_chunk_queue) {
                    _chunk_queue.emplace_back(std::move(item));
                }

                chunk_queues.erase(it);
                ++max_processed_sequence;
            }

            _recvr->_num_buffered_bytes += total_chunk_bytes;
        }
    }
    return Status::OK();
}

void DataStreamRecvr::NonPipelineSenderQueue::decrement_senders(int be_number) {
    std::lock_guard<Mutex> l(_lock);
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
        _data_arrival_cv.notify_all();
    }
}

void DataStreamRecvr::NonPipelineSenderQueue::cancel() {
    {
        std::lock_guard<Mutex> l(_lock);
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
        std::lock_guard<Mutex> l(_lock);
        clean_buffer_queues();
    }
}

void DataStreamRecvr::NonPipelineSenderQueue::close() {
    // If _is_cancelled is not set to true, there may be concurrent send
    // which add batch to _batch_queue. The batch added after _batch_queue
    // is clear will be memory leak
    std::lock_guard<Mutex> l(_lock);
    _is_cancelled = true;

    clean_buffer_queues();
}

void DataStreamRecvr::NonPipelineSenderQueue::clean_buffer_queues() {
    auto& metrics = _recvr->_metrics[0];
    for (auto& item : _chunk_queue) {
        if (item.closure != nullptr) {
            COUNTER_UPDATE(metrics.closure_block_timer, MonotonicNanos() - item.queue_enter_time);
            item.closure->Run();
        }
    }
    _chunk_queue.clear();
    for (auto& [_, chunk_queues] : _buffered_chunk_queues) {
        for (auto& [_, chunk_queue] : chunk_queues) {
            for (auto& item : chunk_queue) {
                if (item.closure != nullptr) {
                    item.closure->Run();
                }
            }
        }
    }
    _buffered_chunk_queues.clear();
}

DataStreamRecvr::PipelineSenderQueue::PipelineSenderQueue(DataStreamRecvr* parent_recvr, int32_t num_senders,
                                                          int32_t degree_of_parallism)
        : SenderQueue(parent_recvr), _num_remaining_senders(num_senders), _chunk_queue_states(degree_of_parallism) {
    for (int i = 0; i < degree_of_parallism; i++) {
        _chunk_queues.emplace_back();
    }

    if (parent_recvr->_is_merging) {
        _producer_token = std::make_unique<ChunkQueue::producer_token_t>(_chunk_queues[0]);
    }
}

Status DataStreamRecvr::PipelineSenderQueue::get_chunk(vectorized::Chunk** chunk, const int32_t driver_sequence) {
    if (_is_cancelled) {
        return Status::Cancelled("Cancelled SenderQueueForPipeline::get_chunk");
    }
    size_t index = _is_pipeline_level_shuffle ? driver_sequence : 0;
    auto& chunk_queue = _chunk_queues[index];
    auto& chunk_queue_state = _chunk_queue_states[index];
    auto& metrics = _recvr->_metrics[driver_sequence];

    ChunkItem item;
    if (!chunk_queue.try_dequeue(item)) {
        chunk_queue_state.unpluging = false;
        VLOG_ROW << "DataStreamRecvr no new data, stop unpluging";
        return Status::OK();
    }
    DeferOp defer_op([&]() {
        auto* closure = item.closure;
        if (closure != nullptr) {
#ifndef BE_TEST
            MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(ExecEnv::GetInstance()->process_mem_tracker());
            DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });
#endif
            COUNTER_UPDATE(metrics.closure_block_timer, MonotonicNanos() - item.queue_enter_time);
            closure->Run();
            chunk_queue_state.blocked_closure_num--;
        }
    });

    if (item.chunk_ptr == nullptr) {
        ChunkUniquePtr chunk_ptr = std::make_unique<vectorized::Chunk>();
        faststring uncompressed_buffer;
        RETURN_IF_ERROR(_deserialize_chunk(item.pchunk, chunk_ptr.get(), metrics, &uncompressed_buffer));
        *chunk = chunk_ptr.release();
    } else {
        *chunk = item.chunk_ptr.release();
    }
    VLOG_ROW << "DataStreamRecvr fetched #rows=" << (*chunk)->num_rows();

    _total_chunks--;
    _recvr->_num_buffered_bytes -= item.chunk_bytes;
    return Status::OK();
}

bool DataStreamRecvr::PipelineSenderQueue::has_chunk() {
    if (_is_cancelled) {
        return true;
    }
    if (_chunk_queues[0].size_approx() == 0 && _num_remaining_senders > 0) {
        return false;
    }
    return true;
}

bool DataStreamRecvr::PipelineSenderQueue::try_get_chunk(vectorized::Chunk** chunk) {
    if (_is_cancelled) {
        return false;
    }
    auto& chunk_queue = _chunk_queues[0];
    auto& chunk_queue_state = _chunk_queue_states[0];
    auto& metrics = _recvr->get_metrics_round_robin();
    ChunkItem item;
    if (!chunk_queue.try_dequeue(item)) {
        return false;
    }
    DCHECK(item.chunk_ptr != nullptr);
    *chunk = item.chunk_ptr.release();
    VLOG_ROW << "DataStreamRecvr fetched #rows=" << (*chunk)->num_rows();
    auto* closure = item.closure;
    if (closure != nullptr) {
        COUNTER_UPDATE(metrics.closure_block_timer, MonotonicNanos() - item.queue_enter_time);
        closure->Run();
        chunk_queue_state.blocked_closure_num--;
    }
    _total_chunks--;
    _recvr->_num_buffered_bytes -= item.chunk_bytes;
    return true;
}

Status DataStreamRecvr::PipelineSenderQueue::add_chunks(const PTransmitChunkParams& request, Metrics& metrics,
                                                        ::google::protobuf::Closure** done) {
    return add_chunks<false>(request, metrics, done);
}

Status DataStreamRecvr::PipelineSenderQueue::add_chunks_and_keep_order(const PTransmitChunkParams& request,
                                                                       Metrics& metrics,
                                                                       ::google::protobuf::Closure** done) {
    return add_chunks<true>(request, metrics, done);
}

void DataStreamRecvr::PipelineSenderQueue::decrement_senders(int be_number) {
    {
        std::lock_guard<Mutex> l(_lock);
        if (_sender_eos_set.find(be_number) != _sender_eos_set.end()) {
            return;
        }
        _sender_eos_set.insert(be_number);
    }
    _num_remaining_senders--;
    VLOG_FILE << "decremented senders: fragment_instance_id=" << print_id(_recvr->fragment_instance_id())
              << " node_id=" << _recvr->dest_node_id() << " #senders=" << _num_remaining_senders
              << " be_number=" << be_number;
}

void DataStreamRecvr::PipelineSenderQueue::cancel() {
    _is_cancelled = true;
    clean_buffer_queues();
}

void DataStreamRecvr::PipelineSenderQueue::close() {
    clean_buffer_queues();
}

void DataStreamRecvr::PipelineSenderQueue::clean_buffer_queues() {
    std::lock_guard<Mutex> l(_lock);
    auto& metrics = _recvr->_metrics[0];
    for (size_t i = 0; i < _chunk_queues.size(); i++) {
        auto& chunk_queue = _chunk_queues[i];
        auto& chunk_queue_state = _chunk_queue_states[i];
        ChunkItem item;
        while (chunk_queue.size_approx() > 0) {
            if (chunk_queue.try_dequeue(item)) {
                if (item.closure != nullptr) {
                    COUNTER_UPDATE(metrics.closure_block_timer, MonotonicNanos() - item.queue_enter_time);
                    item.closure->Run();
                    chunk_queue_state.blocked_closure_num--;
                }
                --_total_chunks;
                _recvr->_num_buffered_bytes -= item.chunk_bytes;
            }
        }
    }

    for (auto& [_, chunk_queues] : _buffered_chunk_queues) {
        for (auto& [_, chunk_queue] : chunk_queues) {
            for (auto& item : chunk_queue) {
                if (item.closure != nullptr) {
                    COUNTER_UPDATE(metrics.closure_block_timer, MonotonicNanos() - item.queue_enter_time);
                    item.closure->Run();
                }
            }
            chunk_queue.clear();
        }
    }
}

void DataStreamRecvr::PipelineSenderQueue::check_leak_closure() {
    std::lock_guard<Mutex> l(_lock);
    for (size_t i = 0; i < _chunk_queues.size(); i++) {
        auto& chunk_queue = _chunk_queues[i];
        ChunkItem item;
        while (chunk_queue.size_approx() > 0) {
            if (chunk_queue.try_dequeue(item)) {
                if (item.closure != nullptr) {
                    DCHECK(false) << "leak closure detected";
                    LOG(WARNING) << "leak closure detected in fragment:" << print_id(_recvr->fragment_instance_id());
                }
            }
        }
    }

    for (auto& [_, chunk_queues] : _buffered_chunk_queues) {
        for (auto& [_, chunk_queue] : chunk_queues) {
            for (auto& item : chunk_queue) {
                if (item.closure != nullptr) {
                    DCHECK(false) << "leak closure detected";
                    LOG(WARNING) << "leak closure detected in fragment:" << print_id(_recvr->fragment_instance_id());
                }
            }
        }
    }
}

Status DataStreamRecvr::PipelineSenderQueue::try_to_build_chunk_meta(const PTransmitChunkParams& request,
                                                                     Metrics& metrics) {
    ScopedTimer<MonotonicStopWatch> wait_timer(metrics.wait_lock_timer);
    std::lock_guard<Mutex> l(_lock);
    wait_timer.stop();
    // We only need to build chunk meta on first chunk and not use_pass_through
    // By using pass through, chunks are transmitted in shared memory without ser/deser
    // So there is no need to build chunk meta.
    if (_chunk_meta.types.empty() && !request.use_pass_through()) {
        SCOPED_TIMER(metrics.deserialize_chunk_timer);
        auto& pchunk = request.chunks(0);
        return _build_chunk_meta(pchunk);
    }
    return Status::OK();
}

StatusOr<DataStreamRecvr::PipelineSenderQueue::ChunkList>
DataStreamRecvr::PipelineSenderQueue::get_chunks_from_pass_through(int32_t sender_id, size_t& total_chunk_bytes) {
    ChunkUniquePtrVector swap_chunks;
    std::vector<size_t> swap_bytes;
    _recvr->_pass_through_context.pull_chunks(sender_id, &swap_chunks, &swap_bytes);
    DCHECK(swap_chunks.size() == swap_bytes.size());
    ChunkList chunks;
    for (size_t i = 0; i < swap_chunks.size(); i++) {
        // The sending and receiving of chunks from _pass_through_context may out of order, and
        // considering the following sequences:
        // 1. add chunk_1 to _pass_through_context and send request_1
        // 2. add chunk_2 to _pass_through_context and send request_2
        // 3. receive request_1 and get both chunk_1 and chunk_2
        // 4. receive request_2 and get nothing
        // So one receiving may receive two or more chunks, and we need to use the chunk's driver_sequence
        // but not the request's driver_sequence
        chunks.emplace_back(swap_bytes[i], swap_chunks[i].second, nullptr, std::move(swap_chunks[i].first));
        total_chunk_bytes += swap_bytes[i];
    }
    return chunks;
}

template <bool need_deserialization>
StatusOr<DataStreamRecvr::PipelineSenderQueue::ChunkList> DataStreamRecvr::PipelineSenderQueue::get_chunks_from_request(
        const PTransmitChunkParams& request, Metrics& metrics, size_t& total_chunk_bytes) {
    ChunkList chunks;
    faststring uncompressed_buffer;
    for (auto i = 0; i < request.chunks().size(); i++) {
        auto& pchunk = request.chunks().Get(i);
        int32_t driver_sequence = _is_pipeline_level_shuffle ? request.driver_sequences(i) : -1;
        int64_t chunk_bytes = pchunk.data().size();
        if constexpr (need_deserialization) {
            ChunkUniquePtr chunk = std::make_unique<vectorized::Chunk>();
            RETURN_IF_ERROR(_deserialize_chunk(pchunk, chunk.get(), metrics, &uncompressed_buffer));
            chunks.emplace_back(chunk_bytes, driver_sequence, nullptr, std::move(chunk));
        } else {
            chunks.emplace_back(chunk_bytes, driver_sequence, nullptr, pchunk);
        }
        total_chunk_bytes += chunk_bytes;
    }
    return chunks;
}

template <bool keep_order>
Status DataStreamRecvr::PipelineSenderQueue::add_chunks(const PTransmitChunkParams& request, Metrics& metrics,
                                                        ::google::protobuf::Closure** done) {
    if (keep_order) {
        DCHECK(!request.has_is_pipeline_level_shuffle() && !request.is_pipeline_level_shuffle());
    }
    const bool use_pass_through = request.use_pass_through();
    DCHECK(!(keep_order && use_pass_through));
    DCHECK(request.chunks_size() > 0 || use_pass_through);
    if (_is_cancelled || _num_remaining_senders <= 0) {
        return Status::OK();
    }

    RETURN_IF_ERROR(try_to_build_chunk_meta(request, metrics));

    size_t total_chunk_bytes = 0;
    _is_pipeline_level_shuffle = request.has_is_pipeline_level_shuffle() && request.is_pipeline_level_shuffle();

    // NOTE: in the merge scenario, chunk is obtained through try_get_chunk and its return type is not Status.
    // there is no chance to handle deserialize error, so the lazy deserialization is not supported now,
    // we can change related interface's defination to do this later.
    ChunkList chunks;
<<<<<<< HEAD
    ASSIGN_OR_RETURN(chunks, use_pass_through
                                     ? get_chunks_from_pass_through(request.sender_id(), total_chunk_bytes)
                                     : (keep_order ? get_chunks_from_request<true>(request, total_chunk_bytes)
                                                   : get_chunks_from_request<false>(request, total_chunk_bytes)));
    COUNTER_UPDATE(use_pass_through ? _recvr->_bytes_pass_through_counter : _recvr->_bytes_received_counter,
=======
    ASSIGN_OR_RETURN(chunks,
                     use_pass_through
                             ? get_chunks_from_pass_through(request.sender_id(), total_chunk_bytes)
                             : (keep_order ? get_chunks_from_request<true>(request, metrics, total_chunk_bytes)
                                           : get_chunks_from_request<false>(request, metrics, total_chunk_bytes)));
    COUNTER_UPDATE(use_pass_through ? metrics.bytes_pass_through_counter : metrics.bytes_received_counter,
>>>>>>> 2.5.18
                   total_chunk_bytes);

    if (_is_cancelled) {
        return Status::OK();
    }

    if (keep_order) {
        const int32_t be_number = request.be_number();
        const int32_t sequence = request.sequence();
        ScopedTimer<MonotonicStopWatch> wait_timer(metrics.wait_lock_timer);
        std::lock_guard<Mutex> l(_lock);
        wait_timer.stop();

        if (_is_cancelled) {
            LOG(ERROR) << "Cancelled receiver cannot add_chunk for keep order!";
            return Status::OK();
        }

        _max_processed_sequences.lazy_emplace(be_number, [be_number](const auto& ctor) { ctor(be_number, -1); });

        _buffered_chunk_queues.lazy_emplace(be_number, [be_number](const auto& ctor) {
            ctor(be_number, phmap::flat_hash_map<int64_t, ChunkList>());
        });

        auto& chunk_queues = _buffered_chunk_queues[be_number];

        if (!chunks.empty() && done != nullptr && _recvr->exceeds_limit(total_chunk_bytes)) {
            chunks.back().closure = *done;
            chunks.back().queue_enter_time = MonotonicNanos();
            COUNTER_UPDATE(metrics.closure_block_counter, 1);
            *done = nullptr;
        }

        // The queue in chunk_queues cannot be changed, so it must be
        // assigned to chunk_queues after local_chunk_queue is initialized
        // Otherwise, other threads may see the intermediate state because
        // the initialization of local_chunk_queue is beyond mutex
        chunk_queues[sequence] = std::move(chunks);

        phmap::flat_hash_map<int64_t, ChunkList>::iterator it;
        int64_t& max_processed_sequence = _max_processed_sequences[be_number];

        // max_processed_sequence + 1 means the first unprocessed sequence
        while ((it = chunk_queues.find(max_processed_sequence + 1)) != chunk_queues.end()) {
            ChunkList& unprocessed_chunk_queue = (*it).second;

            // Now, all the packets with sequance <= unprocessed_sequence have been received
            // so chunks of unprocessed_sequence can be flushed to ready queue
            for (auto& item : unprocessed_chunk_queue) {
                size_t chunk_bytes = item.chunk_bytes;
                auto* closure = item.closure;
                _chunk_queues[0].enqueue(*_producer_token, std::move(item));
                _chunk_queue_states[0].blocked_closure_num += closure != nullptr;
                _total_chunks++;
                _recvr->_num_buffered_bytes += chunk_bytes;
            }

            chunk_queues.erase(it);
            ++max_processed_sequence;
        }
    } else {
        // NOTICE: The enqueue process use a lock-free approach to avoid lock contention,
        // and double check is introduced to handle the exception cases like short circuit and cancel.
        // And it may lead to closure leak if it is not well handled.

        // remove the short-circuited chunks
        for (auto iter = chunks.begin(); iter != chunks.end();) {
            if (_is_pipeline_level_shuffle &&
                // First check here for short circuit compatibility without introducing a critical section
                _chunk_queue_states[iter->driver_sequence].is_short_circuited.load(std::memory_order_relaxed)) {
                total_chunk_bytes -= iter->chunk_bytes;
                chunks.erase(iter++);
                continue;
            }
            iter++;
        }

        if (!chunks.empty() && done != nullptr && _recvr->exceeds_limit(total_chunk_bytes)) {
            chunks.back().closure = *done;
            chunks.back().queue_enter_time = MonotonicNanos();
            COUNTER_UPDATE(metrics.closure_block_counter, 1);
            *done = nullptr;
        }

        for (auto& chunk : chunks) {
            int index = _is_pipeline_level_shuffle ? chunk.driver_sequence : 0;
            size_t chunk_bytes = chunk.chunk_bytes;
            auto* closure = chunk.closure;
            _chunk_queues[index].enqueue(std::move(chunk));
            _chunk_queue_states[index].blocked_closure_num += closure != nullptr;
            _total_chunks++;
            // Double check here for short circuit compatibility without introducing a critical section
            if (_chunk_queue_states[index].is_short_circuited.load(std::memory_order_relaxed)) {
                short_circuit(index);
                // We cannot early-return for short circuit, it may occur for parts of parallelism,
                // and the other parallelism may need to proceed.
            }
            _recvr->_num_buffered_bytes += chunk_bytes;
        }
    }

    // if senderqueue is cancelled clear all closure buffers
    if (_is_cancelled) {
        clean_buffer_queues();
    }

    return Status::OK();
}

void DataStreamRecvr::PipelineSenderQueue::short_circuit(const int32_t driver_sequence) {
    auto& chunk_queue_state = _chunk_queue_states[driver_sequence];
    auto& metrics = _recvr->_metrics[driver_sequence];
    chunk_queue_state.is_short_circuited.store(true, std::memory_order_relaxed);
    if (_is_pipeline_level_shuffle) {
        auto& chunk_queue = _chunk_queues[driver_sequence];
        ChunkItem item;
        while (chunk_queue.size_approx() > 0) {
            if (chunk_queue.try_dequeue(item)) {
                if (item.closure != nullptr) {
                    COUNTER_UPDATE(metrics.closure_block_timer, MonotonicNanos() - item.queue_enter_time);
                    item.closure->Run();
                    chunk_queue_state.blocked_closure_num--;
                }
                --_total_chunks;
                _recvr->_num_buffered_bytes -= item.chunk_bytes;
            }
        }
    }
}

bool DataStreamRecvr::PipelineSenderQueue::has_output(const int32_t driver_sequence) {
    if (_is_cancelled.load()) {
        return false;
    }

    size_t index = _is_pipeline_level_shuffle ? driver_sequence : 0;
    size_t chunk_num = _chunk_queues[index].size_approx();
    auto& chunk_queue_state = _chunk_queue_states[index];
    auto& metrics = _recvr->_metrics[driver_sequence];
    // introduce an unplug mechanism similar to scan operator to reduce scheduling overhead

    // 1. in the unplug state, return true if there is a chunk, otherwise return false and exit the unplug state
    if (chunk_queue_state.unpluging) {
        if (chunk_num > 0) {
            return true;
        }
        chunk_queue_state.unpluging = false;
        return false;
    }
    // 2. if this queue is not in the unplug state, try to batch as much chunk as possible before returning
    // @TODO need an adaptive strategy to determin this threshold
    if (chunk_num >= kUnplugBufferThreshold) {
        COUNTER_UPDATE(metrics.buffer_unplug_counter, 1);
        chunk_queue_state.unpluging = true;
        return true;
    }

    bool is_buffer_full = _recvr->_num_buffered_bytes > _recvr->_total_buffer_limit;
    // 3. if buffer is full and this queue has chunks, return true to release the buffer capacity ASAP
    if (is_buffer_full && chunk_num > 0) {
        return true;
    }
    // 4. if there is no new data, return true if this queue has chunks
    if (_num_remaining_senders == 0) {
        return chunk_num > 0;
    }
    // 5. if this queue has blocked closures, return true to release the closure ASAP to trigger the next transmit requests
    return chunk_queue_state.blocked_closure_num > 0;
}

bool DataStreamRecvr::PipelineSenderQueue::is_finished() const {
    return _is_cancelled || (_num_remaining_senders == 0 && _total_chunks == 0);
}

} // namespace starrocks
