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

#include <google/protobuf/stubs/common.h>

#include <condition_variable>
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "column/chunk.h"
#include "gen_cpp/data.pb.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/row_batch.h"
#include "runtime/sorted_run_merger.h"
#include "runtime/vectorized/sorted_chunks_merger.h"
#include "util/block_compression.h"
#include "util/debug_util.h"
#include "util/faststring.h"
#include "util/logging.h"
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
    SenderQueue(DataStreamRecvr* parent_recvr, int num_senders);

    ~SenderQueue() = default;

    // Return the next batch form this sender queue. Sets the returned batch in _cur_batch.
    // A returned batch that is not filled to capacity does *not* indicate
    // end-of-stream.
    // The call blocks until another batch arrives or all senders close
    // their channels. The returned batch is owned by the sender queue. The caller
    // must acquire data from the returned batch before the next call to get_batch().
    Status get_batch(RowBatch** next_batch);
    Status get_chunk(vectorized::Chunk** chunk);

    // check if data has come, work with try_get_chunk.
    bool has_chunk();
    // Probe for chunks, because _chunk_queue maybe empty when data hasn't come yet.
    // So compute thread should do other works.
    bool try_get_chunk(vectorized::Chunk** chunk);

    // Adds a row batch to this sender queue if this stream has not been cancelled;
    // blocks if this will make the stream exceed its buffer limit.
    // If the total size of the batches in this queue would exceed the allowed buffer size,
    // the queue is considered full and the call blocks until a batch is dequeued.
    void add_batch(const PRowBatch& pb_batch, int be_number, int64_t packet_seq, ::google::protobuf::Closure** done);

    // Adds a column chunk to this sender queue if this stream has not been cancelled;
    // blocks if this will make the stream exceed its buffer limit.
    // If the total size of the chunks in this queue would exceed the allowed buffer size,
    // the queue is considered full and the call blocks until a chunk is dequeued.
    Status add_chunks(const PTransmitChunkParams& request, ::google::protobuf::Closure** done);

    // add_chunks_for_pipeline is almost the same like add_chunks except that it didn't
    // notify compute thread to grab chunks, compute thread is notified by pipeline's dispatch thread.
    Status add_chunks_for_pipeline(const PTransmitChunkParams& request, ::google::protobuf::Closure** done);

    // Decrement the number of remaining senders for this queue and signal eos ("new data")
    // if the count drops to 0. The number of senders will be 1 for a merging
    // DataStreamRecvr.
    void decrement_senders(int be_number);

    // Set cancellation flag and signal cancellation to receiver and sender. Subsequent
    // incoming batches will be dropped.
    void cancel();

    // Must be called once to cleanup any queued resources.
    void close();

    // Returns the current batch from this queue being processed by a consumer.
    RowBatch* current_batch() const { return _current_batch.get(); }

    bool has_output() const;

    bool is_finished() const;

private:
    // _add_chunks_internal is called by add_chunks and add_chunks_for_pipeline
    Status _add_chunks_internal(const PTransmitChunkParams& request, ::google::protobuf::Closure** done,
                                const std::function<void()>& cb);

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

    // queue of (batch length, batch) pairs.  The SenderQueue block owns memory to
    // these batches. They are handed off to the caller via get_batch.
    typedef list<pair<int, RowBatch*>> RowBatchQueue;
    RowBatchQueue _batch_queue;

    typedef std::list<std::pair<int, ChunkUniquePtr>> ChunkQueue;
    ChunkQueue _chunk_queue;
    vectorized::RuntimeChunkMeta _chunk_meta;
    vectorized::Buffer<uint8_t> _uncompressed_chunk_data;

    // The batch that was most recently returned via get_batch(), i.e. the current batch
    // from this queue being processed by a consumer. Is destroyed when the next batch
    // is retrieved.
    std::unique_ptr<RowBatch> _current_batch;

    // Set to true when the first batch has been received
    bool _received_first_batch;

    std::unordered_set<int> _sender_eos_set;          // sender_id
    std::unordered_map<int, int64_t> _packet_seq_map; // be_number => packet_seq

    std::deque<std::pair<google::protobuf::Closure*, MonotonicStopWatch>> _pending_closures;
};

DataStreamRecvr::SenderQueue::SenderQueue(DataStreamRecvr* parent_recvr, int num_senders)
        : _recvr(parent_recvr),
          _is_cancelled(false),
          _num_remaining_senders(num_senders),
          _received_first_batch(false) {}

Status DataStreamRecvr::SenderQueue::get_batch(RowBatch** next_batch) {
    std::unique_lock<std::mutex> l(_lock);
    // wait until something shows up or we know we're done
    while (!_is_cancelled && _batch_queue.empty() && _num_remaining_senders > 0) {
        VLOG_ROW << "wait arrival fragment_instance_id=" << _recvr->fragment_instance_id()
                 << " node=" << _recvr->dest_node_id();
        // Don't count time spent waiting on the sender as active time.
        CANCEL_SAFE_SCOPED_TIMER(_recvr->_data_arrival_timer, &_is_cancelled);
        CANCEL_SAFE_SCOPED_TIMER(_received_first_batch ? NULL : _recvr->_first_batch_wait_total_timer, &_is_cancelled);
        _data_arrival_cv.wait(l);
    }

    // _cur_batch must be replaced with the returned batch.
    _current_batch.reset();
    *next_batch = NULL;
    if (_is_cancelled) {
        return Status::Cancelled("Cancelled SenderQueue::get_batch");
    }

    if (_batch_queue.empty()) {
        DCHECK_EQ(_num_remaining_senders, 0);
        return Status::OK();
    }

    _received_first_batch = true;

    DCHECK(!_batch_queue.empty());
    RowBatch* result = _batch_queue.front().second;
    _recvr->_num_buffered_bytes -= _batch_queue.front().first;
    VLOG_ROW << "fetched #rows=" << result->num_rows();
    _batch_queue.pop_front();
    _current_batch.reset(result);
    *next_batch = _current_batch.get();

    if (!_pending_closures.empty()) {
        auto closure_pair = _pending_closures.front();
        closure_pair.first->Run();
        _pending_closures.pop_front();

        closure_pair.second.stop();
        _recvr->_buffer_full_total_timer->update(closure_pair.second.elapsed_time());
    }

    return Status::OK();
}

bool DataStreamRecvr::SenderQueue::has_output() const {
    std::lock_guard<std::mutex> l(_lock);
    return !_is_cancelled && !_chunk_queue.empty();
}

bool DataStreamRecvr::SenderQueue::is_finished() const {
    std::lock_guard<std::mutex> l(_lock);
    return _is_cancelled || (_num_remaining_senders == 0 && _chunk_queue.empty());
}

bool DataStreamRecvr::SenderQueue::has_chunk() {
    std::unique_lock<std::mutex> l(_lock);
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
    std::unique_lock<std::mutex> l(_lock);
    if (_is_cancelled) {
        return false;
    }

    if (_chunk_queue.empty()) {
        DCHECK_EQ(_num_remaining_senders, 0);
        return false;
    } else {
        *chunk = _chunk_queue.front().second.release();
        _recvr->_num_buffered_bytes -= _chunk_queue.front().first;
        VLOG_ROW << "DataStreamRecvr fetched #rows=" << (*chunk)->num_rows();
        _chunk_queue.pop_front();
        if (!_pending_closures.empty()) {
            auto closure_pair = _pending_closures.front();
            closure_pair.first->Run();
            _pending_closures.pop_front();
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
        DCHECK_EQ(_num_remaining_senders, 0);
        return Status::OK();
    }

    *chunk = _chunk_queue.front().second.release();
    _recvr->_num_buffered_bytes -= _chunk_queue.front().first;
    VLOG_ROW << "DataStreamRecvr fetched #rows=" << (*chunk)->num_rows();
    _chunk_queue.pop_front();

    if (!_pending_closures.empty()) {
        auto closure_pair = _pending_closures.front();
        closure_pair.first->Run();
        _pending_closures.pop_front();
    }

    return Status::OK();
}

void DataStreamRecvr::SenderQueue::add_batch(const PRowBatch& pb_batch, int be_number, int64_t packet_seq,
                                             ::google::protobuf::Closure** done) {
    std::unique_lock<std::mutex> l(_lock);
    if (_is_cancelled) {
        return;
    }
    auto iter = _packet_seq_map.find(be_number);
    if (iter != _packet_seq_map.end()) {
        if (iter->second >= packet_seq) {
            LOG(WARNING) << "packet already exist [cur_packet_id= " << iter->second
                         << " receive_packet_id=" << packet_seq << "]";
            return;
        }
        iter->second = packet_seq;
    } else {
        _packet_seq_map.emplace(be_number, packet_seq);
    }

    int batch_size = RowBatch::get_batch_size(pb_batch);
    COUNTER_UPDATE(_recvr->_bytes_received_counter, batch_size);

    // Following situation will match the following condition.
    // Sender send a packet failed, then close the channel.
    // but closed packet reach first, then the failed packet.
    // Then meet the assert
    // we remove the assert
    // DCHECK_GT(_num_remaining_senders, 0);
    if (_num_remaining_senders <= 0) {
        DCHECK(_sender_eos_set.end() != _sender_eos_set.find(be_number));
        return;
    }

    // We always accept the batch regardless of buffer limit, to avoid rpc pipeline stall.
    // If exceed buffer limit, we just do not response ACK to client, so the client won't
    // send data until ACK received.
    // Note that if this be needs to receive data from N BEs, the size of buffer
    // may reach as many as (buffer_size + n * buffer_size)
    //
    // Note: It's important that we enqueue thrift_batch regardless of buffer limit if
    //  the queue is currently empty. In the case of a merging receiver, batches are
    //  received from a specific queue based on data order, and the pipeline will stall
    //  if the merger is waiting for data from an empty queue that cannot be filled
    //  because the limit has been reached.
    if (_is_cancelled) {
        return;
    }

    RowBatch* batch = NULL;
    {
        SCOPED_TIMER(_recvr->_deserialize_row_batch_timer);
        // Note: if this function makes a row batch, the batch *must* be added
        // to _batch_queue. It is not valid to create the row batch and destroy
        // it in this thread.
        batch = new RowBatch(_recvr->row_desc(), pb_batch, _recvr->mem_tracker());
    }

    VLOG_ROW << "added #rows=" << batch->num_rows() << " batch_size=" << batch_size << "\n";
    _batch_queue.emplace_back(batch_size, batch);

    // if done is nullptr, this function can't delay this response
    if (done != nullptr && _recvr->exceeds_limit(batch_size)) {
        MonotonicStopWatch monotonicStopWatch;
        monotonicStopWatch.start();
        DCHECK(*done != nullptr);
        _pending_closures.emplace_back(*done, monotonicStopWatch);
        *done = nullptr;
    }
    _recvr->_num_buffered_bytes += batch_size;
    _data_arrival_cv.notify_one();
}

Status DataStreamRecvr::SenderQueue::_build_chunk_meta(const ChunkPB& pb_chunk) {
    if (UNLIKELY(pb_chunk.is_nulls().empty() || pb_chunk.slot_id_map().empty())) {
        return Status::InternalError("pb_chunk meta could not be empty");
    }

    _chunk_meta.slot_id_to_index.init(pb_chunk.slot_id_map().size());
    for (int i = 0; i < pb_chunk.slot_id_map().size(); i += 2) {
        _chunk_meta.slot_id_to_index.insert(pb_chunk.slot_id_map()[i], pb_chunk.slot_id_map()[i + 1]);
    }

    _chunk_meta.tuple_id_to_index.init(pb_chunk.tuple_id_map().size());
    for (int i = 0; i < pb_chunk.tuple_id_map().size(); i += 2) {
        _chunk_meta.tuple_id_to_index.insert(pb_chunk.tuple_id_map()[i], pb_chunk.tuple_id_map()[i + 1]);
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
    for (size_t i = 0; i < _recvr->_row_desc.tuple_descriptors().size(); i++) {
        TupleDescriptor* tuple_desc = _recvr->_row_desc.tuple_descriptors()[i];
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

Status DataStreamRecvr::SenderQueue::_add_chunks_internal(const PTransmitChunkParams& request,
                                                          ::google::protobuf::Closure** done,
                                                          const std::function<void()>& cb) {
    DCHECK(request.chunks_size() > 0);

    int32_t be_number = request.be_number();
    int64_t sequence = request.sequence();
    ScopedTimer<MonotonicStopWatch> wait_timer(_recvr->_sender_wait_lock_timer);
    {
        std::unique_lock<std::mutex> l(_lock);
        wait_timer.stop();
        if (_is_cancelled) {
            return Status::OK();
        }
        // TODO(zc): Do we really need this check?
        auto iter = _packet_seq_map.find(be_number);
        if (iter != _packet_seq_map.end()) {
            if (iter->second >= sequence) {
                LOG(WARNING) << "packet already exist [cur_packet_id= " << iter->second
                             << " receive_packet_id=" << sequence << "]";
                return Status::OK();
            }
            iter->second = sequence;
        } else {
            _packet_seq_map.emplace(be_number, sequence);
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
        if (_chunk_meta.types.empty()) {
            SCOPED_TIMER(_recvr->_deserialize_row_batch_timer);
            auto& pchunk = request.chunks(0);
            RETURN_IF_ERROR(_build_chunk_meta(pchunk));
        }
    }

    ChunkQueue chunks;
    size_t total_chunk_bytes = 0;
    faststring uncompressed_buffer;
    for (auto& pchunk : request.chunks()) {
        size_t chunk_bytes = pchunk.data().size();
        ChunkUniquePtr chunk = std::make_unique<vectorized::Chunk>();
        RETURN_IF_ERROR(_deserialize_chunk(pchunk, chunk.get(), &uncompressed_buffer));

        // TODO(zc): review this chunk_bytes
        chunks.emplace_back(chunk_bytes, std::move(chunk));

        total_chunk_bytes += chunk_bytes;
    }
    COUNTER_UPDATE(_recvr->_bytes_received_counter, total_chunk_bytes);

    wait_timer.start();
    {
        std::unique_lock<std::mutex> l(_lock);
        wait_timer.stop();

        for (auto& pair : chunks) {
            _chunk_queue.emplace_back(std::move(pair));
        }
        // if done is nullptr, this function can't delay this response
        if (done != nullptr && _recvr->exceeds_limit(total_chunk_bytes)) {
            MonotonicStopWatch monotonicStopWatch;
            DCHECK(*done != nullptr);
            _pending_closures.emplace_back(*done, monotonicStopWatch);
            *done = nullptr;
        }
        _recvr->_num_buffered_bytes += total_chunk_bytes;
    }
    cb();
    return Status::OK();
}

Status DataStreamRecvr::SenderQueue::add_chunks(const PTransmitChunkParams& request,
                                                ::google::protobuf::Closure** done) {
    auto& condition = _data_arrival_cv;
    return _add_chunks_internal(request, done, [&condition]() -> void { condition.notify_one(); });
}

Status DataStreamRecvr::SenderQueue::add_chunks_for_pipeline(const PTransmitChunkParams& request,
                                                             ::google::protobuf::Closure** done) {
    return _add_chunks_internal(request, done, []() -> void {});
}

Status DataStreamRecvr::SenderQueue::_deserialize_chunk(const ChunkPB& pchunk, vectorized::Chunk* chunk,
                                                        faststring* uncompressed_buffer) {
    if (pchunk.compress_type() == CompressionTypePB::NO_COMPRESSION) {
        SCOPED_TIMER(_recvr->_deserialize_row_batch_timer);
        RETURN_IF_ERROR(chunk->deserialize((const uint8_t*)pchunk.data().data(), pchunk.data().size(), _chunk_meta));
    } else {
        size_t uncompressed_size = 0;
        {
            SCOPED_TIMER(_recvr->_decompress_row_batch_timer);
            const BlockCompressionCodec* codec = nullptr;
            RETURN_IF_ERROR(get_block_compression_codec(pchunk.compress_type(), &codec));
            uncompressed_size = pchunk.uncompressed_size();
            uncompressed_buffer->resize(uncompressed_size);
            Slice output{uncompressed_buffer->data(), uncompressed_size};
            RETURN_IF_ERROR(codec->decompress(pchunk.data(), &output));
        }
        {
            SCOPED_TIMER(_recvr->_deserialize_row_batch_timer);
            RETURN_IF_ERROR(chunk->deserialize(uncompressed_buffer->data(), uncompressed_size, _chunk_meta));
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
    VLOG_FILE << "decremented senders: fragment_instance_id=" << _recvr->fragment_instance_id()
              << " node_id=" << _recvr->dest_node_id() << " #senders=" << _num_remaining_senders;
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
        for (auto closure_pair : _pending_closures) {
            closure_pair.first->Run();
        }
        _pending_closures.clear();
    }
}

void DataStreamRecvr::SenderQueue::close() {
    {
        // If _is_cancelled is not set to true, there may be concurrent send
        // which add batch to _batch_queue. The batch added after _batch_queue
        // is clear will be memory leak
        std::lock_guard<std::mutex> l(_lock);
        _is_cancelled = true;

        for (auto closure_pair : _pending_closures) {
            closure_pair.first->Run();
        }
        _pending_closures.clear();
    }

    // Delete any batches queued in _batch_queue
    for (RowBatchQueue::iterator it = _batch_queue.begin(); it != _batch_queue.end(); ++it) {
        delete it->second;
    }

    _current_batch.reset();
}

Status DataStreamRecvr::create_merger(const TupleRowComparator& less_than) {
    DCHECK(_is_merging);
    vector<SortedRunMerger::RunBatchSupplier> input_batch_suppliers;
    input_batch_suppliers.reserve(_sender_queues.size());

    // Create the merger that will a single stream of sorted rows.
    _merger.reset(new SortedRunMerger(less_than, &_row_desc, _profile.get(), false));

    for (SenderQueue* q : _sender_queues) {
        auto f = [q](RowBatch** batch) -> Status { return q->get_batch(batch); };
        input_batch_suppliers.emplace_back(std::move(f));
    }
    RETURN_IF_ERROR(_merger->prepare(input_batch_suppliers));
    return Status::OK();
}

Status DataStreamRecvr::create_merger(const SortExecExprs* exprs, const std::vector<bool>* is_asc,
                                      const std::vector<bool>* is_null_first) {
    DCHECK(_is_merging);
    _chunks_merger = std::make_unique<vectorized::SortedChunksMerger>(_is_pipeline);
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

Status DataStreamRecvr::create_merger_for_pipeline(const SortExecExprs* exprs, const std::vector<bool>* is_asc,
                                                   const std::vector<bool>* is_null_first) {
    DCHECK(_is_merging);
    _chunks_merger = std::make_unique<vectorized::SortedChunksMerger>(_is_pipeline);
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

void DataStreamRecvr::transfer_all_resources(RowBatch* transfer_batch) {
    for (SenderQueue* sender_queue : _sender_queues) {
        if (sender_queue->current_batch() != NULL) {
            sender_queue->current_batch()->transfer_resource_ownership(transfer_batch);
        }
    }
}

DataStreamRecvr::DataStreamRecvr(DataStreamMgr* stream_mgr, MemTracker* parent_tracker, const RowDescriptor& row_desc,
                                 const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int num_senders,
                                 bool is_merging, int total_buffer_limit, std::shared_ptr<RuntimeProfile> profile,
                                 std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr,
                                 bool is_pipeline)
        : _mgr(stream_mgr),
          _fragment_instance_id(fragment_instance_id),
          _dest_node_id(dest_node_id),
          _total_buffer_limit(total_buffer_limit),
          _row_desc(row_desc),
          _is_merging(is_merging),
          _num_buffered_bytes(0),
          _profile(std::move(profile)),
          _sub_plan_query_statistics_recvr(std::move(sub_plan_query_statistics_recvr)),
          _is_pipeline(is_pipeline) {
    (void)parent_tracker;
    // TODO: Now the parent tracker may cause problem when we need spill to disk, so we
    // replace parent_tracker with nullptr, fix future
    _mem_tracker.reset(new MemTracker(_profile.get(), -1, "DataStreamRecvr", nullptr));
    // _mem_tracker.reset(new MemTracker(_profile.get(), -1, "DataStreamRecvr", parent_tracker));

    // Create one queue per sender if is_merging is true.

    int num_queues = is_merging ? num_senders : 1;
    _sender_queues.reserve(num_queues);
    int num_sender_per_queue = is_merging ? 1 : num_senders;
    for (int i = 0; i < num_queues; ++i) {
        SenderQueue* queue = _sender_queue_pool.add(new SenderQueue(this, num_sender_per_queue));
        _sender_queues.push_back(queue);
    }

    // Initialize the counters
    _bytes_received_counter = ADD_COUNTER(_profile, "BytesReceived", TUnit::BYTES);
    _request_received_counter = ADD_COUNTER(_profile, "RequestReceived", TUnit::BYTES);
    // _bytes_received_time_series_counter =
    //     ADD_TIME_SERIES_COUNTER(_profile, "BytesReceived", _bytes_received_counter);
    _deserialize_row_batch_timer = ADD_TIMER(_profile, "DeserializeRowBatchTimer");
    _data_arrival_timer = ADD_TIMER(_profile, "DataArrivalWaitTime");
    _decompress_row_batch_timer = ADD_TIMER(_profile, "DecompressRowBatchTimer");
    _deserialize_chunk_meta_timer = ADD_TIMER(_profile, "DeserializeChunkMetaTimer");
    _buffer_full_total_timer = ADD_TIMER(_profile, "SendersBlockedTotalTimer(*)");
    _first_batch_wait_total_timer = ADD_TIMER(_profile, "FirstBatchArrivalWaitTime");

    _sender_total_timer = ADD_TIMER(_profile, "SenderTotalTime");
    _sender_wait_lock_timer = ADD_TIMER(_profile, "SenderWaitLockTime");
}

Status DataStreamRecvr::get_next(RowBatch* output_batch, bool* eos) {
    DCHECK(_merger.get() != NULL);
    return _merger->get_next(output_batch, eos);
}

Status DataStreamRecvr::get_next(vectorized::ChunkPtr* chunk, bool* eos) {
    DCHECK(_chunks_merger.get() != NULL);
    return _chunks_merger->get_next(chunk, eos);
}

Status DataStreamRecvr::get_next_for_pipeline(vectorized::ChunkPtr* chunk, std::atomic<bool>* eos, bool* should_exit) {
    DCHECK(_chunks_merger.get() != NULL);
    return _chunks_merger->get_next_for_pipeline(chunk, eos, should_exit);
}

bool DataStreamRecvr::is_data_ready() {
    return _chunks_merger->is_data_ready();
}

void DataStreamRecvr::add_batch(const PRowBatch& batch, int sender_id, int be_number, int64_t packet_seq,
                                ::google::protobuf::Closure** done) {
    int use_sender_id = _is_merging ? sender_id : 0;
    // Add all batches to the same queue if _is_merging is false.
    _sender_queues[use_sender_id]->add_batch(batch, be_number, packet_seq, done);
}

Status DataStreamRecvr::add_chunks(const PTransmitChunkParams& request, ::google::protobuf::Closure** done) {
    SCOPED_TIMER(_sender_total_timer);
    COUNTER_UPDATE(_request_received_counter, 1);
    int use_sender_id = _is_merging ? request.sender_id() : 0;
    // Add all batches to the same queue if _is_merging is false.

    if (!_is_pipeline) {
        return _sender_queues[use_sender_id]->add_chunks(request, done);
    } else {
        return _sender_queues[use_sender_id]->add_chunks_for_pipeline(request, done);
    }
}

void DataStreamRecvr::remove_sender(int sender_id, int be_number) {
    int use_sender_id = _is_merging ? sender_id : 0;
    _sender_queues[use_sender_id]->decrement_senders(be_number);
}

void DataStreamRecvr::cancel_stream() {
    for (int i = 0; i < _sender_queues.size(); ++i) {
        _sender_queues[i]->cancel();
    }
}

void DataStreamRecvr::close() {
    for (int i = 0; i < _sender_queues.size(); ++i) {
        _sender_queues[i]->close();
    }
    // Remove this receiver from the DataStreamMgr that created it.
    // TODO: log error msg
    _mgr->deregister_recvr(fragment_instance_id(), dest_node_id());
    _mgr = NULL;
    _merger.reset();
    _chunks_merger.reset();
    _mem_tracker->close();
    _mem_tracker.reset();
}

DataStreamRecvr::~DataStreamRecvr() {
    DCHECK(_mgr == NULL) << "Must call close()";
}

Status DataStreamRecvr::get_batch(RowBatch** next_batch) {
    DCHECK(!_is_merging);
    DCHECK_EQ(_sender_queues.size(), 1);
    return _sender_queues[0]->get_batch(next_batch);
}

Status DataStreamRecvr::get_chunk(std::unique_ptr<vectorized::Chunk>* chunk) {
    DCHECK(!_is_merging);
    DCHECK_EQ(_sender_queues.size(), 1);
    vectorized::Chunk* tmp_chunk = nullptr;
    Status status = _sender_queues[0]->get_chunk(&tmp_chunk);
    chunk->reset(tmp_chunk);
    return status;
}

bool DataStreamRecvr::has_output() const {
    DCHECK(!_is_merging);
    return _sender_queues[0]->has_output();
}

bool DataStreamRecvr::is_finished() const {
    DCHECK(!_is_merging);
    return _sender_queues[0]->is_finished();
}

} // namespace starrocks
