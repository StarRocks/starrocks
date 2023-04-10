// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/tablets_channel.cpp

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

#include "runtime/tablets_channel.h"

#include <brpc/controller.h>
#include <fmt/format.h>

#include <chrono>

#include "common/closure_guard.h"
#include "exec/tablet_info.h"
#include "gutil/strings/join.h"
#include "gutil/strings/substitute.h"
#include "runtime/load_channel.h"
#include "serde/protobuf_serde.h"
#include "storage/storage_engine.h"
#include "storage/vectorized/delta_writer.h"
#include "storage/vectorized/memtable.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

std::atomic<uint64_t> TabletsChannel::_s_tablet_writer_count;

TabletsChannel::TabletsChannel(LoadChannel* load_channel, const TabletsChannelKey& key, MemTracker* mem_tracker)
        : _load_channel(load_channel),
          _key(key),
          _mem_tracker(mem_tracker),
          _has_chunk_meta(false),
          _mem_pool(std::make_unique<MemPool>()) {
    static std::once_flag once_flag;
    std::call_once(once_flag, [] {
        REGISTER_GAUGE_STARROCKS_METRIC(tablet_writer_count, [&]() { return _s_tablet_writer_count.load(); });
    });
}

TabletsChannel::~TabletsChannel() {
    _s_tablet_writer_count -= _delta_writers.size();
    delete _row_desc;
    delete _schema;
    _mem_pool.reset();
}

Status TabletsChannel::open(const PTabletWriterOpenRequest& params) {
    _txn_id = params.txn_id();
    _index_id = params.index_id();
    _schema = new OlapTableSchemaParam();
    RETURN_IF_ERROR(_schema->init(params.schema()));
    _tuple_desc = _schema->tuple_desc();
    _row_desc = new RowDescriptor(_tuple_desc, false);

    _num_remaining_senders.store(params.num_senders(), std::memory_order_release);
    _senders = std::vector<Sender>(params.num_senders());

    RETURN_IF_ERROR(_open_all_writers(params));
    return Status::OK();
}

void TabletsChannel::add_chunk(brpc::Controller* cntl, const PTabletWriterAddChunkRequest& request,
                               PTabletWriterAddBatchResult* response, google::protobuf::Closure* done) {
    ClosureGuard done_guard(done);

    auto t0 = std::chrono::steady_clock::now();

    if (UNLIKELY(!request.has_sender_id())) {
        response->mutable_status()->set_status_code(TStatusCode::INVALID_ARGUMENT);
        response->mutable_status()->add_error_msgs("no sender_id in PTabletWriterAddChunkRequest");
        return;
    }
    if (UNLIKELY(request.sender_id() < 0)) {
        response->mutable_status()->set_status_code(TStatusCode::INVALID_ARGUMENT);
        response->mutable_status()->add_error_msgs("negative sender_id in PTabletWriterAddChunkRequest");
        return;
    }
    if (UNLIKELY(request.sender_id() >= _senders.size())) {
        response->mutable_status()->set_status_code(TStatusCode::INVALID_ARGUMENT);
        response->mutable_status()->add_error_msgs(
                fmt::format("invalid sender_id {} in PTabletWriterAddChunkRequest, limit={}", request.sender_id(),
                            _senders.size()));
        return;
    }
    if (UNLIKELY(!request.has_packet_seq())) {
        response->mutable_status()->set_status_code(TStatusCode::INVALID_ARGUMENT);
        response->mutable_status()->add_error_msgs("no packet_seq in PTabletWriterAddChunkRequest");
        return;
    }

    Sender& sender = _senders[request.sender_id()];
    std::lock_guard l(sender.lock);

    if (request.packet_seq() == sender.next_seq) {
        sender.next_seq++;
    } else if (request.packet_seq() < sender.next_seq) {
        LOG(INFO) << "Ignore outdated request from " << cntl->remote_side() << ". seq=" << request.packet_seq()
                  << " expect=" << sender.next_seq << " load_id=" << _load_channel->load_id();
        response->mutable_status()->set_status_code(TStatusCode::OK);
        return;
    } else {
        LOG(WARNING) << "Out-of-order request from " << cntl->remote_side() << ". seq=" << request.packet_seq()
                     << " expect=" << sender.next_seq << " load_id=" << _load_channel->load_id();
        response->mutable_status()->set_status_code(TStatusCode::INVALID_ARGUMENT);
        response->mutable_status()->add_error_msgs("out-of-order request");
        return;
    }

    auto res = _create_write_context(request, response, done);
    if (!res.ok()) {
        res.status().to_protobuf(response->mutable_status());
        return;
    } else {
        // Assuming that most writes will be successful, by setting the status code to OK before submitting
        // `AsyncDeltaWriterRequest`s, there will be no lock contention most of the time in
        // `WriteContext::update_status()`
        response->mutable_status()->set_status_code(TStatusCode::OK);
    }

    auto context = std::move(res).value();
    auto channel_size = request.has_chunk() ? _tablet_id_to_sorted_indexes.size() : 0;
    auto tablet_ids = request.tablet_ids().data();
    auto channel_row_idx_start_points = context->_channel_row_idx_start_points.get(); // May be a nullptr
    auto row_indexes = context->_row_indexes.get();                                   // May be a nullptr

    auto count_down_latch = BThreadCountDownLatch(1);

    context->set_count_down_latch(&count_down_latch);

    for (int i = 0; i < channel_size; ++i) {
        size_t from = channel_row_idx_start_points[i];
        size_t size = channel_row_idx_start_points[i + 1] - from;
        if (size == 0) {
            continue;
        }
        auto tablet_id = tablet_ids[row_indexes[from]];
        auto it = _delta_writers.find(tablet_id);
        DCHECK(it != _delta_writers.end());
        auto& delta_writer = it->second;

        AsyncDeltaWriterRequest req;
        req.chunk = &context->_chunk;
        req.indexes = row_indexes + from;
        req.indexes_size = size;
        req.commit_after_write = false;

        // The reference count of context is increased in the constructor of WriteCallback
        // and decreased in the destructor of WriteCallback.
        auto cb = new WriteCallback(context.get());

        delta_writer->write(req, cb);
    }

    // _channel_row_idx_start_points no longer used, release it to free memory.
    context->_channel_row_idx_start_points.reset();

    bool close_channel = false;

    // NOTE: Must close sender *AFTER* the write requests submitted, otherwise a delta writer commit request may
    // be executed ahead of the write requests submitted by other senders.
    if (request.eos() && _close_sender(&sender, request.partition_ids().data(), request.partition_ids_size()) == 0) {
        close_channel = true;
        std::lock_guard l1(_partitions_ids_lock);
        for (auto& [_, delta_writer] : _delta_writers) {
            if (UNLIKELY(_partition_ids.count(delta_writer->partition_id()) == 0)) {
                // no data load, abort txn without printing log
                delta_writer->abort(false);
            } else {
                auto cb = new WriteCallback(context.get());
                delta_writer->commit(cb);
            }
        }
    }

    // Must reset the context pointer before waiting on the |count_down_latch|,
    // because the |count_down_latch| is decreased in the destructor of the context,
    // and the destructor of the context cannot be called unless we reset the pointer
    // here.
    context.reset();

    // This will only block the bthread, will not block the pthread
    count_down_latch.wait();

    auto t1 = std::chrono::steady_clock::now();
    response->set_execution_time_us(std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
    response->set_wait_lock_time_us(0); // We didn't measure the lock wait time, just give the caller a fake time

    if (close_channel) {
        _load_channel->remove_tablets_channel(_index_id);

        // persist txn.
        auto tablet_ids = request.tablet_ids();
        std::vector<TabletSharedPtr> tablets(tablet_ids.size());
        for (const auto tablet_id : tablet_ids) {
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
            if (tablet != nullptr) {
                tablets.push_back(tablet);
            }
        }
        auto st = StorageEngine::instance()->txn_manager()->persist_tablet_related_txns(tablets);
        if (!st.ok()) {
            LOG(WARNING) << "failed to persist transactions, tablets num: " << tablets.size() << " err: " << st;
        }
    }
}

Status TabletsChannel::_build_chunk_meta(const ChunkPB& pb_chunk) {
    if (_has_chunk_meta.load(std::memory_order_acquire)) {
        return Status::OK();
    }
    std::lock_guard l(_chunk_meta_lock);
    if (_has_chunk_meta.load(std::memory_order_acquire)) {
        return Status::OK();
    }
    StatusOr<serde::ProtobufChunkMeta> res = serde::build_protobuf_chunk_meta(*_row_desc, pb_chunk);
    if (!res.ok()) return res.status();
    _chunk_meta = std::move(res).value();
    _has_chunk_meta.store(true, std::memory_order_release);
    return Status::OK();
}

// NOTE: Assume sender->lock has been acquired.
int TabletsChannel::_close_sender(Sender* sender, const int64_t* partitions, size_t partitions_size) {
    DCHECK(!sender->closed);
    sender->closed = true;
    int n = _num_remaining_senders.fetch_sub(1);
    DCHECK_GE(n, 1);
    std::lock_guard l(_partitions_ids_lock);
    for (int i = 0; i < partitions_size; i++) {
        _partition_ids.insert(partitions[i]);
    }
    return n - 1;
}

Status TabletsChannel::_open_all_writers(const PTabletWriterOpenRequest& params) {
    std::vector<SlotDescriptor*>* index_slots = nullptr;
    int32_t schema_hash = 0;
    for (auto& index : _schema->indexes()) {
        if (index->index_id == _index_id) {
            index_slots = &index->slots;
            schema_hash = index->schema_hash;
            break;
        }
    }
    if (index_slots == nullptr) {
        return Status::InvalidArgument(fmt::format("Unknown index_id: {}", _key.to_string()));
    }
    // init global dict info if needed
    for (auto& slot : params.schema().slot_descs()) {
        vectorized::GlobalDictMap global_dict;
        if (slot.global_dict_words_size()) {
            for (size_t i = 0; i < slot.global_dict_words_size(); i++) {
                const std::string& dict_word = slot.global_dict_words(i);
                auto* data = _mem_pool->allocate(dict_word.size());
                RETURN_IF_UNLIKELY_NULL(data, Status::MemoryAllocFailed("alloc mem for global dict failed"));
                memcpy(data, dict_word.data(), dict_word.size());
                Slice slice(data, dict_word.size());
                global_dict.emplace(slice, i);
            }
            _global_dicts.insert(std::make_pair(slot.col_name(), std::move(global_dict)));
        }
    }

    std::vector<int64_t> tablet_ids;
    tablet_ids.reserve(params.tablets_size());
    for (const PTabletWithPartition& tablet : params.tablets()) {
        vectorized::DeltaWriterOptions options;
        options.tablet_id = tablet.tablet_id();
        options.schema_hash = schema_hash;
        options.write_type = vectorized::WriteType::LOAD;
        options.txn_id = _txn_id;
        options.partition_id = tablet.partition_id();
        options.load_id = params.id();
        options.tuple_desc = _tuple_desc;
        options.slots = index_slots;
        options.global_dicts = &_global_dicts;

        auto res = AsyncDeltaWriter::open(options, _mem_tracker);
        RETURN_IF_ERROR(res.status());
        auto writer = std::move(res).value();
        _delta_writers.emplace(tablet.tablet_id(), std::move(writer));
        tablet_ids.emplace_back(tablet.tablet_id());
    }
    _s_tablet_writer_count += _delta_writers.size();
    DCHECK_EQ(_delta_writers.size(), params.tablets_size());
    // In order to get sorted index for each tablet
    std::sort(tablet_ids.begin(), tablet_ids.end());
    for (size_t i = 0; i < tablet_ids.size(); ++i) {
        _tablet_id_to_sorted_indexes.emplace(tablet_ids[i], i);
    }
    return Status::OK();
}

void TabletsChannel::cancel() {
    vector<int64_t> tablet_ids;
    tablet_ids.reserve(_delta_writers.size());
    for (auto& it : _delta_writers) {
        (void)it.second->abort(false);
        tablet_ids.emplace_back(it.first);
    }
    string tablet_id_list_str;
    JoinInts(tablet_ids, ",", &tablet_id_list_str);
    LOG(INFO) << "cancel LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << _key.id
              << " index_id: " << _key.index_id << " #tablet:" << _delta_writers.size()
              << " tablet_ids:" << tablet_id_list_str;
}

StatusOr<scoped_refptr<TabletsChannel::WriteContext>> TabletsChannel::_create_write_context(
        const PTabletWriterAddChunkRequest& request, PTabletWriterAddBatchResult* response,
        google::protobuf::Closure* done) {
    if (!request.has_chunk() && !request.eos()) {
        return Status::InvalidArgument("PTabletWriterAddChunkRequest has no chunk or eos");
    }

    scoped_refptr<WriteContext> context(new WriteContext(response));

    if (!request.has_chunk()) {
        return std::move(context);
    }

    auto& pchunk = request.chunk();
    RETURN_IF_ERROR(_build_chunk_meta(pchunk));

    vectorized::Chunk& chunk = context->_chunk;
    serde::ProtobufChunkDeserializer des(_chunk_meta);
    StatusOr<vectorized::Chunk> res = des.deserialize(pchunk.data());
    if (!res.ok()) return res.status();
    chunk = std::move(res).value();
    if (UNLIKELY(request.tablet_ids_size() != chunk.num_rows())) {
        return Status::InvalidArgument("request.tablet_ids_size() != chunk.num_rows()");
    }

    const auto channel_size = _tablet_id_to_sorted_indexes.size();
    context->_row_indexes = std::make_unique<uint32_t[]>(chunk.num_rows());
    context->_channel_row_idx_start_points = std::make_unique<uint32_t[]>(channel_size + 1);

    auto& row_indexes = context->_row_indexes;
    auto& channel_row_idx_start_points = context->_channel_row_idx_start_points;

    // compute row indexes for each channel
    for (uint32_t i = 0; i < request.tablet_ids_size(); ++i) {
        uint32_t channel_index = _tablet_id_to_sorted_indexes[request.tablet_ids(i)];
        channel_row_idx_start_points[channel_index]++;
    }

    // NOTE: we make the last item equal with number of rows of this chunk
    for (int i = 1; i <= channel_size; ++i) {
        channel_row_idx_start_points[i] += channel_row_idx_start_points[i - 1];
    }

    auto tablet_ids = request.tablet_ids().data();
    auto tablet_ids_size = request.tablet_ids_size();
    for (int i = tablet_ids_size - 1; i >= 0; --i) {
        const auto& tablet_id = tablet_ids[i];
        auto it = _tablet_id_to_sorted_indexes.find(tablet_id);
        if (UNLIKELY(it == _tablet_id_to_sorted_indexes.end())) {
            return Status::InternalError("invalid tablet id");
        }
        uint32_t channel_index = it->second;
        row_indexes[channel_row_idx_start_points[channel_index] - 1] = i;
        channel_row_idx_start_points[channel_index]--;
    }
    return std::move(context);
}

void TabletsChannel::WriteCallback::run(const Status& st, const CommittedRowsetInfo* info) {
    _context->update_status(st);
    if (info != nullptr) {
        PTabletInfo tablet_info;
        tablet_info.set_tablet_id(info->tablet->tablet_id());
        tablet_info.set_schema_hash(info->tablet->schema_hash());
        const auto& rowset_global_dict_columns_valid_info = info->rowset_writer->global_dict_columns_valid_info();
        for (const auto& item : rowset_global_dict_columns_valid_info) {
            if (item.second) {
                tablet_info.add_valid_dict_cache_columns(item.first);
            } else {
                tablet_info.add_invalid_dict_cache_columns(item.first);
            }
        }
        _context->add_committed_tablet_info(&tablet_info);
    }
    delete this;
}

std::string TabletsChannelKey::to_string() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

std::ostream& operator<<(std::ostream& os, const TabletsChannelKey& key) {
    os << "(id=" << key.id << ",index_id=" << key.index_id << ")";
    return os;
}

} // namespace starrocks
