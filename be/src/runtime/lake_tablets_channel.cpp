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

#include <brpc/controller.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <fmt/format.h>

#include <chrono>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "column/chunk.h"
#include "common/closure_guard.h"
#include "common/compiler_util.h"
#include "common/statusor.h"
#include "exec/tablet_info.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/macros.h"
#include "runtime/descriptors.h"
#include "runtime/load_channel.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/tablets_channel.h"
#include "serde/protobuf_serde.h"
#include "service/backend_options.h"
#include "storage/lake/async_delta_writer.h"
#include "storage/memtable.h"
#include "storage/storage_engine.h"
#include "util/compression/block_compression.h"
#include "util/countdown_latch.h"
#include "util/stack_trace_mutex.h"

namespace starrocks {

namespace lake {
class TabletManager;
}

class LakeTabletsChannel : public TabletsChannel {
    using AsyncDeltaWriter = lake::AsyncDeltaWriter;

public:
    LakeTabletsChannel(LoadChannel* load_channel, lake::TabletManager* tablet_manager, const TabletsChannelKey& key,
                       MemTracker* mem_tracker);

    ~LakeTabletsChannel() override;

    DISALLOW_COPY_AND_MOVE(LakeTabletsChannel);

    const TabletsChannelKey& key() const { return _key; }

    Status open(const PTabletWriterOpenRequest& params, std::shared_ptr<OlapTableSchemaParam> schema,
                bool is_incremental) override;

    void add_chunk(Chunk* chunk, const PTabletWriterAddChunkRequest& request,
                   PTabletWriterAddBatchResult* response) override;

    Status incremental_open(const PTabletWriterOpenRequest& params,
                            std::shared_ptr<OlapTableSchemaParam> schema) override;

    void cancel() override;

    void abort() override;

    void abort(const std::vector<int64_t>& tablet_ids, const std::string& reason) override { return abort(); }

    MemTracker* mem_tracker() { return _mem_tracker; }

private:
    using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;

    struct Sender {
        StackTraceMutex<bthread::Mutex> lock;
        int64_t next_seq = 0;
    };

    class WriteContext {
    public:
        explicit WriteContext(PTabletWriterAddBatchResult* response)
                : _mtx(), _response(response), _chunk(), _row_indexes(), _channel_row_idx_start_points() {}

        DISALLOW_COPY_AND_MOVE(WriteContext);

        ~WriteContext() = default;

        void update_status(const Status& status) {
            if (status.ok()) {
                return;
            }
            std::string msg = strings::Substitute("$0: $1", BackendOptions::get_localhost(), status.get_error_msg());
            std::lock_guard l(_mtx);
            if (_response->status().status_code() == TStatusCode::OK) {
                _response->mutable_status()->set_status_code(status.code());
                _response->mutable_status()->add_error_msgs(msg);
            }
        }

        void add_finished_tablet(int64_t tablet_id) {
            std::lock_guard l(_mtx);
            auto info = _response->add_tablet_vec();
            info->set_tablet_id(tablet_id);
            info->set_schema_hash(0); // required field
        }

    private:
        friend class LakeTabletsChannel;

        mutable StackTraceMutex<bthread::Mutex> _mtx;
        PTabletWriterAddBatchResult* _response;

        Chunk _chunk;
        std::unique_ptr<uint32_t[]> _row_indexes;
        std::unique_ptr<uint32_t[]> _channel_row_idx_start_points;
    };

    Status _create_delta_writers(const PTabletWriterOpenRequest& params);

    Status _build_chunk_meta(const ChunkPB& pb_chunk);

    StatusOr<std::unique_ptr<WriteContext>> _create_write_context(Chunk* chunk,
                                                                  const PTabletWriterAddChunkRequest& request,
                                                                  PTabletWriterAddBatchResult* response);

    int _close_sender(const int64_t* partitions, size_t partitions_size);

    Status _deserialize_chunk(const ChunkPB& pchunk, Chunk& chunk, faststring* uncompressed_buffer);

    LoadChannel* _load_channel;
    lake::TabletManager* _tablet_manager;

    TabletsChannelKey _key;

    MemTracker* _mem_tracker;

    // initialized in open function
    int64_t _txn_id = -1;
    int64_t _index_id = -1;
    std::shared_ptr<OlapTableSchemaParam> _schema;

    // next sequence we expect
    std::atomic<int> _num_remaining_senders;
    std::vector<Sender> _senders;

    mutable StackTraceMutex<bthread::Mutex> _dirty_partitions_lock;
    std::unordered_set<int64_t> _dirty_partitions;

    mutable StackTraceMutex<bthread::Mutex> _chunk_meta_lock;
    serde::ProtobufChunkMeta _chunk_meta;
    std::atomic<bool> _has_chunk_meta;

    std::unordered_map<int64_t, uint32_t> _tablet_id_to_sorted_indexes;
    std::unordered_map<int64_t, std::unique_ptr<AsyncDeltaWriter>> _delta_writers;

    GlobalDictByNameMaps _global_dicts;
    std::unique_ptr<MemPool> _mem_pool;
};

LakeTabletsChannel::LakeTabletsChannel(LoadChannel* load_channel, lake::TabletManager* tablet_manager,
                                       const TabletsChannelKey& key, MemTracker* mem_tracker)
        : TabletsChannel(),
          _load_channel(load_channel),
          _tablet_manager(tablet_manager),
          _key(key),
          _mem_tracker(mem_tracker),
          _mem_pool(std::make_unique<MemPool>()) {}

LakeTabletsChannel::~LakeTabletsChannel() {
    _mem_pool.reset();
}

Status LakeTabletsChannel::open(const PTabletWriterOpenRequest& params, std::shared_ptr<OlapTableSchemaParam> schema,
                                [[maybe_unused]] bool is_incremental) {
    _txn_id = params.txn_id();
    _index_id = params.index_id();
    _schema = schema;
    _num_remaining_senders.store(params.num_senders(), std::memory_order_release);
    _senders = std::vector<Sender>(params.num_senders());
    RETURN_IF_ERROR(_create_delta_writers(params));
    return Status::OK();
}

void LakeTabletsChannel::add_chunk(Chunk* chunk, const PTabletWriterAddChunkRequest& request,
                                   PTabletWriterAddBatchResult* response) {
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

    auto& sender = _senders[request.sender_id()];

    std::lock_guard l(sender.lock);

    if (UNLIKELY(request.packet_seq() < sender.next_seq && request.eos())) { // duplicated eos packet
        LOG(ERROR) << "Duplicated eos packet. txn_id=" << _txn_id;
        response->mutable_status()->set_status_code(TStatusCode::DUPLICATE_RPC_INVOCATION);
        response->mutable_status()->add_error_msgs("duplicated eos packet");
        return;
    } else if (UNLIKELY(request.packet_seq() < sender.next_seq)) { // duplicated packet
        response->mutable_status()->set_status_code(TStatusCode::OK);
        return;
    } else if (UNLIKELY(request.packet_seq() > sender.next_seq)) { // out-of-order packet
        response->mutable_status()->set_status_code(TStatusCode::INVALID_ARGUMENT);
        response->mutable_status()->add_error_msgs("out-of-order packet");
        return;
    }

    auto res = _create_write_context(chunk, request, response);
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
    auto channel_size = chunk != nullptr ? _tablet_id_to_sorted_indexes.size() : 0;
    auto tablet_ids = request.tablet_ids().data();

    // Maybe a nullptr
    auto channel_row_idx_start_points = context->_channel_row_idx_start_points.get();

    // Maybe a nullptr
    auto row_indexes = context->_row_indexes.get();

    // |channel_size| is the max number of tasks invoking `AsyncDeltaWriter::write()`
    // |_delta_writers.size()| is the max number of tasks invoking `AsyncDeltaWriter::finish()`
    auto count_down_latch = BThreadCountDownLatch(channel_size + (request.eos() ? _delta_writers.size() : 0));

    // Open and write AsyncDeltaWriter
    for (int i = 0; i < channel_size; ++i) {
        size_t from = channel_row_idx_start_points[i];
        size_t size = channel_row_idx_start_points[i + 1] - from;
        if (size == 0) {
            count_down_latch.count_down();
            continue;
        }
        int64_t tablet_id = tablet_ids[row_indexes[from]];
        auto& dw = _delta_writers[tablet_id];
        DCHECK(dw != nullptr);
        if (auto st = dw->open(); !st.ok()) { // Fail to `open()` AsyncDeltaWriter
            context->update_status(st);
            count_down_latch.count_down(channel_size - i);
            // Do NOT return
            break;
        }
        dw->write(chunk, row_indexes + from, size, [&](const Status& st) {
            context->update_status(st);
            count_down_latch.count_down();
        });
    }

    // _channel_row_idx_start_points no longer used, free its memory.
    context->_channel_row_idx_start_points.reset();

    bool close_channel = false;

    // Submit `AsyncDeltaWriter::finish()` tasks if needed
    if (request.eos()) {
        int unfinished_senders = _close_sender(request.partition_ids().data(), request.partition_ids().size());
        if (unfinished_senders > 0) {
            count_down_latch.count_down(_delta_writers.size());
        } else if (unfinished_senders == 0) {
            close_channel = true;
            VLOG(5) << "Closing channel. txn_id=" << _txn_id;
            std::lock_guard l1(_dirty_partitions_lock);
            for (auto& [tablet_id, dw] : _delta_writers) {
                if (_dirty_partitions.count(dw->partition_id()) == 0) {
                    VLOG(5) << "Skip tablet " << tablet_id;
                    // This is a clean AsyncDeltaWriter, skip calling `finish()`
                    count_down_latch.count_down();
                    continue;
                }
                // This AsyncDeltaWriter may have not been `open()`ed
                if (auto st = dw->open(); !st.ok()) {
                    context->update_status(st);
                    count_down_latch.count_down();
                    continue;
                }
                dw->finish([&, id = tablet_id](const Status& st) {
                    if (st.ok()) {
                        context->add_finished_tablet(id);
                        VLOG(5) << "Finished tablet " << id;
                    } else {
                        context->update_status(st);
                        LOG(ERROR) << "Fail to finish tablet " << id << ": " << st;
                    }
                    count_down_latch.count_down();
                });
            }
        } else {
            count_down_latch.count_down(_delta_writers.size());
            context->update_status(Status::InternalError("Unexpected state: unfinished_senders < 0"));
        }
    }

    // Block the current bthread(not pthread) until all `write()` and `finish()` tasks finished.
    count_down_latch.wait();

    if (request.eos() || context->_response->status().status_code() == TStatusCode::OK) {
        // ^^^^^^^^^^ Reject new requests once eos request received.
        sender.next_seq++;
    }

    auto t1 = std::chrono::steady_clock::now();
    response->set_execution_time_us(std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
    response->set_wait_lock_time_us(0); // We didn't measure the lock wait time, just give the caller a fake time

    if (close_channel) {
        _load_channel->remove_tablets_channel(_index_id);
    }
}

int LakeTabletsChannel::_close_sender(const int64_t* partitions, size_t partitions_size) {
    int n = _num_remaining_senders.fetch_sub(1);
    std::lock_guard l(_dirty_partitions_lock);
    for (int i = 0; i < partitions_size; i++) {
        _dirty_partitions.insert(partitions[i]);
    }
    return n - 1;
}

Status LakeTabletsChannel::_create_delta_writers(const PTabletWriterOpenRequest& params) {
    std::vector<SlotDescriptor*>* slots = nullptr;
    for (auto& index : _schema->indexes()) {
        if (index->index_id == _index_id) {
            slots = &index->slots;
            break;
        }
    }
    if (slots == nullptr) {
        return Status::InvalidArgument(fmt::format("Unknown index_id: {}", _key.to_string()));
    }
    // init global dict info if needed
    for (auto& slot : params.schema().slot_descs()) {
        GlobalDictMap global_dict;
        if (slot.global_dict_words_size()) {
            for (size_t i = 0; i < slot.global_dict_words_size(); i++) {
                const std::string& dict_word = slot.global_dict_words(i);
                auto* data = _mem_pool->allocate(dict_word.size());
                RETURN_IF_UNLIKELY_NULL(data, Status::MemoryAllocFailed("alloc mem for global dict failed"));
                memcpy(data, dict_word.data(), dict_word.size());
                Slice slice(data, dict_word.size());
                global_dict.emplace(slice, i);
            }
            GlobalDictsWithVersion<GlobalDictMap> dict;
            dict.dict = std::move(global_dict);
            dict.version = slot.has_global_dict_version() ? slot.global_dict_version() : 0;
            _global_dicts.emplace(std::make_pair(slot.col_name(), std::move(dict)));
        }
    }

    std::vector<int64_t> tablet_ids;
    tablet_ids.reserve(params.tablets_size());
    for (const PTabletWithPartition& tablet : params.tablets()) {
        std::unique_ptr<AsyncDeltaWriter> writer;
        if (!params.merge_condition().empty()) {
            writer = AsyncDeltaWriter::create(_tablet_manager, tablet.tablet_id(), _txn_id, tablet.partition_id(),
                                              slots, params.merge_condition(), _mem_tracker);
        } else {
            writer = AsyncDeltaWriter::create(_tablet_manager, tablet.tablet_id(), _txn_id, tablet.partition_id(),
                                              slots, _mem_tracker);
        }
        _delta_writers.emplace(tablet.tablet_id(), std::move(writer));
        tablet_ids.emplace_back(tablet.tablet_id());
    }
    DCHECK_EQ(_delta_writers.size(), params.tablets_size());
    // In order to get sorted index for each tablet
    std::sort(tablet_ids.begin(), tablet_ids.end());
    for (size_t i = 0; i < tablet_ids.size(); ++i) {
        _tablet_id_to_sorted_indexes.emplace(tablet_ids[i], i);
    }
    return Status::OK();
}

void LakeTabletsChannel::abort() {
    for (auto& it : _delta_writers) {
        it.second->close();
    }
}

void LakeTabletsChannel::cancel() {
    //TODO: Current LakeDeltaWriter don't support fast cancel
}

StatusOr<std::unique_ptr<LakeTabletsChannel::WriteContext>> LakeTabletsChannel::_create_write_context(
        Chunk* chunk, const PTabletWriterAddChunkRequest& request, PTabletWriterAddBatchResult* response) {
    if (chunk == nullptr && !request.eos()) {
        return Status::InvalidArgument("PTabletWriterAddChunkRequest has no chunk or eos");
    }

    std::unique_ptr<WriteContext> context(new WriteContext(response));

    if (chunk == nullptr) {
        return std::move(context);
    }

    if (UNLIKELY(request.tablet_ids_size() != chunk->num_rows())) {
        return Status::InvalidArgument("request.tablet_ids_size() != chunk.num_rows()");
    }

    const auto channel_size = _tablet_id_to_sorted_indexes.size();
    context->_row_indexes = std::make_unique<uint32_t[]>(chunk->num_rows());
    context->_channel_row_idx_start_points = std::make_unique<uint32_t[]>(channel_size + 1);

    auto& row_indexes = context->_row_indexes;
    auto& channel_row_idx_start_points = context->_channel_row_idx_start_points;

    auto tablet_ids = request.tablet_ids().data();
    auto tablet_ids_size = request.tablet_ids_size();
    // compute row indexes for each channel
    for (uint32_t i = 0; i < tablet_ids_size; ++i) {
        uint32_t channel_index = _tablet_id_to_sorted_indexes[tablet_ids[i]];
        channel_row_idx_start_points[channel_index]++;
    }

    // NOTE: we make the last item equal with number of rows of this chunk
    for (int i = 1; i <= channel_size; ++i) {
        channel_row_idx_start_points[i] += channel_row_idx_start_points[i - 1];
    }

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

Status LakeTabletsChannel::incremental_open(const PTabletWriterOpenRequest& params,
                                            std::shared_ptr<OlapTableSchemaParam> schema) {
    return Status::NotSupported("LakeTabletsChannel::incremental_open");
}

std::shared_ptr<TabletsChannel> new_lake_tablets_channel(LoadChannel* load_channel, lake::TabletManager* tablet_manager,
                                                         const TabletsChannelKey& key, MemTracker* mem_tracker) {
    return std::make_shared<LakeTabletsChannel>(load_channel, tablet_manager, key, mem_tracker);
}

} // namespace starrocks
