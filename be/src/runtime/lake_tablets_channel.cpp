// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "common/compiler_util.h"
#include "runtime/tablets_channel.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <brpc/controller.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
DIAGNOSTIC_POP
#include <fmt/format.h>

#include <chrono>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "column/chunk.h"
#include "common/closure_guard.h"
#include "common/statusor.h"
#include "exec/tablet_info.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/macros.h"
#include "runtime/descriptors.h"
#include "runtime/load_channel.h"
#include "runtime/mem_tracker.h"
#include "serde/protobuf_serde.h"
#include "service/backend_options.h"
#include "storage/lake/async_delta_writer.h"
#include "storage/memtable.h"
#include "storage/storage_engine.h"
#include "util/compression/block_compression.h"
#include "util/countdown_latch.h"
#include "util/faststring.h"

namespace starrocks {

class LakeTabletsChannel : public TabletsChannel {
    using AsyncDeltaWriter = lake::AsyncDeltaWriter;

public:
    LakeTabletsChannel(LoadChannel* load_channel, const TabletsChannelKey& key, MemTracker* mem_tracker);
    ~LakeTabletsChannel() override;

    DISALLOW_COPY_AND_MOVE(LakeTabletsChannel);

    const TabletsChannelKey& key() const { return _key; }

    Status open(const PTabletWriterOpenRequest& params) override;

    void add_chunk(brpc::Controller* cntl, const PTabletWriterAddChunkRequest& request,
                   PTabletWriterAddBatchResult* response, google::protobuf::Closure* done) override;

    void cancel() override;

    MemTracker* mem_tracker() { return _mem_tracker; }

private:
    using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;

    struct Sender {
        bthread::Mutex lock;
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
            std::string msg = fmt::format("{}: {}", BackendOptions::get_localhost(), status.message());
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

        mutable bthread::Mutex _mtx;
        PTabletWriterAddBatchResult* _response;

        vectorized::Chunk _chunk;
        std::unique_ptr<uint32_t[]> _row_indexes;
        std::unique_ptr<uint32_t[]> _channel_row_idx_start_points;
    };

    Status _create_delta_writers(const PTabletWriterOpenRequest& params);

    Status _build_chunk_meta(const ChunkPB& pb_chunk);

    StatusOr<std::unique_ptr<WriteContext>> _create_write_context(const PTabletWriterAddChunkRequest& request,
                                                                  PTabletWriterAddBatchResult* response);

    int _close_sender(const int64_t* partitions, size_t partitions_size);

    Status _deserialize_chunk(const ChunkPB& pchunk, vectorized::Chunk& chunk, faststring* uncompressed_buffer);

    LoadChannel* _load_channel;

    TabletsChannelKey _key;

    MemTracker* _mem_tracker;

    // initialized in open function
    int64_t _txn_id = -1;
    int64_t _index_id = -1;
    std::unique_ptr<OlapTableSchemaParam> _schema;
    std::unique_ptr<RowDescriptor> _row_desc;

    // next sequence we expect
    std::atomic<int> _num_remaining_senders;
    std::vector<Sender> _senders;

    mutable bthread::Mutex _dirty_partitions_lock;
    std::unordered_set<int64_t> _dirty_partitions;

    mutable bthread::Mutex _chunk_meta_lock;
    serde::ProtobufChunkMeta _chunk_meta;
    std::atomic<bool> _has_chunk_meta;

    std::unordered_map<int64_t, uint32_t> _tablet_id_to_sorted_indexes;
    std::unordered_map<int64_t, std::unique_ptr<AsyncDeltaWriter>> _delta_writers;
};

LakeTabletsChannel::LakeTabletsChannel(LoadChannel* load_channel, const TabletsChannelKey& key, MemTracker* mem_tracker)
        : TabletsChannel(), _load_channel(load_channel), _key(key), _mem_tracker(mem_tracker), _has_chunk_meta(false) {}

LakeTabletsChannel::~LakeTabletsChannel() = default;

Status LakeTabletsChannel::open(const PTabletWriterOpenRequest& params) {
    _txn_id = params.txn_id();
    _index_id = params.index_id();
    _schema = std::make_unique<OlapTableSchemaParam>();
    RETURN_IF_ERROR(_schema->init(params.schema()));
    _row_desc = std::make_unique<RowDescriptor>(_schema->tuple_desc(), false);
    _num_remaining_senders.store(params.num_senders(), std::memory_order_release);
    _senders = std::vector<Sender>(params.num_senders());
    RETURN_IF_ERROR(_create_delta_writers(params));
    return Status::OK();
}

void LakeTabletsChannel::add_chunk(brpc::Controller* cntl, const PTabletWriterAddChunkRequest& request,
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

    auto res = _create_write_context(request, response);
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
        dw->write(&context->_chunk, row_indexes + from, size, [&](const Status& st) {
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
        close_channel = (unfinished_senders == 0);
        if (!close_channel) {
            count_down_latch.count_down(_delta_writers.size());
        } else {
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
                dw->finish([&, id = tablet_id](Status st) {
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
        }
    }

    // Block the current bthread(not pthread) until all `write()` and `finish()` tasks finished.
    count_down_latch.wait();

    if (context->_response->status().status_code() == TStatusCode::OK) {
        sender.next_seq++;
    }

    auto t1 = std::chrono::steady_clock::now();
    response->set_execution_time_us(std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
    response->set_wait_lock_time_us(0); // We didn't measure the lock wait time, just give the caller a fake time

    if (close_channel) {
        _load_channel->remove_tablets_channel(_index_id);
    }
}

Status LakeTabletsChannel::_build_chunk_meta(const ChunkPB& pb_chunk) {
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

int LakeTabletsChannel::_close_sender(const int64_t* partitions, size_t partitions_size) {
    int n = _num_remaining_senders.fetch_sub(1);
    DCHECK_GE(n, 1);
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

    std::vector<int64_t> tablet_ids;
    tablet_ids.reserve(params.tablets_size());
    for (const PTabletWithPartition& tablet : params.tablets()) {
        auto writer = AsyncDeltaWriter::create(tablet.tablet_id(), _txn_id, tablet.partition_id(), slots, _mem_tracker);
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

void LakeTabletsChannel::cancel() {
    for (auto& it : _delta_writers) {
        it.second->close();
    }
}

Status LakeTabletsChannel::_deserialize_chunk(const ChunkPB& pchunk, vectorized::Chunk& chunk,
                                              faststring* uncompressed_buffer) {
    if (pchunk.compress_type() == CompressionTypePB::NO_COMPRESSION) {
        TRY_CATCH_BAD_ALLOC({
            serde::ProtobufChunkDeserializer des(_chunk_meta);
            StatusOr<vectorized::Chunk> res = des.deserialize(pchunk.data());
            if (!res.ok()) return res.status();
            chunk = std::move(res).value();
        });
    } else {
        [[maybe_unused]] size_t uncompressed_size;
        {
            const BlockCompressionCodec* codec = nullptr;
            RETURN_IF_ERROR(get_block_compression_codec(pchunk.compress_type(), &codec));
            uncompressed_size = pchunk.uncompressed_size();
            TRY_CATCH_BAD_ALLOC(uncompressed_buffer->resize(uncompressed_size));
            Slice output{uncompressed_buffer->data(), uncompressed_size};
            RETURN_IF_ERROR(codec->decompress(pchunk.data(), &output));
        }
        {
            TRY_CATCH_BAD_ALLOC({
                std::string_view buff(reinterpret_cast<const char*>(uncompressed_buffer->data()), uncompressed_size);
                serde::ProtobufChunkDeserializer des(_chunk_meta);
                StatusOr<vectorized::Chunk> res = des.deserialize(buff);
                if (!res.ok()) return res.status();
                chunk = std::move(res).value();
            });
        }
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<LakeTabletsChannel::WriteContext>> LakeTabletsChannel::_create_write_context(
        const PTabletWriterAddChunkRequest& request, PTabletWriterAddBatchResult* response) {
    if (!request.has_chunk() && !request.eos()) {
        return Status::InvalidArgument("PTabletWriterAddChunkRequest has no chunk or eos");
    }

    std::unique_ptr<WriteContext> context(new WriteContext(response));

    if (!request.has_chunk()) {
        return std::move(context);
    }

    auto& pchunk = request.chunk();
    RETURN_IF_ERROR(_build_chunk_meta(pchunk));

    vectorized::Chunk& chunk = context->_chunk;

    faststring uncompressed_buffer;
    RETURN_IF_ERROR(_deserialize_chunk(pchunk, chunk, &uncompressed_buffer));

    if (UNLIKELY(request.tablet_ids_size() != chunk.num_rows())) {
        return Status::InvalidArgument("request.tablet_ids_size() != chunk.num_rows()");
    }

    const auto channel_size = _tablet_id_to_sorted_indexes.size();
    context->_row_indexes = std::make_unique<uint32_t[]>(chunk.num_rows());
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

std::shared_ptr<TabletsChannel> new_lake_tablets_channel(LoadChannel* load_channel, const TabletsChannelKey& key,
                                                         MemTracker* mem_tracker) {
    return std::make_shared<LakeTabletsChannel>(load_channel, key, mem_tracker);
}

} // namespace starrocks
