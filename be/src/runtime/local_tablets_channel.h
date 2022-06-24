// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/compiler_util.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
DIAGNOSTIC_POP

#include <cstdint>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "common/statusor.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/ref_counted.h"
#include "runtime/descriptors.h"
#include "runtime/global_dicts.h"
#include "runtime/mem_tracker.h"
#include "runtime/tablets_channel.h"
#include "serde/protobuf_serde.h"
#include "storage/async_delta_writer.h"
#include "util/countdown_latch.h"

namespace brpc {
class Controller;
}

namespace starrocks {

class LocalTabletsChannel : public TabletsChannel {
    using AsyncDeltaWriter = vectorized::AsyncDeltaWriter;
    using AsyncDeltaWriterCallback = vectorized::AsyncDeltaWriterCallback;
    using AsyncDeltaWriterRequest = vectorized::AsyncDeltaWriterRequest;
    using CommittedRowsetInfo = vectorized::CommittedRowsetInfo;

public:
    LocalTabletsChannel(LoadChannel* load_channel, const TabletsChannelKey& key, MemTracker* mem_tracker);

    LocalTabletsChannel(const LocalTabletsChannel&) = delete;
    LocalTabletsChannel(LocalTabletsChannel&&) = delete;
    void operator=(const LocalTabletsChannel&) = delete;
    void operator=(LocalTabletsChannel&&) = delete;

    const TabletsChannelKey& key() const { return _key; }

    Status open(const PTabletWriterOpenRequest& params) override;

    void add_chunk(brpc::Controller* cntl, const PTabletWriterAddChunkRequest& request,
                   PTabletWriterAddBatchResult* response, google::protobuf::Closure* done) override;

    void cancel() override;

    MemTracker* mem_tracker() { return _mem_tracker; }

private:
    using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;

    ~LocalTabletsChannel() override;

    static std::atomic<uint64_t> _s_tablet_writer_count;

    struct Sender {
        bthread::Mutex lock;

        std::set<int64_t> receive_sliding_window;
        std::set<int64_t> success_sliding_window;

        int64_t last_sliding_packet_seq = -1;
    };

    class WriteContext : public RefCountedThreadSafe<WriteContext> {
    public:
        explicit WriteContext(PTabletWriterAddBatchResult* response) : _response(response), _latch(nullptr) {}

        WriteContext(const WriteContext&) = delete;
        void operator=(const WriteContext&) = delete;
        WriteContext(WriteContext&&) = delete;
        void operator=(WriteContext&&) = delete;

        void update_status(const Status& status) {
            if (status.ok() || _response == nullptr) {
                return;
            }
            std::lock_guard l(_response_lock);
            if (_response->status().status_code() == TStatusCode::OK) {
                status.to_protobuf(_response->mutable_status());
            }
        }

        void add_committed_tablet_info(PTabletInfo* tablet_info) {
            DCHECK(_response != nullptr);
            std::lock_guard l(_response_lock);
            _response->add_tablet_vec()->Swap(tablet_info);
        }

        void set_count_down_latch(BThreadCountDownLatch* latch) { _latch = latch; }

        void clear_response() { _response = nullptr; }

    private:
        friend class LocalTabletsChannel;
        friend class RefCountedThreadSafe<WriteContext>;
        ~WriteContext() {
            if (_latch) _latch->count_down();
        }

        mutable bthread::Mutex _response_lock;
        PTabletWriterAddBatchResult* _response;
        BThreadCountDownLatch* _latch;

        vectorized::Chunk _chunk;
        std::unique_ptr<uint32_t[]> _row_indexes;
        std::unique_ptr<uint32_t[]> _channel_row_idx_start_points;
    };

    class WriteCallback : public AsyncDeltaWriterCallback {
    public:
        explicit WriteCallback(WriteContext* context) : _context(context) { _context->AddRef(); }

        ~WriteCallback() override { _context->Release(); }

        void run(const Status& st, const CommittedRowsetInfo* info) override;

        WriteCallback(const WriteCallback&) = delete;
        void operator=(const WriteCallback&) = delete;
        WriteCallback(WriteCallback&&) = delete;
        void operator=(WriteCallback&&) = delete;

    private:
        WriteContext* _context;
    };

    Status _open_all_writers(const PTabletWriterOpenRequest& params);

    Status _build_chunk_meta(const ChunkPB& pb_chunk);

    StatusOr<scoped_refptr<WriteContext>> _create_write_context(const PTabletWriterAddChunkRequest& request,
                                                                PTabletWriterAddBatchResult* response,
                                                                google::protobuf::Closure* done);

    int _close_sender(const int64_t* partitions, size_t partitions_size);

    Status _deserialize_chunk(const ChunkPB& pchunk, vectorized::Chunk& chunk, faststring* uncompressed_buffer);

    LoadChannel* _load_channel;

    TabletsChannelKey _key;

    MemTracker* _mem_tracker;

    // initialized in open function
    int64_t _txn_id = -1;
    int64_t _index_id = -1;
    OlapTableSchemaParam* _schema = nullptr;
    TupleDescriptor* _tuple_desc = nullptr;
    RowDescriptor* _row_desc = nullptr;

    // next sequence we expect
    std::atomic<int> _num_remaining_senders;
    std::vector<Sender> _senders;
    size_t _max_sliding_window_size = config::max_load_dop * 3;

    mutable bthread::Mutex _partitions_ids_lock;
    std::unordered_set<int64_t> _partition_ids;

    mutable bthread::Mutex _chunk_meta_lock;
    serde::ProtobufChunkMeta _chunk_meta;
    std::atomic<bool> _has_chunk_meta;

    std::unordered_map<int64_t, uint32_t> _tablet_id_to_sorted_indexes;
    // tablet_id -> TabletChannel
    std::unordered_map<int64_t, std::unique_ptr<AsyncDeltaWriter>> _delta_writers;

    vectorized::GlobalDictByNameMaps _global_dicts;
    std::unique_ptr<MemPool> _mem_pool;
};

} // namespace starrocks
