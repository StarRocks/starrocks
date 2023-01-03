// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <brpc/controller.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include "common/compiler_util.h"
#include "runtime/tablets_channel.h"
#include "service/backend_options.h"
#include "storage/async_delta_writer.h"
#include "util/countdown_latch.h"

namespace brpc {
class Controller;
}

namespace starrocks {

class MemTracker;

class LocalTabletsChannel : public TabletsChannel {
    using AsyncDeltaWriter = vectorized::AsyncDeltaWriter;
    using AsyncDeltaWriterCallback = vectorized::AsyncDeltaWriterCallback;
    using AsyncDeltaWriterRequest = vectorized::AsyncDeltaWriterRequest;
    using AsyncDeltaWriterSegmentRequest = vectorized::AsyncDeltaWriterSegmentRequest;
    using CommittedRowsetInfo = vectorized::CommittedRowsetInfo;
    using FailedRowsetInfo = vectorized::FailedRowsetInfo;

public:
    LocalTabletsChannel(LoadChannel* load_channel, const TabletsChannelKey& key, MemTracker* mem_tracker);
    ~LocalTabletsChannel() override;

    LocalTabletsChannel(const LocalTabletsChannel&) = delete;
    LocalTabletsChannel(LocalTabletsChannel&&) = delete;
    void operator=(const LocalTabletsChannel&) = delete;
    void operator=(LocalTabletsChannel&&) = delete;

    const TabletsChannelKey& key() const { return _key; }

    Status open(const PTabletWriterOpenRequest& params, std::shared_ptr<OlapTableSchemaParam> schema) override;

    void add_chunk(vectorized::Chunk* chunk, const PTabletWriterAddChunkRequest& request,
                   PTabletWriterAddBatchResult* response) override;

    void add_segment(brpc::Controller* cntl, const PTabletWriterAddSegmentRequest* request,
                     PTabletWriterAddSegmentResult* response, google::protobuf::Closure* done);

    void cancel() override;

    void abort() override;

    void abort(const std::vector<int64_t>& tablet_ids);

    MemTracker* mem_tracker() { return _mem_tracker; }

private:
    using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;

    static std::atomic<uint64_t> _s_tablet_writer_count;

    struct Sender {
        bthread::Mutex lock;

        std::set<int64_t> receive_sliding_window;
        std::set<int64_t> success_sliding_window;

        int64_t last_sliding_packet_seq = -1;
    };

    class WriteContext {
    public:
        explicit WriteContext(PTabletWriterAddBatchResult* response)
                : _response_lock(),
                  _response(response),
                  _latch(nullptr),
                  _chunk(),
                  _row_indexes(),
                  _channel_row_idx_start_points() {}

        ~WriteContext() {
            if (_latch) _latch->count_down();
        }

        DISALLOW_COPY_AND_MOVE(WriteContext);

        void update_status(const Status& status) {
            if (status.ok() || _response == nullptr) {
                return;
            }
            std::string msg = fmt::format("{}: {}", BackendOptions::get_localhost(), status.message());
            std::lock_guard l(_response_lock);
            if (_response->status().status_code() == TStatusCode::OK) {
                _response->mutable_status()->set_status_code(status.code());
                _response->mutable_status()->add_error_msgs(msg);
            }
        }

        void add_committed_tablet_info(PTabletInfo* tablet_info) {
            DCHECK(_response != nullptr);
            std::lock_guard l(_response_lock);
            _response->add_tablet_vec()->Swap(tablet_info);
        }

        void add_failed_tablet_info(PTabletInfo* tablet_info) {
            DCHECK(_response != nullptr);
            std::lock_guard l(_response_lock);
            _response->add_failed_tablet_vec()->Swap(tablet_info);
        }

        void set_count_down_latch(BThreadCountDownLatch* latch) { _latch = latch; }

    private:
        friend class LocalTabletsChannel;

        mutable bthread::Mutex _response_lock;
        PTabletWriterAddBatchResult* _response;
        BThreadCountDownLatch* _latch;

        vectorized::Chunk _chunk;
        std::unique_ptr<uint32_t[]> _row_indexes;
        std::unique_ptr<uint32_t[]> _channel_row_idx_start_points;
    };

    class WriteCallback : public AsyncDeltaWriterCallback {
    public:
        explicit WriteCallback(std::shared_ptr<WriteContext> context) : _context(std::move(context)) {}

        ~WriteCallback() override = default;

        void run(const Status& st, const CommittedRowsetInfo* info, const FailedRowsetInfo* failed_info) override;

        WriteCallback(const WriteCallback&) = delete;
        void operator=(const WriteCallback&) = delete;
        WriteCallback(WriteCallback&&) = delete;
        void operator=(WriteCallback&&) = delete;

    private:
        std::shared_ptr<WriteContext> _context;
    };

    Status _open_all_writers(const PTabletWriterOpenRequest& params);

    StatusOr<std::shared_ptr<WriteContext>> _create_write_context(vectorized::Chunk* chunk,
                                                                  const PTabletWriterAddChunkRequest& request,
                                                                  PTabletWriterAddBatchResult* response);

    int _close_sender(const int64_t* partitions, size_t partitions_size);

    void _commit_tablets(const PTabletWriterAddChunkRequest& request,
                         std::shared_ptr<LocalTabletsChannel::WriteContext> context);

    LoadChannel* _load_channel;

    TabletsChannelKey _key;

    MemTracker* _mem_tracker;

    // initialized in open function
    int64_t _txn_id = -1;
    int64_t _index_id = -1;
    int64_t _node_id = -1;
    std::shared_ptr<OlapTableSchemaParam> _schema;
    TupleDescriptor* _tuple_desc = nullptr;

    // next sequence we expect
    std::atomic<int> _num_remaining_senders;
    std::vector<Sender> _senders;
    size_t _max_sliding_window_size = config::max_load_dop * 3;

    mutable bthread::Mutex _partitions_ids_lock;
    std::unordered_set<int64_t> _partition_ids;

    std::unordered_map<int64_t, uint32_t> _tablet_id_to_sorted_indexes;
    // tablet_id -> TabletChannel
    std::unordered_map<int64_t, std::unique_ptr<AsyncDeltaWriter>> _delta_writers;

    vectorized::GlobalDictByNameMaps _global_dicts;
    std::unique_ptr<MemPool> _mem_pool;

    bool _is_replicated_storage = false;

    std::unordered_map<int64_t, PNetworkAddress> _node_id_to_endpoint;
};

std::shared_ptr<TabletsChannel> new_local_tablets_channel(LoadChannel* load_channel, const TabletsChannelKey& key,
                                                          MemTracker* mem_tracker);

} // namespace starrocks
