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

#include <brpc/controller.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include "common/compiler_util.h"
#include "runtime/tablets_channel.h"
#include "service/backend_options.h"
#include "storage/async_delta_writer.h"
#include "util/bthreads/bthread_shared_mutex.h"
#include "util/countdown_latch.h"

namespace brpc {
class Controller;
}

namespace starrocks {

class MemTracker;

class LocalTabletsChannel : public TabletsChannel {
public:
    LocalTabletsChannel(LoadChannel* load_channel, const TabletsChannelKey& key, MemTracker* mem_tracker,
                        RuntimeProfile* parent_profile);
    ~LocalTabletsChannel() override;

    LocalTabletsChannel(const LocalTabletsChannel&) = delete;
    LocalTabletsChannel(LocalTabletsChannel&&) = delete;
    void operator=(const LocalTabletsChannel&) = delete;
    void operator=(LocalTabletsChannel&&) = delete;

    const TabletsChannelKey& key() const { return _key; }

    Status open(const PTabletWriterOpenRequest& params, PTabletWriterOpenResult* result,
                std::shared_ptr<OlapTableSchemaParam> schema, bool is_incremental) override;

    void add_chunk(Chunk* chunk, const PTabletWriterAddChunkRequest& request, PTabletWriterAddBatchResult* response,
                   bool* close_channel_ptr) override;

    Status incremental_open(const PTabletWriterOpenRequest& params, PTabletWriterOpenResult* result,
                            std::shared_ptr<OlapTableSchemaParam> schema) override;

    void add_segment(brpc::Controller* cntl, const PTabletWriterAddSegmentRequest* request,
                     PTabletWriterAddSegmentResult* response, google::protobuf::Closure* done);

    void cancel() override;

    void abort() override;

    void abort(const std::vector<int64_t>& tablet_ids, const std::string& reason) override;

    void update_profile() override;

    MemTracker* mem_tracker() { return _mem_tracker; }

private:
    using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;

    static std::atomic<uint64_t> _s_tablet_writer_count;

    struct Sender {
        bthread::Mutex lock;

        std::set<int64_t> receive_sliding_window;
        std::set<int64_t> success_sliding_window;

        int64_t last_sliding_packet_seq = -1;
        bool has_incremental_open = false;
    };

    class WriteContext {
    public:
        explicit WriteContext(PTabletWriterAddBatchResult* response)
                : _response_lock(),
                  _response(response),

                  _chunk(),
                  _row_indexes(),
                  _channel_row_idx_start_points() {}

        ~WriteContext() noexcept {
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

        void add_failed_replica_node_id(int64_t node_id, int64_t tablet_id) {
            DCHECK(_response != nullptr);
            std::lock_guard l(_response_lock);
            (*_node_id_to_abort_tablets)[node_id].emplace_back(tablet_id);
        }

        void set_node_id_to_abort_tablets(std::unordered_map<int64_t, std::vector<int64_t>>* node_id_to_abort_tablets) {
            _node_id_to_abort_tablets = node_id_to_abort_tablets;
        }

        void set_count_down_latch(BThreadCountDownLatch* latch) { _latch = latch; }

    private:
        friend class LocalTabletsChannel;

        mutable bthread::Mutex _response_lock;
        PTabletWriterAddBatchResult* _response;
        BThreadCountDownLatch* _latch{nullptr};

        Chunk _chunk;
        std::unique_ptr<uint32_t[]> _row_indexes;
        std::unique_ptr<uint32_t[]> _channel_row_idx_start_points;
        std::unordered_map<int64_t, std::vector<int64_t>>* _node_id_to_abort_tablets;
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

    StatusOr<std::shared_ptr<WriteContext>> _create_write_context(Chunk* chunk,
                                                                  const PTabletWriterAddChunkRequest& request,
                                                                  PTabletWriterAddBatchResult* response);

    int _close_sender(const int64_t* partitions, size_t partitions_size);

    void _commit_tablets(const PTabletWriterAddChunkRequest& request,
                         const std::shared_ptr<LocalTabletsChannel::WriteContext>& context);

    void _abort_replica_tablets(const PTabletWriterAddChunkRequest& request, const std::string& abort_reason,
                                const std::unordered_map<int64_t, std::vector<int64_t>>& node_id_to_abort_tablets);

    void _flush_stale_memtables();

    void _update_peer_replica_profile(DeltaWriter* writer, RuntimeProfile* profile);
    void _update_primary_replica_profile(DeltaWriter* writer, RuntimeProfile* profile);
    void _update_secondary_replica_profile(DeltaWriter* writer, RuntimeProfile* profile);

    void _diagnose_primary_replica_stack_trace(int64_t tablet_id, const PUniqueId& load_id,
                                               AsyncDeltaWriter* async_delta_writer);

    LoadChannel* _load_channel;

    TabletsChannelKey _key;

    MemTracker* _mem_tracker;

    RuntimeProfile* _profile;

    // initialized in open function
    int64_t _txn_id = -1;
    int64_t _index_id = -1;
    int64_t _node_id = -1;
    std::shared_ptr<OlapTableSchemaParam> _schema;
    TupleDescriptor* _tuple_desc = nullptr;

    std::vector<Sender> _senders;
    size_t _max_sliding_window_size = config::max_load_dop * 3;

    mutable bthread::Mutex _partitions_ids_lock;
    std::unordered_set<int64_t> _partition_ids;

    // rw mutex to protect the following two maps
    mutable bthreads::BThreadSharedMutex _rw_mtx;
    std::unordered_map<int64_t, uint32_t> _tablet_id_to_sorted_indexes;
    // tablet_id -> TabletChannel
    std::unordered_map<int64_t, std::unique_ptr<AsyncDeltaWriter>> _delta_writers;

    GlobalDictByNameMaps _global_dicts;
    std::unique_ptr<MemPool> _mem_pool;

    bool _is_replicated_storage = false;

    std::unordered_map<int64_t, PNetworkAddress> _node_id_to_endpoint;

    // Initially load tablets are not present on this node, so there will be no TabletsChannel.
    // After the partition is created during data loading, there are some tablets of the new partitions on this node,
    // so a TabletsChannel needs to be created, such that _is_incremental_channel=true
    bool _is_incremental_channel = false;

    mutable bthread::Mutex _status_lock;
    Status _status = Status::OK();

    std::map<string, string> _column_to_expr_value;

    // Profile counters
    // Number of times that update_profile() is called
    RuntimeProfile::Counter* _profile_update_counter = nullptr;
    // Accumulated time for update_profile()
    RuntimeProfile::Counter* _profile_update_timer = nullptr;
    // Number of times that open() is called
    RuntimeProfile::Counter* _open_counter = nullptr;
    // Accumulated time of open()
    RuntimeProfile::Counter* _open_timer = nullptr;
    // Number of times that add_chunk() is called
    RuntimeProfile::Counter* _add_chunk_counter = nullptr;
    // Accumulated time of add_chunk()
    RuntimeProfile::Counter* _add_chunk_timer = nullptr;
    // Number of rows added to this channel
    RuntimeProfile::Counter* _add_row_num = nullptr;
    // Accumulated time to wait for memtable flush in add_chunk()
    RuntimeProfile::Counter* _wait_flush_timer = nullptr;
    // Accumulated time to submit write task to delta writer thread pool in add_chunk()
    RuntimeProfile::Counter* _submit_write_task_timer = nullptr;
    // Accumulated time to submit commit task to delta writer thread pool in add_chunk()
    RuntimeProfile::Counter* _submit_commit_task_timer = nullptr;
    // Accumulated time to wait for async delta writers in add_chunk()
    RuntimeProfile::Counter* _wait_write_timer = nullptr;
    // Accumulated time to wait for secondary replicas in add_chunk()
    RuntimeProfile::Counter* _wait_replica_timer = nullptr;
    // Accumulated time to wait for txn persist in add_chunk()
    RuntimeProfile::Counter* _wait_txn_persist_timer = nullptr;
    // Accumulated time to wait sender close in add_chunk()
    RuntimeProfile::Counter* _wait_drain_sender_timer = nullptr;

    std::atomic<bool> _is_updating_profile{false};
    std::unique_ptr<RuntimeProfile> _tablets_profile;
};

std::shared_ptr<TabletsChannel> new_local_tablets_channel(LoadChannel* load_channel, const TabletsChannelKey& key,
                                                          MemTracker* mem_tracker, RuntimeProfile* parent_profile);

} // namespace starrocks
