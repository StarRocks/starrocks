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
#include "storage/lake/delta_writer.h"
#include "storage/lake/delta_writer_finish_mode.h"
#include "storage/memtable.h"
#include "storage/memtable_flush_executor.h"
#include "storage/storage_engine.h"
#include "util/bthreads/bthread_shared_mutex.h"
#include "util/compression/block_compression.h"
#include "util/countdown_latch.h"
#include "util/runtime_profile.h"
#include "util/stack_trace_mutex.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

namespace lake {
class TabletManager;
}

class LakeTabletsChannel : public TabletsChannel {
    using AsyncDeltaWriter = lake::AsyncDeltaWriter;
    using DeltaWriter = lake::DeltaWriter;
    using TxnLogPtr = AsyncDeltaWriter::TxnLogPtr;

public:
    LakeTabletsChannel(LoadChannel* load_channel, lake::TabletManager* tablet_manager, const TabletsChannelKey& key,
                       MemTracker* mem_tracker, RuntimeProfile* parent_profile);

    ~LakeTabletsChannel() override;

    DISALLOW_COPY_AND_MOVE(LakeTabletsChannel);

    const TabletsChannelKey& key() const { return _key; }

    Status open(const PTabletWriterOpenRequest& params, PTabletWriterOpenResult* result,
                std::shared_ptr<OlapTableSchemaParam> schema, bool is_incremental) override;

    void add_chunk(Chunk* chunk, const PTabletWriterAddChunkRequest& request, PTabletWriterAddBatchResult* response,
                   bool* close_channel_ptr) override;

    Status incremental_open(const PTabletWriterOpenRequest& params, PTabletWriterOpenResult* result,
                            std::shared_ptr<OlapTableSchemaParam> schema) override;

    void cancel() override;

    void abort() override;

    void abort(const std::vector<int64_t>& tablet_ids, const std::string& reason) override { return abort(); }

    void update_profile() override;

    MemTracker* mem_tracker() { return _mem_tracker; }

private:
    using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;

    struct Sender {
        StackTraceMutex<bthread::Mutex> lock;
        int64_t next_seq = 0;
        bool has_incremental_open = false;
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
            std::string msg = strings::Substitute("$0: $1", BackendOptions::get_localhost(), status.message());
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

        // NOT thread-safe
        void add_txn_logs(const std::vector<TxnLogPtr>& logs) {
            _response->mutable_lake_tablet_data()->mutable_txn_logs()->Reserve(logs.size());
            for (auto& log : logs) {
                _response->mutable_lake_tablet_data()->add_txn_logs()->CopyFrom(*log);
            }
        }

    private:
        friend class LakeTabletsChannel;

        mutable StackTraceMutex<bthread::Mutex> _mtx;
        PTabletWriterAddBatchResult* _response;

        Chunk _chunk;
        std::unique_ptr<uint32_t[]> _row_indexes;
        std::unique_ptr<uint32_t[]> _channel_row_idx_start_points;
    };

    class TxnLogCollector {
    public:
        void add(TxnLogPtr log) {
            std::lock_guard l(_mtx);
            _logs.emplace_back(std::move(log));
        }

        void update_status(const Status& st) {
            std::lock_guard l(_mtx);
            _st.update(st);
        }

        Status status() const {
            std::lock_guard l(_mtx);
            return _st;
        }

        std::vector<TxnLogPtr> logs() {
            std::lock_guard l(_mtx);
            return _logs;
        }

        // Returns true on notified, false on timeout
        bool wait(int64_t timeout_ms) {
            std::unique_lock l(_mtx);
            while (!_notified) {
                if (_cond.wait_for(l, timeout_ms * 1000L) == ETIMEDOUT) {
                    return false;
                }
            }
            return true;
        }

        void notify() {
            {
                std::lock_guard l(_mtx);
                _notified = true;
            }
            _cond.notify_all();
        }

    private:
        mutable bthread::Mutex _mtx;
        bthread::ConditionVariable _cond;
        std::vector<TxnLogPtr> _logs;
        Status _st;
        bool _notified{false};
    };

    // called by open() or incremental_open to build AsyncDeltaWriter for tablets
    Status _create_delta_writers(const PTabletWriterOpenRequest& params, bool is_incremental);

    StatusOr<std::unique_ptr<WriteContext>> _create_write_context(Chunk* chunk,
                                                                  const PTabletWriterAddChunkRequest& request,
                                                                  PTabletWriterAddBatchResult* response);

    int _close_sender(const int64_t* partitions, size_t partitions_size);

    void _flush_stale_memtables();

    void _update_tablet_profile(DeltaWriter* writer, RuntimeProfile* profile);

    LoadChannel* _load_channel;
    lake::TabletManager* _tablet_manager;

    TabletsChannelKey _key;

    MemTracker* _mem_tracker;

    RuntimeProfile* _profile;

    // initialized in open function
    int64_t _txn_id = -1;
    int64_t _index_id = -1;
    std::shared_ptr<OlapTableSchemaParam> _schema;

    std::vector<Sender> _senders;

    mutable StackTraceMutex<bthread::Mutex> _dirty_partitions_lock;
    std::unordered_set<int64_t> _dirty_partitions;

    mutable StackTraceMutex<bthread::Mutex> _chunk_meta_lock;
    serde::ProtobufChunkMeta _chunk_meta;
    std::atomic<bool> _has_chunk_meta;

    // shared mutex to protect the unordered_map
    // * open/incremental_open needs to modify the map
    // * other interfaces needs to read the map
    mutable bthreads::BThreadSharedMutex _rw_mtx;
    std::unordered_map<int64_t, uint32_t> _tablet_id_to_sorted_indexes;
    std::unordered_map<int64_t, std::unique_ptr<AsyncDeltaWriter>> _delta_writers;

    GlobalDictByNameMaps _global_dicts;
    std::unique_ptr<MemPool> _mem_pool;
    bool _is_incremental_channel{false};
    lake::DeltaWriterFinishMode _finish_mode{lake::DeltaWriterFinishMode::kWriteTxnLog};
    TxnLogCollector _txn_log_collector;

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
    // Accumulated time to wait for async delta writers in add_chunk()
    RuntimeProfile::Counter* _wait_writer_timer = nullptr;

    std::atomic<bool> _is_updating_profile{false};
    std::unique_ptr<RuntimeProfile> _tablets_profile;
};

LakeTabletsChannel::LakeTabletsChannel(LoadChannel* load_channel, lake::TabletManager* tablet_manager,
                                       const TabletsChannelKey& key, MemTracker* mem_tracker,
                                       RuntimeProfile* parent_profile)
        : TabletsChannel(),
          _load_channel(load_channel),
          _tablet_manager(tablet_manager),
          _key(key),
          _mem_tracker(mem_tracker),
          _mem_pool(std::make_unique<MemPool>()) {
    _profile = parent_profile->create_child(fmt::format("Index (id={})", key.index_id));
    _profile_update_counter = ADD_COUNTER(_profile, "ProfileUpdateCount", TUnit::UNIT);
    _profile_update_timer = ADD_TIMER(_profile, "ProfileUpdateTime");
    _open_counter = ADD_COUNTER(_profile, "OpenRpcCount", TUnit::UNIT);
    _open_timer = ADD_TIMER(_profile, "OpenRpcTime");
    _add_chunk_counter = ADD_COUNTER(_profile, "AddChunkRpcCount", TUnit::UNIT);
    _add_chunk_timer = ADD_TIMER(_profile, "AddChunkRpcTime");
    _add_row_num = ADD_COUNTER(_profile, "AddRowNum", TUnit::UNIT);
    _wait_flush_timer = ADD_CHILD_TIMER(_profile, "WaitFlushTime", "AddChunkRpcTime");
    _wait_writer_timer = ADD_CHILD_TIMER(_profile, "WaitWriteTime", "AddChunkRpcTime");
    _tablets_profile = std::make_unique<RuntimeProfile>("TabletsProfile");
}

LakeTabletsChannel::~LakeTabletsChannel() {
    _mem_pool.reset();
}

Status LakeTabletsChannel::open(const PTabletWriterOpenRequest& params, PTabletWriterOpenResult* result,
                                std::shared_ptr<OlapTableSchemaParam> schema, bool is_incremental) {
    DCHECK_EQ(-1, _txn_id);
    SCOPED_TIMER(_open_timer);
    COUNTER_UPDATE(_open_counter, 1);
    std::unique_lock<bthreads::BThreadSharedMutex> l(_rw_mtx);
    _txn_id = params.txn_id();
    _index_id = params.index_id();
    _schema = schema;
#ifndef BE_TEST
    _table_metrics = StarRocksMetrics::instance()->table_metrics(_schema->table_id());
#endif
    _is_incremental_channel = is_incremental;
    if (params.has_lake_tablet_params() && params.lake_tablet_params().has_write_txn_log()) {
        _finish_mode = params.lake_tablet_params().write_txn_log() ? lake::DeltaWriterFinishMode::kWriteTxnLog
                                                                   : lake::DeltaWriterFinishMode::kDontWriteTxnLog;
    }

    _senders = std::vector<Sender>(params.num_senders());
    if (_is_incremental_channel) {
        _num_remaining_senders.fetch_add(1, std::memory_order_release);
        _senders[params.sender_id()].has_incremental_open = true;
    } else {
        _num_remaining_senders.store(params.num_senders(), std::memory_order_release);
        _num_initial_senders.store(params.num_senders(), std::memory_order_release);
    }

    for (auto& index_schema : params.schema().indexes()) {
        if (index_schema.id() != _index_id) {
            continue;
        }
        for (auto& entry : index_schema.column_to_expr_value()) {
            _column_to_expr_value.insert({entry.first, entry.second});
        }
    }

    RETURN_IF_ERROR(_create_delta_writers(params, false));

    for (auto& [id, writer] : _delta_writers) {
        auto st = writer->check_immutable();
        if (!st.ok()) {
            LOG(WARNING) << "check immutable failed, tablet " << id << ", txn " << _txn_id << ", status " << st;
        }
        if (writer->is_immutable()) {
            result->add_immutable_tablet_ids(id);
            result->add_immutable_partition_ids(writer->partition_id());
        }
        VLOG(2) << "check tablet writer for tablet " << id << ", partition " << writer->partition_id() << ", txn "
                << _txn_id << ", is_immutable  " << writer->is_immutable();
    }

    return Status::OK();
}

void LakeTabletsChannel::add_chunk(Chunk* chunk, const PTabletWriterAddChunkRequest& request,
                                   PTabletWriterAddBatchResult* response, bool* close_channel_ptr) {
    bool& close_channel = *close_channel_ptr;
    close_channel = false;
    MonotonicStopWatch watch;
    watch.start();
    std::shared_lock<bthreads::BThreadSharedMutex> rolk(_rw_mtx);

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
    size_t chunk_size = chunk != nullptr ? chunk->bytes_usage() : 0;

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

    int64_t wait_memtable_flush_time_ns = 0;
    int32_t total_row_num = 0;
    // Open and write AsyncDeltaWriter
    for (int i = 0; i < channel_size; ++i) {
        size_t from = channel_row_idx_start_points[i];
        size_t size = channel_row_idx_start_points[i + 1] - from;
        if (size == 0) {
            count_down_latch.count_down();
            continue;
        }
        total_row_num += size;
        int64_t tablet_id = tablet_ids[row_indexes[from]];
        auto& dw = _delta_writers[tablet_id];
        if (dw == nullptr) {
            LOG(WARNING) << "LakeTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(request.id())
                         << " not found tablet_id: " << tablet_id;
            response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
            response->mutable_status()->add_error_msgs(
                    fmt::format("Failed to add_chunk since tablet_id {} not exists, txn_id: {}, load_id: {}", tablet_id,
                                _txn_id, print_id(request.id())));
            return;
        }

        // back pressure OlapTableSink since there are too many memtables need to flush
        while (dw->queueing_memtable_num() >= config::max_queueing_memtable_per_tablet) {
            if (watch.elapsed_time() / 1000000 > request.timeout_ms()) {
                LOG(INFO) << "LakeTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(request.id())
                          << " wait tablet " << tablet_id << " flush memtable " << request.timeout_ms()
                          << "ms still has queueing num " << dw->queueing_memtable_num();
                break;
            }
            bthread_usleep(10000); // 10ms
            wait_memtable_flush_time_ns += 10000000;
        }

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
                dw->finish(_finish_mode, [&, id = tablet_id](StatusOr<TxnLogPtr> res) {
                    if (!res.ok()) {
                        context->update_status(res.status());
                        LOG(ERROR) << "Fail to finish tablet " << id << ": " << res.status();
                    } else {
                        context->add_finished_tablet(id);
                        VLOG(5) << "Finished tablet " << id;
                    }
                    if (_finish_mode == lake::DeltaWriterFinishMode::kDontWriteTxnLog) {
                        if (!res.ok()) {
                            _txn_log_collector.update_status(res.status());
                        } else {
                            _txn_log_collector.add(std::move(res).value());
                        }
                    }
                    count_down_latch.count_down();
                });
            }
        } else {
            count_down_latch.count_down(_delta_writers.size());
            context->update_status(Status::InternalError("Unexpected state: unfinished_senders < 0"));
        }
    }

    auto start_wait_writer_ts = watch.elapsed_time();
    // Block the current bthread(not pthread) until all `write()` and `finish()` tasks finished.
    count_down_latch.wait();
    auto finish_wait_writer_ts = watch.elapsed_time();

    if (request.eos() || context->_response->status().status_code() == TStatusCode::OK) {
        // ^^^^^^^^^^ Reject new requests once eos request received.
        sender.next_seq++;
    }

    std::set<long> immutable_tablet_ids;
    for (auto tablet_id : request.tablet_ids()) {
        auto& writer = _delta_writers[tablet_id];
        if (writer->is_immutable() && immutable_tablet_ids.count(tablet_id) == 0) {
            response->add_immutable_tablet_ids(tablet_id);
            response->add_immutable_partition_ids(writer->partition_id());
            immutable_tablet_ids.insert(tablet_id);

            _insert_immutable_partition(writer->partition_id());
        }
    }

    // stale memtable generated by transaction stream load / automatic bucket
    // since there may be a long period without new write requests,
    // which prevents triggering a flush,
    // we need to proactively perform a flush when memory resources are insufficient.
    _flush_stale_memtables();

    response->set_execution_time_us(watch.elapsed_time() / 1000);
    response->set_wait_lock_time_us(0); // We didn't measure the lock wait time, just give the caller a fake time
    response->set_wait_memtable_flush_time_us(wait_memtable_flush_time_ns / 1000);

    if (!close_channel && request.wait_all_sender_close()) {
        _num_initial_senders.fetch_sub(1);
        std::string msg = fmt::format("LakeTabletsChannel txn_id: {} load_id: {}", _txn_id, print_id(request.id()));
        // wait for senders to be closed, may be timed out
        auto remain = request.timeout_ms();
        remain -= watch.elapsed_time() / 1000000;
        LOG(INFO) << msg << ", wait for all senders closed ...";

        // unlock write lock so that incremental open can aquire read lock
        rolk.unlock();
        drain_senders(remain * 1000, msg);
    }

    auto wait_writer_ns = finish_wait_writer_ts - start_wait_writer_ts;
#ifndef BE_TEST
    _table_metrics->load_rows.increment(total_row_num);
    _table_metrics->load_bytes.increment(chunk_size);
#endif
    COUNTER_UPDATE(_add_chunk_counter, 1);
    COUNTER_UPDATE(_add_chunk_timer, watch.elapsed_time());
    COUNTER_UPDATE(_add_row_num, total_row_num);
    COUNTER_UPDATE(_wait_flush_timer, wait_memtable_flush_time_ns);
    COUNTER_UPDATE(_wait_writer_timer, wait_writer_ns);

    if (close_channel) {
        _load_channel->remove_tablets_channel(_key);
        if (_finish_mode == lake::DeltaWriterFinishMode::kDontWriteTxnLog) {
            _txn_log_collector.notify();
        }
    }

    // Sender 0 is responsible for waiting for all other senders to finish and collecting txn logs
    if (_finish_mode == lake::kDontWriteTxnLog && request.eos() && (request.sender_id() == 0) &&
        response->status().status_code() == TStatusCode::OK) {
        rolk.unlock();
        auto t = request.timeout_ms() - (int64_t)(watch.elapsed_time() / 1000 / 1000);
        auto ok = _txn_log_collector.wait(t);
        auto st = ok ? _txn_log_collector.status() : Status::TimedOut(fmt::format("wait txn log timed out: {}", t));
        if (st.ok()) {
            context->add_txn_logs(_txn_log_collector.logs());
        } else {
            context->update_status(st);
        }
    }
}

int LakeTabletsChannel::_close_sender(const int64_t* partitions, size_t partitions_size) {
    int n = _num_remaining_senders.fetch_sub(1);
    // if sender close means data send finished, we need to decrease _num_initial_senders
    _num_initial_senders.fetch_sub(1);
    std::lock_guard l(_dirty_partitions_lock);
    for (int i = 0; i < partitions_size; i++) {
        _dirty_partitions.insert(partitions[i]);
    }
    return n - 1;
}

static void null_callback(const Status& status) {
    (void)status;
}

void LakeTabletsChannel::_flush_stale_memtables() {
    if (_is_immutable_partition_empty() && config::stale_memtable_flush_time_sec <= 0) {
        return;
    }
    bool high_mem_usage = false;
    if (_mem_tracker->limit_exceeded_by_ratio(70) ||
        (_mem_tracker->parent() != nullptr && _mem_tracker->parent()->limit_exceeded_by_ratio(70))) {
        high_mem_usage = true;
    }

    int64_t now = butil::gettimeofday_s();
    for (auto& [tablet_id, writer] : _delta_writers) {
        bool log_flushed = false;
        auto last_write_ts = writer->last_write_ts();
        if (last_write_ts > 0) {
            if (_has_immutable_partition(writer->partition_id())) {
                if (high_mem_usage) {
                    log_flushed = true;
                    writer->flush(null_callback);
                } else if (now - last_write_ts > 1) {
                    log_flushed = true;
                    writer->flush(null_callback);
                }
            } else if (config::stale_memtable_flush_time_sec > 0) {
                if (high_mem_usage && now - last_write_ts > config::stale_memtable_flush_time_sec) {
                    log_flushed = true;
                    writer->flush(null_callback);
                }
            }
            if (log_flushed) {
                VLOG(2) << "Flush stale memtable tablet_id: " << tablet_id << " txn_id: " << _txn_id
                        << " partition_id: " << writer->partition_id() << " is_immutable: " << writer->is_immutable()
                        << " last_write_ts: " << now - last_write_ts
                        << " job_mem_usage: " << _mem_tracker->consumption()
                        << " job_mem_limit: " << _mem_tracker->limit()
                        << " load_mem_usage: " << _mem_tracker->parent()->consumption()
                        << " load_mem_limit: " << _mem_tracker->parent()->limit();
            }
        }
    }
}

Status LakeTabletsChannel::_create_delta_writers(const PTabletWriterOpenRequest& params, bool is_incremental) {
    int64_t schema_id = 0;
    std::vector<SlotDescriptor*>* slots = nullptr;
    for (auto& index : _schema->indexes()) {
        if (index->index_id == _index_id) {
            slots = &index->slots;
            schema_id = index->schema_id;
            break;
        }
    }
    if (slots == nullptr) {
        return Status::InvalidArgument(fmt::format("Unknown index_id: {}", _key.to_string()));
    }
    // init global dict info if needed
    if (!is_incremental) {
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
    }

    std::vector<int64_t> tablet_ids;
    tablet_ids.reserve(params.tablets_size());
    for (const PTabletWithPartition& tablet : params.tablets()) {
        if (_delta_writers.count(tablet.tablet_id()) != 0) {
            // already created for the tablet, usually in incremental open case
            continue;
        }
        ASSIGN_OR_RETURN(auto writer, lake::AsyncDeltaWriterBuilder()
                                              .set_tablet_manager(_tablet_manager)
                                              .set_tablet_id(tablet.tablet_id())
                                              .set_txn_id(_txn_id)
                                              .set_partition_id(tablet.partition_id())
                                              .set_slot_descriptors(slots)
                                              .set_merge_condition(params.merge_condition())
                                              .set_miss_auto_increment_column(params.miss_auto_increment_column())
                                              .set_table_id(params.table_id())
                                              .set_immutable_tablet_size(params.immutable_tablet_size())
                                              .set_mem_tracker(_mem_tracker)
                                              .set_schema_id(schema_id)
                                              .set_partial_update_mode(params.partial_update_mode())
                                              .set_column_to_expr_value(&_column_to_expr_value)
                                              .set_load_id(params.id())
                                              .set_profile(_profile)
                                              .build());
        _delta_writers.emplace(tablet.tablet_id(), std::move(writer));
        tablet_ids.emplace_back(tablet.tablet_id());
    }
    if (!tablet_ids.empty()) { // has new tablets added, need rebuild the sorted index
        tablet_ids.reserve(tablet_ids.size() + _tablet_id_to_sorted_indexes.size());
        for (auto& iter : _tablet_id_to_sorted_indexes) {
            tablet_ids.emplace_back(iter.first);
        }
        // In order to get sorted index for each tablet
        std::sort(tablet_ids.begin(), tablet_ids.end());
        DCHECK_EQ(_delta_writers.size(), tablet_ids.size());
        for (size_t i = 0; i < tablet_ids.size(); ++i) {
            _tablet_id_to_sorted_indexes[tablet_ids[i]] = i;
        }
    }
    return Status::OK();
}

void LakeTabletsChannel::abort() {
    std::shared_lock<bthreads::BThreadSharedMutex> l(_rw_mtx);
    for (auto& it : _delta_writers) {
        it.second->close();
    }
}

void LakeTabletsChannel::cancel() {
    //TODO: Current LakeDeltaWriter don't support fast cancel
}

StatusOr<std::unique_ptr<LakeTabletsChannel::WriteContext>> LakeTabletsChannel::_create_write_context(
        Chunk* chunk, const PTabletWriterAddChunkRequest& request, PTabletWriterAddBatchResult* response) {
    if (chunk == nullptr && !request.eos() && !request.wait_all_sender_close()) {
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

Status LakeTabletsChannel::incremental_open(const PTabletWriterOpenRequest& params, PTabletWriterOpenResult* result,
                                            std::shared_ptr<OlapTableSchemaParam> schema) {
    std::unique_lock<bthreads::BThreadSharedMutex> l(_rw_mtx);
    RETURN_IF_ERROR(_create_delta_writers(params, true));
    // properly set incremental flags and counters
    if (_is_incremental_channel && !_senders[params.sender_id()].has_incremental_open) {
        _num_remaining_senders.fetch_add(1, std::memory_order_release);
        _senders[params.sender_id()].has_incremental_open = true;
    }
    return Status::OK();
}

void LakeTabletsChannel::update_profile() {
    if (_profile == nullptr) {
        return;
    }

    bool expect = false;
    if (!_is_updating_profile.compare_exchange_strong(expect, true)) {
        // skip concurrent update
        return;
    }
    DeferOp defer([this]() { _is_updating_profile.store(false); });
    _profile->inc_version();
    COUNTER_UPDATE(_profile_update_counter, 1);
    SCOPED_TIMER(_profile_update_timer);

    std::vector<DeltaWriter*> delta_writers;
    {
        std::shared_lock<bthreads::BThreadSharedMutex> lk(_rw_mtx);
        delta_writers.reserve(_delta_writers.size());
        for (auto& [tablet_id, async_writer] : _delta_writers) {
            delta_writers.push_back(async_writer->delta_writer());
        }
    }

    std::vector<RuntimeProfile*> tablets_profile;
    tablets_profile.reserve(delta_writers.size());
    for (auto* writer : delta_writers) {
        RuntimeProfile* profile = _tablets_profile->create_child(fmt::format("{}", writer->tablet_id()));
        _update_tablet_profile(writer, profile);
        tablets_profile.push_back(profile);
    }

    ObjectPool obj_pool;
    if (!tablets_profile.empty()) {
        auto* merged_profile = RuntimeProfile::merge_isomorphic_profiles(&obj_pool, tablets_profile);
        RuntimeProfile* final_profile = _profile->create_child("PeerReplicas");
        auto* tablets_counter = ADD_COUNTER(final_profile, "TabletsNum", TUnit::UNIT);
        COUNTER_UPDATE(tablets_counter, tablets_profile.size());
        final_profile->copy_all_info_strings_from(merged_profile);
        final_profile->copy_all_counters_from(merged_profile);
    }
}

#define ADD_AND_UPDATE_COUNTER(profile, name, type, val) (ADD_COUNTER(profile, name, type))->update(val)
#define ADD_AND_UPDATE_TIMER(profile, name, val) (ADD_TIMER(profile, name))->update(val)
#define DEFAULT_IF_NULL(ptr, value, default_value) ((ptr) ? (value) : (default_value))

void LakeTabletsChannel::_update_tablet_profile(DeltaWriter* writer, RuntimeProfile* profile) {
    const lake::DeltaWriterStat& writer_stat = writer->get_writer_stat();
    ADD_AND_UPDATE_COUNTER(profile, "WriterTaskCount", TUnit::UNIT, writer_stat.task_count);
    ADD_AND_UPDATE_TIMER(profile, "WriterTaskPendingTime", writer_stat.pending_time_ns);
    ADD_AND_UPDATE_COUNTER(profile, "WriteCount", TUnit::UNIT, writer_stat.write_count);
    ADD_AND_UPDATE_COUNTER(profile, "RowCount", TUnit::UNIT, writer_stat.row_count);
    ADD_AND_UPDATE_TIMER(profile, "WriteTime", writer_stat.write_time_ns);
    ADD_AND_UPDATE_COUNTER(profile, "MemtableFullCount", TUnit::UNIT, writer_stat.memtable_full_count);
    ADD_AND_UPDATE_COUNTER(profile, "MemoryExceedCount", TUnit::UNIT, writer_stat.memory_exceed_count);
    ADD_AND_UPDATE_TIMER(profile, "WriteWaitFlushTime", writer_stat.write_wait_flush_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "CloseTime", writer_stat.close_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "FinishTime", writer_stat.finish_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "FinishWaitFlushTime", writer_stat.finish_wait_flush_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "FinishPrepareTxnLogTime", writer_stat.finish_prepare_txn_log_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "FinishPutTxnLogTime", writer_stat.finish_put_txn_log_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "FinishPkPreloadTime", writer_stat.finish_pk_preload_time_ns);

    const FlushStatistic* flush_stat = writer->get_flush_stats();
    ADD_AND_UPDATE_COUNTER(profile, "MemtableFlushedCount", TUnit::UNIT,
                           DEFAULT_IF_NULL(flush_stat, flush_stat->flush_count, 0));
    ADD_AND_UPDATE_COUNTER(profile, "MemtableFlushingCount", TUnit::UNIT,
                           DEFAULT_IF_NULL(flush_stat, flush_stat->cur_flush_count, 0));
    ADD_AND_UPDATE_COUNTER(profile, "MemtableQueueCount", TUnit::UNIT,
                           DEFAULT_IF_NULL(flush_stat, flush_stat->queueing_memtable_num.load(), 0));
    ADD_AND_UPDATE_TIMER(profile, "FlushTaskPendingTime", DEFAULT_IF_NULL(flush_stat, flush_stat->pending_time_ns, 0));
    ADD_AND_UPDATE_COUNTER(profile, "MemtableInsertCount", TUnit::UNIT,
                           DEFAULT_IF_NULL(flush_stat, flush_stat->memtable_stats.insert_count.load(), 0));
    ADD_AND_UPDATE_TIMER(profile, "MemtableInsertTime",
                         DEFAULT_IF_NULL(flush_stat, flush_stat->memtable_stats.insert_time_ns.load(), 0));
    ADD_AND_UPDATE_TIMER(profile, "MemtableFinalizeTime",
                         DEFAULT_IF_NULL(flush_stat, flush_stat->memtable_stats.finalize_time_ns.load(), 0));
    ADD_AND_UPDATE_COUNTER(profile, "MemtableSortCount", TUnit::UNIT,
                           DEFAULT_IF_NULL(flush_stat, flush_stat->memtable_stats.sort_count.load(), 0));
    ADD_AND_UPDATE_TIMER(profile, "MemtableSortTime",
                         DEFAULT_IF_NULL(flush_stat, flush_stat->memtable_stats.sort_time_ns.load(), 0));
    ADD_AND_UPDATE_COUNTER(profile, "MemtableAggCount", TUnit::UNIT,
                           DEFAULT_IF_NULL(flush_stat, flush_stat->memtable_stats.agg_count.load(), 0));
    ADD_AND_UPDATE_TIMER(profile, "MemtableAggTime",
                         DEFAULT_IF_NULL(flush_stat, flush_stat->memtable_stats.agg_time_ns.load(), 0));
    ADD_AND_UPDATE_TIMER(profile, "MemtableFlushTime",
                         DEFAULT_IF_NULL(flush_stat, flush_stat->memtable_stats.flush_time_ns.load(), 0));
    ADD_AND_UPDATE_TIMER(profile, "MemtableIOTime",
                         DEFAULT_IF_NULL(flush_stat, flush_stat->memtable_stats.io_time_ns.load(), 0));
    ADD_AND_UPDATE_COUNTER(profile, "MemtableMemorySize", TUnit::BYTES,
                           DEFAULT_IF_NULL(flush_stat, flush_stat->memtable_stats.flush_memory_size.load(), 0));
    ADD_AND_UPDATE_COUNTER(profile, "MemtableDiskSize", TUnit::BYTES,
                           DEFAULT_IF_NULL(flush_stat, flush_stat->memtable_stats.flush_disk_size.load(), 0));
}

std::shared_ptr<TabletsChannel> new_lake_tablets_channel(LoadChannel* load_channel, lake::TabletManager* tablet_manager,
                                                         const TabletsChannelKey& key, MemTracker* mem_tracker,
                                                         RuntimeProfile* parent_profile) {
    return std::make_shared<LakeTabletsChannel>(load_channel, tablet_manager, key, mem_tracker, parent_profile);
}

} // namespace starrocks
