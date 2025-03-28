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

#include "runtime/local_tablets_channel.h"

#include <fmt/format.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <unordered_map>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "common/closure_guard.h"
#include "common/statusor.h"
#include "exec/tablet_info.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/ref_counted.h"
#include "gutil/strings/join.h"
#include "runtime/descriptors.h"
#include "runtime/global_dict/types.h"
#include "runtime/global_dict/types_fwd_decl.h"
#include "runtime/load_channel.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/tablets_channel.h"
#include "serde/protobuf_serde.h"
#include "storage/delta_writer.h"
#include "storage/memtable.h"
#include "storage/segment_flush_executor.h"
#include "storage/segment_replicate_executor.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/txn_manager.h"
#include "util/brpc_stub_cache.h"
#include "util/compression/block_compression.h"
#include "util/failpoint/fail_point.h"
#include "util/faststring.h"
#include "util/starrocks_metrics.h"
#include "util/stopwatch.hpp"

namespace starrocks {

DEFINE_FAIL_POINT(tablets_channel_add_chunk_wait_write_block);
DEFINE_FAIL_POINT(tablets_channel_wait_secondary_replica_block);

std::atomic<uint64_t> LocalTabletsChannel::_s_tablet_writer_count;

LocalTabletsChannel::LocalTabletsChannel(LoadChannel* load_channel, const TabletsChannelKey& key,
                                         MemTracker* mem_tracker, RuntimeProfile* parent_profile)
        : TabletsChannel(),
          _load_channel(load_channel),
          _key(key),
          _mem_tracker(mem_tracker),
          _mem_pool(std::make_unique<MemPool>()) {
    static std::once_flag once_flag;
    std::call_once(once_flag, [] {
        REGISTER_GAUGE_STARROCKS_METRIC(tablet_writer_count, [&]() { return _s_tablet_writer_count.load(); });
    });

    _profile = parent_profile->create_child(fmt::format("Index (id={})", key.index_id));
    _profile_update_counter = ADD_COUNTER(_profile, "ProfileUpdateCount", TUnit::UNIT);
    _profile_update_timer = ADD_TIMER(_profile, "ProfileUpdateTime");
    _open_counter = ADD_COUNTER(_profile, "OpenRpcCount", TUnit::UNIT);
    _open_timer = ADD_TIMER(_profile, "OpenRpcTime");
    _add_chunk_counter = ADD_COUNTER(_profile, "AddChunkRpcCount", TUnit::UNIT);
    _add_row_num = ADD_COUNTER(_profile, "AddRowNum", TUnit::UNIT);
    _add_chunk_timer = ADD_TIMER(_profile, "AddChunkRpcTime");
    _wait_flush_timer = ADD_CHILD_TIMER(_profile, "WaitFlushTime", "AddChunkRpcTime");
    _submit_write_task_timer = ADD_CHILD_TIMER(_profile, "SubmitWriteTaskTime", "AddChunkRpcTime");
    _submit_commit_task_timer = ADD_CHILD_TIMER(_profile, "SubmitCommitTaskTime", "AddChunkRpcTime");
    _wait_write_timer = ADD_CHILD_TIMER(_profile, "WaitWriteTime", "AddChunkRpcTime");
    _wait_replica_timer = ADD_CHILD_TIMER(_profile, "WaitReplicaTime", "AddChunkRpcTime");
    _wait_txn_persist_timer = ADD_CHILD_TIMER(_profile, "WaitTxnPersistTime", "AddChunkRpcTime");
    _wait_drain_sender_timer = ADD_CHILD_TIMER(_profile, "WaitDrainSenderTime", "AddChunkRpcTime");
    _tablets_profile = std::make_unique<RuntimeProfile>("TabletsProfile");
}

LocalTabletsChannel::~LocalTabletsChannel() {
    _s_tablet_writer_count -= _delta_writers.size();
    _mem_pool.reset();
}

Status LocalTabletsChannel::open(const PTabletWriterOpenRequest& params, PTabletWriterOpenResult* result,
                                 std::shared_ptr<OlapTableSchemaParam> schema, bool is_incremental) {
    SCOPED_TIMER(_open_timer);
    COUNTER_UPDATE(_open_counter, 1);
    std::unique_lock<bthreads::BThreadSharedMutex> lk(_rw_mtx);
    _txn_id = params.txn_id();
    _index_id = params.index_id();
    _schema = schema;
    _tuple_desc = _schema->tuple_desc();
    _node_id = params.node_id();
#ifndef BE_TEST
    _table_metrics = StarRocksMetrics::instance()->table_metrics(_schema->table_id());
#endif

    _senders = std::vector<Sender>(params.num_senders());
    if (is_incremental) {
        _num_remaining_senders.fetch_add(1, std::memory_order_release);
        _is_incremental_channel = true;
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

    RETURN_IF_ERROR(_open_all_writers(params));

    for (auto& [id, writer] : _delta_writers) {
        if (writer->is_immutable()) {
            result->add_immutable_tablet_ids(id);
            result->add_immutable_partition_ids(writer->partition_id());
        }
    }

    return Status::OK();
}

void LocalTabletsChannel::add_segment(brpc::Controller* cntl, const PTabletWriterAddSegmentRequest* request,
                                      PTabletWriterAddSegmentResult* response, google::protobuf::Closure* done) {
    std::shared_lock<bthreads::BThreadSharedMutex> lk(_rw_mtx);
    ClosureGuard closure_guard(done);
    auto it = _delta_writers.find(request->tablet_id());
    if (it == _delta_writers.end()) {
        response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
        response->mutable_status()->add_error_msgs(
                fmt::format("PTabletWriterAddSegmentRequest tablet_id {} not exists", request->tablet_id()));
        return;
    }
    auto& delta_writer = it->second;

    AsyncDeltaWriterSegmentRequest req;
    req.cntl = cntl;
    req.request = request;
    req.response = response;
    req.done = done;

    delta_writer->write_segment(req);
    closure_guard.release();
}

void LocalTabletsChannel::add_chunk(Chunk* chunk, const PTabletWriterAddChunkRequest& request,
                                    PTabletWriterAddBatchResult* response, bool* close_channel_ptr) {
    bool& close_channel = *close_channel_ptr;
    close_channel = false;
    MonotonicStopWatch watch;
    watch.start();
    std::shared_lock<bthreads::BThreadSharedMutex> lk(_rw_mtx);

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

    {
        std::lock_guard lock(_senders[request.sender_id()].lock);

        // receive exists packet
        if (_senders[request.sender_id()].receive_sliding_window.count(request.packet_seq()) != 0) {
            if (_senders[request.sender_id()].success_sliding_window.count(request.packet_seq()) == 0 ||
                request.eos()) {
                // still in process
                response->mutable_status()->set_status_code(TStatusCode::DUPLICATE_RPC_INVOCATION);
                response->mutable_status()->add_error_msgs(fmt::format(
                        "packet_seq {} in PTabletWriterAddChunkRequest already process", request.packet_seq()));
                return;
            } else {
                // already success
                LOG(INFO) << "LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(request.id())
                          << " packet_seq " << request.packet_seq()
                          << " in PTabletWriterAddChunkRequest already success";
                response->mutable_status()->set_status_code(TStatusCode::OK);
                return;
            }
        } else {
            // receive packet before sliding window
            if (request.packet_seq() <= _senders[request.sender_id()].last_sliding_packet_seq) {
                LOG(INFO) << "LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(request.id())
                          << " packet_seq " << request.packet_seq()
                          << " in PTabletWriterAddChunkRequest less than last success packet_seq "
                          << _senders[request.sender_id()].last_sliding_packet_seq;
                response->mutable_status()->set_status_code(TStatusCode::OK);
                return;
            } else if (request.packet_seq() >
                       _senders[request.sender_id()].last_sliding_packet_seq + _max_sliding_window_size) {
                response->mutable_status()->set_status_code(TStatusCode::INVALID_ARGUMENT);
                response->mutable_status()->add_error_msgs(fmt::format(
                        "packet_seq {} in PTabletWriterAddChunkRequest forward last success packet_seq {} too much",
                        request.packet_seq(), _senders[request.sender_id()].last_sliding_packet_seq));
                return;
            } else {
                _senders[request.sender_id()].receive_sliding_window.insert(request.packet_seq());
            }
        }
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
    auto channel_row_idx_start_points = context->_channel_row_idx_start_points.get(); // May be a nullptr
    auto row_indexes = context->_row_indexes.get();                                   // May be a nullptr

    auto count_down_latch = BThreadCountDownLatch(1);

    context->set_count_down_latch(&count_down_latch);

    std::unordered_map<int64_t, std::vector<int64_t>> node_id_to_abort_tablets;
    context->set_node_id_to_abort_tablets(&node_id_to_abort_tablets);

    auto start_submit_write_task_ts = watch.elapsed_time();
    int64_t wait_memtable_flush_time_us = 0;
    int32_t total_row_num = 0;
    for (int i = 0; i < channel_size; ++i) {
        size_t from = channel_row_idx_start_points[i];
        size_t size = channel_row_idx_start_points[i + 1] - from;
        if (size == 0) {
            continue;
        }
        total_row_num += size;
        auto tablet_id = tablet_ids[row_indexes[from]];
        auto it = _delta_writers.find(tablet_id);
        if (it == _delta_writers.end()) {
            LOG(WARNING) << "LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(request.id())
                         << " not found tablet_id: " << tablet_id;
            response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
            response->mutable_status()->add_error_msgs(
                    fmt::format("Failed to add_chunk since tablet_id {} not exists, txn_id: {}, load_id: {}", tablet_id,
                                _txn_id, print_id(request.id())));
            return;
        }
        auto& delta_writer = it->second;

        // back pressure OlapTableSink since there are too many memtables need to flush
        while (delta_writer->get_state() != kAborted &&
               delta_writer->get_flush_stats().queueing_memtable_num >= config::max_queueing_memtable_per_tablet) {
            if (watch.elapsed_time() / 1000000 > request.timeout_ms()) {
                LOG(INFO) << "LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(request.id())
                          << " wait tablet " << tablet_id << " flush memtable " << request.timeout_ms()
                          << "ms still has queueing num " << delta_writer->get_flush_stats().queueing_memtable_num;
                break;
            }
            bthread_usleep(10000); // 10ms
            wait_memtable_flush_time_us += 10000;
        }

        AsyncDeltaWriterRequest req;
        req.chunk = chunk;
        req.indexes = row_indexes + from;
        req.indexes_size = size;
        req.commit_after_write = false;

        // The reference count of context is increased in the constructor of WriteCallback
        // and decreased in the destructor of WriteCallback.
        auto cb = new WriteCallback(context);

        delta_writer->write(req, cb);
    }
    auto finish_submit_write_task_ts = watch.elapsed_time();

    // _channel_row_idx_start_points no longer used, release it to free memory.
    context->_channel_row_idx_start_points.reset();

    // NOTE: Must close sender *AFTER* the write requests submitted, otherwise a delta writer commit request may
    // be executed ahead of the write requests submitted by other senders.
    if (request.eos() && _close_sender(request.partition_ids().data(), request.partition_ids_size()) == 0) {
        close_channel = true;
        _commit_tablets(request, context);
    }
    auto finish_submit_commit_task_ts = watch.elapsed_time();

    // Must reset the context pointer before waiting on the |count_down_latch|,
    // because the |count_down_latch| is decreased in the destructor of the context,
    // and the destructor of the context cannot be called unless we reset the pointer
    // here.
    context.reset();

    auto start_wait_writer_ts = watch.elapsed_time();
    // This will only block the bthread, will not block the pthread
    count_down_latch.wait();
    FAIL_POINT_TRIGGER_EXECUTE(tablets_channel_add_chunk_wait_write_block, {
        int32_t timeout_ms = config::load_fp_tablets_channel_add_chunk_block_ms;
        if (timeout_ms > 0) {
            bthread_usleep(timeout_ms * 1000);
        }
    });
    auto finish_wait_writer_ts = watch.elapsed_time();

    // Abort tablets which primary replica already failed
    if (response->status().status_code() != TStatusCode::OK) {
        _abort_replica_tablets(request, response->status().error_msgs()[0], node_id_to_abort_tablets);
    }

    // We need wait all secondary replica commit before we close the channel
    if (_is_replicated_storage && close_channel && response->status().status_code() == TStatusCode::OK) {
        bool timeout = false;
        for (auto& [tablet_id, delta_writer] : _delta_writers) {
            // Wait util seconary replica commit/abort by primary
            if (delta_writer->replica_state() == Secondary) {
                int i = 0;
                int64_t start_wait_time_ms = MonotonicMillis();
                bool trigger_diagnose = false;
                do {
                    auto state = delta_writer->get_state();
                    if (state == kCommitted || state == kAborted || state == kUninitialized) {
                        break;
                    }
                    i++;
                    // only sleep in bthread
                    bthread_usleep(10000); // 10ms
                    FAIL_POINT_TRIGGER_EXECUTE(tablets_channel_wait_secondary_replica_block, {
                        int32_t timeout_ms = config::load_fp_tablets_channel_wait_secondary_replica_block_ms;
                        if (timeout_ms > 0) {
                            bthread_usleep(timeout_ms * 1000);
                        }
                    });
                    if (!trigger_diagnose && (MonotonicMillis() - start_wait_time_ms >
                                              config::load_diagnose_rpc_timeout_stack_trace_threshold_ms)) {
                        _diagnose_primary_replica_stack_trace(tablet_id, request.id(), delta_writer.get());
                        trigger_diagnose = true;
                    }
                    auto elapse_time_ms = watch.elapsed_time() / 1000000;
                    if (elapse_time_ms > request.timeout_ms()) {
                        LOG(INFO) << "LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(request.id())
                                  << " wait tablet " << tablet_id << " secondary replica finish timeout "
                                  << request.timeout_ms() << "ms still in state " << state;
                        timeout = true;
                        break;
                    }

                    if (i % 6000 == 0) {
                        LOG(INFO) << "LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(request.id())
                                  << " wait tablet " << tablet_id << " secondary replica finish already "
                                  << elapse_time_ms << "ms still in state " << state;
                    }
                } while (true);
            }
            if (timeout) {
                break;
            }
        }
    }
    auto finish_wait_replica_ts = watch.elapsed_time();

    {
        std::lock_guard lock(_senders[request.sender_id()].lock);

        _senders[request.sender_id()].success_sliding_window.insert(request.packet_seq());
        while (_senders[request.sender_id()].success_sliding_window.size() > _max_sliding_window_size / 2) {
            auto last_success_iter = _senders[request.sender_id()].success_sliding_window.cbegin();
            auto last_receive_iter = _senders[request.sender_id()].receive_sliding_window.cbegin();
            if (_senders[request.sender_id()].last_sliding_packet_seq + 1 == *last_success_iter &&
                *last_success_iter == *last_receive_iter) {
                _senders[request.sender_id()].receive_sliding_window.erase(last_receive_iter);
                _senders[request.sender_id()].success_sliding_window.erase(last_success_iter);
                _senders[request.sender_id()].last_sliding_packet_seq++;
            }
        }
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

    if (close_channel) {
        // persist txn.
        std::vector<TabletSharedPtr> tablets;
        tablets.reserve(request.tablet_ids().size());
        for (const auto tablet_id : request.tablet_ids()) {
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
            if (tablet != nullptr) {
                tablets.emplace_back(std::move(tablet));
            }
        }
        auto persist_start = watch.elapsed_time();
        auto st = StorageEngine::instance()->txn_manager()->persist_tablet_related_txns(tablets);
        COUNTER_UPDATE(_wait_txn_persist_timer, watch.elapsed_time() - persist_start);
        LOG_IF(WARNING, !st.ok()) << "failed to persist transactions: " << st;
    } else if (request.wait_all_sender_close()) {
        _num_initial_senders.fetch_sub(1);
        std::string msg = fmt::format("LocalTabletsChannel txn_id: {} load_id: {}", _txn_id, print_id(request.id()));
        auto remain = request.timeout_ms();
        remain -= watch.elapsed_time() / 1000000;

        // unlock write lock so that incremental open can aquire read lock
        lk.unlock();
        // wait for all senders closed, may be timed out
        auto start_wait_drain_sender_ts = watch.elapsed_time();
        drain_senders(remain * 1000, msg);
        COUNTER_UPDATE(_wait_drain_sender_timer, watch.elapsed_time() - start_wait_drain_sender_ts);
    }

    int64_t last_execution_time_us = 0;
    if (response->has_execution_time_us()) {
        last_execution_time_us = response->execution_time_us();
    }
    response->set_execution_time_us(last_execution_time_us + watch.elapsed_time() / 1000);
    response->set_wait_lock_time_us(0); // We didn't measure the lock wait time, just give the caller a fake time
    response->set_wait_memtable_flush_time_us(wait_memtable_flush_time_us);

    // reset error message if it already set by other replica
    {
        std::lock_guard l(_status_lock);
        if (!_status.ok()) {
            response->mutable_status()->set_status_code(_status.code());
            response->mutable_status()->add_error_msgs(std::string(_status.message()));
        }
    }
    auto wait_writer_ns = finish_wait_writer_ts - start_wait_writer_ts;
    auto wait_replica_ns = finish_wait_replica_ts - finish_wait_writer_ts;
    StarRocksMetrics::instance()->load_channel_add_chunks_wait_memtable_duration_us.increment(
            wait_memtable_flush_time_us);
    StarRocksMetrics::instance()->load_channel_add_chunks_wait_writer_duration_us.increment(wait_writer_ns / 1000);
    StarRocksMetrics::instance()->load_channel_add_chunks_wait_replica_duration_us.increment(wait_replica_ns / 1000);
#ifndef BE_TEST
    _table_metrics->load_rows.increment(total_row_num);
    size_t chunk_size = chunk != nullptr ? chunk->bytes_usage() : 0;
    _table_metrics->load_bytes.increment(chunk_size);
#endif

    COUNTER_UPDATE(_add_chunk_counter, 1);
    COUNTER_UPDATE(_add_chunk_timer, watch.elapsed_time());
    COUNTER_UPDATE(_add_row_num, total_row_num);
    COUNTER_UPDATE(_wait_flush_timer, wait_memtable_flush_time_us * 1000);
    COUNTER_UPDATE(_submit_write_task_timer, finish_submit_write_task_ts - start_submit_write_task_ts);
    COUNTER_UPDATE(_submit_commit_task_timer, finish_submit_commit_task_ts - finish_submit_write_task_ts);
    COUNTER_UPDATE(_wait_write_timer, wait_writer_ns);
    COUNTER_UPDATE(_wait_replica_timer, wait_replica_ns);

    // remove tablets channel and load channel after all things done
    if (close_channel) {
        _load_channel->remove_tablets_channel(_key);
    }
}

void LocalTabletsChannel::_flush_stale_memtables() {
    if (_is_immutable_partition_empty() && config::stale_memtable_flush_time_sec <= 0) {
        return;
    }
    bool high_mem_usage = false;
    bool full_mem_usage = false;
    if (_mem_tracker->limit_exceeded_by_ratio(70) ||
        (_mem_tracker->parent() != nullptr && _mem_tracker->parent()->limit_exceeded_by_ratio(70))) {
        high_mem_usage = true;

        if (_mem_tracker->limit_exceeded_by_ratio(95) ||
            (_mem_tracker->parent() != nullptr && _mem_tracker->parent()->limit_exceeded_by_ratio(95))) {
            full_mem_usage = true;
        }
    }

    int64_t now = butil::gettimeofday_s();
    int64_t total_flush_bytes = 0;
    int64_t total_flush_writer = 0;
    int64_t total_active_writer = 0;
    for (auto& [tablet_id, writer] : _delta_writers) {
        bool need_flush = false;
        auto last_write_ts = writer->last_write_ts();
        if (last_write_ts > 0) {
            if (_has_immutable_partition(writer->partition_id())) {
                if (high_mem_usage) {
                    // immutable tablet flush stale memtable immediately when high mem usage
                    need_flush = true;
                } else if (now - last_write_ts > 1) {
                    // If the memtable of an immutable tablet is not updated within 1 second,
                    // it generally means that the sender has already marked it as immutable.
                    need_flush = true;
                }
            }
            if (config::stale_memtable_flush_time_sec > 0) {
                // when high mem usage, prior to flush stale memtable which is not updated for a long time
                if (high_mem_usage && now - last_write_ts > config::stale_memtable_flush_time_sec) {
                    need_flush = true;
                }
                // when full mem usage, flush all memtable which size is larger than 1/4 of write_buffer_size
                if (full_mem_usage && writer->write_buffer_size() > config::write_buffer_size / 4) {
                    need_flush = true;
                }
            }
            // has write means active writer
            ++total_active_writer;
            if (need_flush) {
                VLOG(2) << "Flush stale memtable tablet_id: " << tablet_id << " txn_id: " << _txn_id
                        << " partition_id: " << writer->partition_id() << " is_immutable: " << writer->is_immutable()
                        << " write_buffer_size: " << writer->write_buffer_size()
                        << " stale_time: " << now - last_write_ts << " job_mem_usage: " << _mem_tracker->consumption()
                        << " job_mem_limit: " << _mem_tracker->limit()
                        << " load_mem_usage: " << _mem_tracker->parent()->consumption()
                        << " load_mem_limit: " << _mem_tracker->parent()->limit();
                total_flush_bytes += writer->write_buffer_size();
                ++total_flush_writer;
                writer->flush();
            }
        }
    }

    if (total_flush_bytes > 0 || total_flush_writer > 0) {
        LOG(INFO) << "Flush stale memtable txn_id: " << _txn_id << " total_flush_bytes: " << total_flush_bytes
                  << " total_flush_writer: " << total_flush_writer << " total_active_writer: " << total_active_writer
                  << " total_writer: " << _delta_writers.size() << " job_mem_usage: " << _mem_tracker->consumption()
                  << " load_mem_usage: " << _mem_tracker->parent()->consumption();
    }
}

void LocalTabletsChannel::_abort_replica_tablets(
        const PTabletWriterAddChunkRequest& request, const std::string& abort_reason,
        const std::unordered_map<int64_t, std::vector<int64_t>>& node_id_to_abort_tablets) {
    for (auto& [node_id, tablet_ids] : node_id_to_abort_tablets) {
        auto& endpoint = _node_id_to_endpoint[node_id];
        auto stub = ExecEnv::GetInstance()->brpc_stub_cache()->get_stub(endpoint.host(), endpoint.port());
        if (stub == nullptr) {
            auto msg =
                    fmt::format("Failed to Connect node {} {}:{} failed.", node_id, endpoint.host(), endpoint.port());
            LOG(WARNING) << msg;
            continue;
        }

        PTabletWriterCancelRequest cancel_request;
        *cancel_request.mutable_id() = request.id();
        cancel_request.set_sender_id(0);
        cancel_request.mutable_tablet_ids()->CopyFrom({tablet_ids.begin(), tablet_ids.end()});
        cancel_request.set_txn_id(_txn_id);
        cancel_request.set_index_id(_index_id);
        cancel_request.set_reason(abort_reason);
        cancel_request.set_sink_id(request.sink_id());

        auto closure = new ReusableClosure<PTabletWriterCancelResult>();

        closure->ref();
        closure->cntl.set_timeout_ms(request.timeout_ms());

        string node_abort_tablet_id_list_str;
        JoinInts(tablet_ids, ",", &node_abort_tablet_id_list_str);

        stub->tablet_writer_cancel(&closure->cntl, &cancel_request, &closure->result, closure);

        VLOG(2) << "LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(request.id()) << " Cancel "
                << tablet_ids.size() << " tablets " << node_abort_tablet_id_list_str << " request to "
                << endpoint.host() << ":" << endpoint.port();
    }
}

void LocalTabletsChannel::_commit_tablets(const PTabletWriterAddChunkRequest& request,
                                          const std::shared_ptr<LocalTabletsChannel::WriteContext>& context) {
    vector<int64_t> commit_tablet_ids;
    std::unordered_map<int64_t, std::vector<int64_t>> node_id_to_abort_tablets;
    {
        std::lock_guard l1(_partitions_ids_lock);
        for (auto& [tablet_id, delta_writer] : _delta_writers) {
            // Secondary replica will commit/abort by Primary replica
            if (delta_writer->replica_state() != Secondary) {
                if (UNLIKELY(_partition_ids.count(delta_writer->partition_id()) == 0)) {
                    // no data load, abort txn without printing log
                    delta_writer->abort(false);

                    // secondary replicas need abort by primary replica
                    if (_is_replicated_storage) {
                        auto& replicas = delta_writer->replicas();
                        for (int i = 1; i < replicas.size(); ++i) {
                            node_id_to_abort_tablets[replicas[i].node_id()].emplace_back(tablet_id);
                        }
                    }
                } else {
                    auto cb = new WriteCallback(context);
                    delta_writer->commit(cb);
                    commit_tablet_ids.emplace_back(tablet_id);
                }
            }
        }
    }
    string commit_tablet_id_list_str;
    JoinInts(commit_tablet_ids, ",", &commit_tablet_id_list_str);
    LOG(INFO) << "LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(request.id())
              << " sink_id: " << request.sink_id() << " commit " << commit_tablet_ids.size()
              << " tablets: " << commit_tablet_id_list_str;

    // abort seconary replicas located on other nodes which have no data
    _abort_replica_tablets(request, "", node_id_to_abort_tablets);
}

int LocalTabletsChannel::_close_sender(const int64_t* partitions, size_t partitions_size) {
    std::lock_guard l(_partitions_ids_lock);
    for (int i = 0; i < partitions_size; i++) {
        _partition_ids.insert(partitions[i]);
    }
    // when replicated storage is true, the partitions of each sender will be different
    // So we need to make sure that all partitions are added to _partition_ids when committing
    int n = _num_remaining_senders.fetch_sub(1);
    DCHECK_GE(n, 1);

    // if sender close means data send finished, we need to decrease _num_initial_senders
    _num_initial_senders.fetch_sub(1);

    VLOG(2) << "LocalTabletsChannel txn_id: " << _txn_id << " close " << partitions_size << " partitions remaining "
            << n - 1 << " senders";
    return n - 1;
}

Status LocalTabletsChannel::_open_all_writers(const PTabletWriterOpenRequest& params) {
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

    _is_replicated_storage = params.is_replicated_storage();
    std::vector<int64_t> tablet_ids;
    tablet_ids.reserve(params.tablets_size());
    std::vector<int64_t> failed_tablet_ids;
    for (const PTabletWithPartition& tablet : params.tablets()) {
        DeltaWriterOptions options;
        options.tablet_id = tablet.tablet_id();
        options.schema_hash = schema_hash;
        options.txn_id = _txn_id;
        options.partition_id = tablet.partition_id();
        options.load_id = params.id();
        options.slots = index_slots;
        options.global_dicts = &_global_dicts;
        options.parent_span = _load_channel->get_span();
        options.index_id = _index_id;
        options.node_id = _node_id;
        options.sink_id = params.sink_id();
        options.timeout_ms = params.timeout_ms();
        options.write_quorum = params.write_quorum();
        options.miss_auto_increment_column = params.miss_auto_increment_column();
        options.ptable_schema_param = &(params.schema());
        options.column_to_expr_value = &(_column_to_expr_value);
        if (params.is_replicated_storage()) {
            for (auto& replica : tablet.replicas()) {
                options.replicas.emplace_back(replica);
                if (_node_id_to_endpoint.count(replica.node_id()) == 0) {
                    _node_id_to_endpoint[replica.node_id()] = replica;
                }
            }
            if (options.replicas.size() > 0 && options.replicas[0].node_id() == options.node_id) {
                options.replica_state = Primary;
            } else {
                options.replica_state = Secondary;
            }
        } else {
            options.replica_state = Peer;
        }
        options.merge_condition = params.merge_condition();
        options.partial_update_mode = params.partial_update_mode();
        options.immutable_tablet_size = params.immutable_tablet_size();

        auto res = AsyncDeltaWriter::open(options, _mem_tracker);
        if (res.status().ok()) {
            auto writer = std::move(res).value();
            _delta_writers.emplace(tablet.tablet_id(), std::move(writer));
            tablet_ids.emplace_back(tablet.tablet_id());
        } else {
            if (options.replica_state == Secondary) {
                failed_tablet_ids.emplace_back(tablet.tablet_id());
            } else {
                return res.status();
            }
        }
    }
    _s_tablet_writer_count += _delta_writers.size();
    // In order to get sorted index for each tablet
    std::sort(tablet_ids.begin(), tablet_ids.end());
    for (size_t i = 0; i < tablet_ids.size(); ++i) {
        _tablet_id_to_sorted_indexes.emplace(tablet_ids[i], i);
    }
    if (_is_replicated_storage) {
        std::stringstream ss;
        ss << "LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(params.id())
           << " sink_id: " << params.sink_id() << " open " << _delta_writers.size() << " delta writer: ";
        int i = 0;
        for (auto& [tablet_id, delta_writer] : _delta_writers) {
            ss << "[" << tablet_id << ":" << delta_writer->replica_state() << "]";
            if (++i > 128) {
                break;
            }
        }
        ss << failed_tablet_ids.size() << " failed_tablets: ";
        for (auto& tablet_id : failed_tablet_ids) {
            ss << tablet_id << ",";
        }
        if (_is_incremental_channel) {
            ss << " on incremental channel";
        }
        ss << " _num_remaining_senders: " << _num_remaining_senders;
        LOG(INFO) << ss.str();
    }
    return Status::OK();
}

void LocalTabletsChannel::cancel() {
    std::shared_lock<bthreads::BThreadSharedMutex> lk(_rw_mtx);
    for (auto& it : _delta_writers) {
        it.second->cancel(Status::Cancelled("cancel"));
    }
}

void LocalTabletsChannel::abort() {
    std::shared_lock<bthreads::BThreadSharedMutex> lk(_rw_mtx);
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

void LocalTabletsChannel::abort(const std::vector<int64_t>& tablet_ids, const std::string& reason) {
    std::shared_lock<bthreads::BThreadSharedMutex> lk(_rw_mtx);
    bool abort_with_exception = !reason.empty();
    for (auto tablet_id : tablet_ids) {
        auto it = _delta_writers.find(tablet_id);
        if (it != _delta_writers.end()) {
            it->second->cancel(Status::Cancelled(reason));
            it->second->abort(abort_with_exception);
        } else {
            LOG(WARNING) << "tablet_id: " << tablet_id << " not found in LocalTabletsChannel txn_id: " << _txn_id
                         << " load_id: " << _key.id << " index_id: " << _key.index_id;
        }
    }
    string tablet_id_list_str;
    JoinInts(tablet_ids, ",", &tablet_id_list_str);
    LOG(INFO) << "cancel LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << _key.id
              << " index_id: " << _key.index_id << " tablet_ids:" << tablet_id_list_str;

    if (abort_with_exception) {
        std::lock_guard l(_status_lock);
        _status = Status::Aborted(reason);
    }
}

StatusOr<std::shared_ptr<LocalTabletsChannel::WriteContext>> LocalTabletsChannel::_create_write_context(
        Chunk* chunk, const PTabletWriterAddChunkRequest& request, PTabletWriterAddBatchResult* response) {
    if (chunk == nullptr && !request.eos() && !request.wait_all_sender_close()) {
        return Status::InvalidArgument("PTabletWriterAddChunkRequest has no chunk or eos");
    }

    auto context = std::make_shared<WriteContext>(response);

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

Status LocalTabletsChannel::incremental_open(const PTabletWriterOpenRequest& params, PTabletWriterOpenResult* result,
                                             std::shared_ptr<OlapTableSchemaParam> schema) {
    std::unique_lock<bthreads::BThreadSharedMutex> lk(_rw_mtx);
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
    // update tablets
    std::vector<int64_t> tablet_ids;
    tablet_ids.reserve(params.tablets_size());
    size_t incremental_tablet_num = 0;
    std::stringstream ss;
    ss << "LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(params.id())
       << " sink_id: " << params.sink_id() << " incremental open delta writer: ";

    for (const PTabletWithPartition& tablet : params.tablets()) {
        if (_delta_writers.count(tablet.tablet_id()) != 0) {
            continue;
        }
        incremental_tablet_num++;

        DeltaWriterOptions options;
        options.tablet_id = tablet.tablet_id();
        options.schema_hash = schema_hash;
        options.txn_id = _txn_id;
        options.partition_id = tablet.partition_id();
        options.load_id = params.id();
        options.slots = index_slots;
        options.global_dicts = &_global_dicts;
        options.parent_span = _load_channel->get_span();
        options.index_id = _index_id;
        options.node_id = _node_id;
        options.sink_id = params.sink_id();
        options.timeout_ms = params.timeout_ms();
        options.write_quorum = params.write_quorum();
        options.miss_auto_increment_column = params.miss_auto_increment_column();
        options.ptable_schema_param = &(params.schema());
        options.immutable_tablet_size = params.immutable_tablet_size();
        options.column_to_expr_value = &(_column_to_expr_value);
        if (params.is_replicated_storage()) {
            for (auto& replica : tablet.replicas()) {
                options.replicas.emplace_back(replica);
                if (_node_id_to_endpoint.count(replica.node_id()) == 0) {
                    _node_id_to_endpoint[replica.node_id()] = replica;
                }
            }
            if (options.replicas.size() > 0 && options.replicas[0].node_id() == options.node_id) {
                options.replica_state = Primary;
            } else {
                options.replica_state = Secondary;
            }
        } else {
            options.replica_state = Peer;
        }

        auto res = AsyncDeltaWriter::open(options, _mem_tracker);
        RETURN_IF_ERROR(res.status());
        auto writer = std::move(res).value();
        ss << "[" << tablet.tablet_id() << ":" << writer->replica_state() << "]";
        _delta_writers.emplace(tablet.tablet_id(), std::move(writer));
        tablet_ids.emplace_back(tablet.tablet_id());
    }

    if (incremental_tablet_num > 0) {
        _s_tablet_writer_count += incremental_tablet_num;

        auto it = _tablet_id_to_sorted_indexes.begin();
        while (it != _tablet_id_to_sorted_indexes.end()) {
            tablet_ids.emplace_back(it->first);
            it++;
        }
        DCHECK_EQ(_delta_writers.size(), tablet_ids.size());
        std::sort(tablet_ids.begin(), tablet_ids.end());
        for (size_t i = 0; i < tablet_ids.size(); ++i) {
            _tablet_id_to_sorted_indexes[tablet_ids[i]] = i;
        }
    }

    if (_is_incremental_channel && !_senders[params.sender_id()].has_incremental_open) {
        _num_remaining_senders.fetch_add(1, std::memory_order_release);
        _senders[params.sender_id()].has_incremental_open = true;
        ss << " on incremental channel _num_remaining_senders: " << _num_remaining_senders;
        ++incremental_tablet_num;
    }

    // no update no need to log
    if (incremental_tablet_num > 0) {
        LOG(INFO) << ss.str();
    }

    return Status::OK();
}

void LocalTabletsChannel::WriteCallback::run(const Status& st, const CommittedRowsetInfo* committed_info,
                                             const FailedRowsetInfo* failed_info) {
    _context->update_status(st);
    if (failed_info != nullptr) {
        PTabletInfo tablet_info;
        tablet_info.set_tablet_id(failed_info->tablet_id);
        // unused but it's required field of protobuf
        tablet_info.set_schema_hash(0);
        _context->add_failed_tablet_info(&tablet_info);

        // failed tablets from seconary replica
        if (failed_info->replicate_token) {
            const auto failed_tablet_infos = failed_info->replicate_token->failed_tablet_infos();
            for (const auto& failed_tablet_info : *failed_tablet_infos) {
                _context->add_failed_tablet_info(failed_tablet_info.get());
            }

            // primary replica already fail, we need cancel all secondary replica whether it's failed or not
            auto failed_replica_node_ids = failed_info->replicate_token->replica_node_ids();
            for (auto& node_id : failed_replica_node_ids) {
                _context->add_failed_replica_node_id(node_id, failed_info->tablet_id);
            }
        }
    }
    if (committed_info != nullptr) {
        // committed tablets from primary replica
        // TODO: dup code with SegmentFlushToken::submit
        PTabletInfo tablet_info;
        tablet_info.set_tablet_id(committed_info->tablet->tablet_id());
        tablet_info.set_schema_hash(committed_info->tablet->schema_hash());
        const auto& rowset_global_dict_columns_valid_info =
                committed_info->rowset_writer->global_dict_columns_valid_info();
        const auto* rowset_global_dicts = committed_info->rowset_writer->rowset_global_dicts();
        for (const auto& item : rowset_global_dict_columns_valid_info) {
            if (item.second && rowset_global_dicts != nullptr &&
                rowset_global_dicts->find(item.first) != rowset_global_dicts->end()) {
                tablet_info.add_valid_dict_cache_columns(item.first);
                tablet_info.add_valid_dict_collected_version(rowset_global_dicts->at(item.first).version);
            } else {
                tablet_info.add_invalid_dict_cache_columns(item.first);
            }
        }
        _context->add_committed_tablet_info(&tablet_info);

        // committed tablets from seconary replica
        if (committed_info->replicate_token) {
            const auto replicated_tablet_infos = committed_info->replicate_token->replicated_tablet_infos();
            for (const auto& synced_tablet_info : *replicated_tablet_infos) {
                _context->add_committed_tablet_info(synced_tablet_info.get());
            }
        }
    }
    delete this;
}

void LocalTabletsChannel::update_profile() {
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

    std::vector<AsyncDeltaWriter*> async_writers;
    {
        std::shared_lock<bthreads::BThreadSharedMutex> lk(_rw_mtx);
        async_writers.reserve(_delta_writers.size());
        for (auto& [tablet_id, delta_writer] : _delta_writers) {
            async_writers.push_back(delta_writer.get());
        }
    }

    bool replicated_storage = true;
    std::vector<RuntimeProfile*> peer_or_primary_replica_profiles;
    std::vector<RuntimeProfile*> secondary_replica_profiles;
    for (auto async_writer : async_writers) {
        DeltaWriter* writer = async_writer->writer();
        RuntimeProfile* profile = _tablets_profile->create_child(fmt::format("{}", writer->tablet()->tablet_id()));
        auto replica_state = writer->replica_state();
        if (replica_state == Peer) {
            _update_peer_replica_profile(writer, profile);
            peer_or_primary_replica_profiles.push_back(profile);
            replicated_storage = false;
        } else if (replica_state == Primary) {
            _update_primary_replica_profile(writer, profile);
            peer_or_primary_replica_profiles.push_back(profile);
        } else if (writer->replica_state() == Secondary) {
            _update_secondary_replica_profile(writer, profile);
            secondary_replica_profiles.push_back(profile);
        } else {
            // ignore unknown replica state
        }
    }

    ObjectPool obj_pool;
    if (!peer_or_primary_replica_profiles.empty()) {
        auto* merged_profile = RuntimeProfile::merge_isomorphic_profiles(&obj_pool, peer_or_primary_replica_profiles);
        RuntimeProfile* final_profile = _profile->create_child(replicated_storage ? "PrimaryReplicas" : "PeerReplicas");
        auto* tablets_counter = ADD_COUNTER(final_profile, "TabletsNum", TUnit::UNIT);
        COUNTER_UPDATE(tablets_counter, peer_or_primary_replica_profiles.size());
        final_profile->copy_all_info_strings_from(merged_profile);
        final_profile->copy_all_counters_from(merged_profile);
    }

    if (!secondary_replica_profiles.empty()) {
        auto* merged_profile = RuntimeProfile::merge_isomorphic_profiles(&obj_pool, secondary_replica_profiles);
        RuntimeProfile* final_profile = _profile->create_child("SecondaryReplicas");
        auto* tablets_counter = ADD_COUNTER(final_profile, "TabletsNum", TUnit::UNIT);
        COUNTER_UPDATE(tablets_counter, peer_or_primary_replica_profiles.size());
        final_profile->copy_all_info_strings_from(merged_profile);
        final_profile->copy_all_counters_from(merged_profile);
    }
}

#define ADD_AND_UPDATE_COUNTER(profile, name, type, val) (ADD_COUNTER(profile, name, type))->update(val)
#define ADD_AND_UPDATE_TIMER(profile, name, val) (ADD_TIMER(profile, name))->update(val)

void LocalTabletsChannel::_update_peer_replica_profile(DeltaWriter* writer, RuntimeProfile* profile) {
    const DeltaWriterStat& writer_stat = writer->get_writer_stat();
    ADD_AND_UPDATE_COUNTER(profile, "WriterTaskCount", TUnit::UNIT, writer_stat.task_count);
    ADD_AND_UPDATE_TIMER(profile, "WriterTaskPendingTime", writer_stat.pending_time_ns);
    ADD_AND_UPDATE_COUNTER(profile, "WriteCount", TUnit::UNIT, writer_stat.write_count);
    ADD_AND_UPDATE_COUNTER(profile, "RowCount", TUnit::UNIT, writer_stat.row_count);
    ADD_AND_UPDATE_TIMER(profile, "WriteTime", writer_stat.write_time_ns);
    ADD_AND_UPDATE_COUNTER(profile, "MemtableFullCount", TUnit::UNIT, writer_stat.memtable_full_count);
    ADD_AND_UPDATE_COUNTER(profile, "MemoryExceedCount", TUnit::UNIT, writer_stat.memory_exceed_count);
    ADD_AND_UPDATE_TIMER(profile, "WriteWaitFlushTime", writer_stat.write_wait_flush_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "CloseTime", writer_stat.close_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "CommitTime", writer_stat.commit_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "CommitWaitFlushTime", writer_stat.commit_wait_flush_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "CommitRowsetBuildTime", writer_stat.commit_rowset_build_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "CommitPkPreloadTime", writer_stat.commit_pk_preload_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "CommitWaitReplicaTime", writer_stat.commit_wait_replica_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "CommitTxnCommitTime", writer_stat.commit_txn_commit_time_ns);

    const FlushStatistic& flush_stat = writer->get_flush_stats();
    ADD_AND_UPDATE_COUNTER(profile, "MemtableFlushedCount", TUnit::UNIT, flush_stat.flush_count);
    ADD_AND_UPDATE_COUNTER(profile, "MemtableFlushingCount", TUnit::UNIT, flush_stat.cur_flush_count);
    ADD_AND_UPDATE_COUNTER(profile, "MemtableQueueCount", TUnit::UNIT, flush_stat.queueing_memtable_num);
    ADD_AND_UPDATE_TIMER(profile, "FlushTaskPendingTime", flush_stat.pending_time_ns);
    auto& memtable_stat = flush_stat.memtable_stats;
    ADD_AND_UPDATE_COUNTER(profile, "MemtableInsertCount", TUnit::UNIT, memtable_stat.insert_count);
    ADD_AND_UPDATE_TIMER(profile, "MemtableInsertTime", memtable_stat.insert_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "MemtableFinalizeTime", memtable_stat.finalize_time_ns);
    ADD_AND_UPDATE_COUNTER(profile, "MemtableSortCount", TUnit::UNIT, memtable_stat.sort_count);
    ADD_AND_UPDATE_TIMER(profile, "MemtableSortTime", memtable_stat.sort_time_ns);
    ADD_AND_UPDATE_COUNTER(profile, "MemtableAggCount", TUnit::UNIT, memtable_stat.agg_count);
    ADD_AND_UPDATE_TIMER(profile, "MemtableAggTime", memtable_stat.agg_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "MemtableFlushTime", memtable_stat.flush_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "MemtableIOTime", memtable_stat.io_time_ns);
    ADD_AND_UPDATE_COUNTER(profile, "MemtableMemorySize", TUnit::BYTES, memtable_stat.flush_memory_size);
    ADD_AND_UPDATE_COUNTER(profile, "MemtableDiskSize", TUnit::BYTES, memtable_stat.flush_disk_size);
}

void LocalTabletsChannel::_update_primary_replica_profile(DeltaWriter* writer, RuntimeProfile* profile) {
    _update_peer_replica_profile(writer, profile);
    auto* replicate_token = writer->replicate_token();
    if (replicate_token == nullptr) {
        return;
    }
    auto& replicate_stat = replicate_token->get_stat();
    ADD_AND_UPDATE_COUNTER(profile, "ReplicatePendingTaskCount", TUnit::UNIT, replicate_stat.num_pending_tasks);
    ADD_AND_UPDATE_COUNTER(profile, "ReplicateExecutingTaskCount", TUnit::UNIT, replicate_stat.num_running_tasks);
    ADD_AND_UPDATE_COUNTER(profile, "ReplicateFinishedTaskCount", TUnit::UNIT, replicate_stat.num_finished_tasks);
    ADD_AND_UPDATE_TIMER(profile, "ReplicateTaskPendingTime", replicate_stat.pending_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "ReplicateTaskExecuteTime", replicate_stat.execute_time_ns);
}

void LocalTabletsChannel::_update_secondary_replica_profile(DeltaWriter* writer, RuntimeProfile* profile) {
    const DeltaWriterStat& writer_stat = writer->get_writer_stat();
    ADD_AND_UPDATE_COUNTER(profile, "RowCount", TUnit::UNIT, writer_stat.row_count);
    ADD_AND_UPDATE_COUNTER(profile, "DataSize", TUnit::BYTES, writer_stat.add_segment_data_size);
    ADD_AND_UPDATE_COUNTER(profile, "AddSegmentCount", TUnit::UNIT, writer_stat.add_segment_count);
    ADD_AND_UPDATE_TIMER(profile, "AddSegmentTime", writer_stat.add_segment_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "AddSegmentIOTime", writer_stat.add_segment_io_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "CommitTime", writer_stat.commit_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "CommitRowsetBuildTime", writer_stat.commit_rowset_build_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "CommitPkPreloadTime", writer_stat.commit_pk_preload_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "CommitTxnCommitTime", writer_stat.commit_txn_commit_time_ns);

    auto* segment_flush_token = writer->segment_flush_token();
    if (segment_flush_token == nullptr) {
        return;
    }
    auto& stat = segment_flush_token->get_stat();
    ADD_AND_UPDATE_COUNTER(profile, "FlushPendingTaskCount", TUnit::UNIT, stat.num_pending_tasks);
    ADD_AND_UPDATE_COUNTER(profile, "FlushExecutingTaskCount", TUnit::UNIT, stat.num_running_tasks);
    ADD_AND_UPDATE_COUNTER(profile, "FlushFinishedTaskCount", TUnit::UNIT, stat.num_finished_tasks);
    ADD_AND_UPDATE_TIMER(profile, "FlushTaskPendingTime", stat.pending_time_ns);
    ADD_AND_UPDATE_TIMER(profile, "FlushTaskExecuteTime", stat.execute_time_ns);
}

void LocalTabletsChannel::_diagnose_primary_replica_stack_trace(int64_t tablet_id, const PUniqueId& load_id,
                                                                AsyncDeltaWriter* async_delta_writer) {
    auto delta_writer = async_delta_writer->writer();
    if (delta_writer->replica_state() != ReplicaState::Secondary || delta_writer->replicas().empty()) {
        return;
    }
    auto& primary_replica = delta_writer->replicas()[0];
    auto stub = ExecEnv::GetInstance()->brpc_stub_cache()->get_stub(primary_replica.host(), primary_replica.port());
    if (stub == nullptr) {
        LOG(WARNING) << "failed to diagnose primary replica, txn_id: " << _txn_id << ", load_id: " << print_id(load_id)
                     << ", tablet_id: " << tablet_id << ", primary_replica: [" << primary_replica.host() << ":"
                     << primary_replica.port() << "]";
        return;
    }
    auto closure = new ReusableClosure<PLoadDiagnoseResult>();
    closure->cntl.set_timeout_ms(config::load_diagnose_send_rpc_timeout_ms);
    SET_IGNORE_OVERCROWDED(closure->cntl, load);
    PLoadDiagnoseRequest request;
    request.mutable_id()->set_hi(load_id.hi());
    request.mutable_id()->set_hi(load_id.lo());
    request.set_txn_id(_txn_id);
    request.set_stack_trace(true);
    closure->ref();
#ifndef BE_TEST
    // best effort to diagnose so do not wait the result
    stub->load_diagnose(&closure->cntl, &request, &closure->result, closure);
#else
    std::pair<PLoadDiagnoseRequest*, ReusableClosure<PLoadDiagnoseResult>*> rpc_pair{&request, closure};
    TEST_SYNC_POINT_CALLBACK("LocalTabletsChannel::rpc::load_diagnose_send", &rpc_pair);
#endif
    LOG(INFO) << "send request to diagnose primary replica, txn_id: " << _txn_id << ", load_id: " << print_id(load_id)
              << ", tablet_id: " << tablet_id << ", primary_replica: [" << primary_replica.host() << ":"
              << primary_replica.port() << "]";
}

std::shared_ptr<TabletsChannel> new_local_tablets_channel(LoadChannel* load_channel, const TabletsChannelKey& key,
                                                          MemTracker* mem_tracker, RuntimeProfile* parent_profile) {
    return std::make_shared<LocalTabletsChannel>(load_channel, key, mem_tracker, parent_profile);
}

} // namespace starrocks
