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

#include "storage/lake/compaction_scheduler.h"

#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <butil/time.h> // NOLINT

#include <chrono>
#include <thread>

#include "agent/master_info.h"
#include "base/testutil/sync_point.h"
#include "common/status.h"
#include "common/thread/threadpool.h"
#include "common/util/misc.h"
#include "fs/fs.h"
#include "fs/key_cache.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gutil/stl_util.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "service/service_be/lake_service.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_parallel_compaction_manager.h"
#include "storage/memtable_flush_executor.h"
#include "storage/storage_engine.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks::lake {

namespace {
static void reject_request(::google::protobuf::RpcController* controller, const CompactRequest* request,
                           CompactResponse* response) {
    auto st = Status::Aborted("Compaction request rejected due to BE/CN shutdown in progress!");
    LOG(WARNING) << "Fail to compact num_of_tablets= " << request->tablet_ids().size()
                 << ". version=" << request->version() << " txn_id=" << request->txn_id() << " : " << st;
    st.to_protobuf(response->mutable_status());
}
} // namespace

CompactionTaskCallback::~CompactionTaskCallback() = default;

CompactionTaskCallback::CompactionTaskCallback(CompactionScheduler* scheduler, const CompactRequest* request,
                                               CompactResponse* response, ::google::protobuf::Closure* done)
        : _scheduler(scheduler),
          _mtx(),
          _request(request),
          _response(response),
          _done(done),
          _last_check_time(INT64_MAX) {
    CHECK(_request != nullptr);
    CHECK(_response != nullptr);
    _timeout_deadline_ms = butil::gettimeofday_ms() + timeout_ms();
    _contexts.reserve(request->tablet_ids_size());
}

int64_t CompactionTaskCallback::timeout_ms() const {
    return _request->has_timeout_ms() ? _request->timeout_ms() : kDefaultTimeoutMs;
}

bool CompactionTaskCallback::allow_partial_success() const {
    if (_request->has_allow_partial_success() && _request->allow_partial_success()) {
        return true;
    } else {
        return false;
    }
}

Status CompactionTaskCallback::has_error() const {
    std::lock_guard l(_mtx);
    if (_status.ok()) {
        if (butil::gettimeofday_ms() >= _timeout_deadline_ms) {
            return Status::Aborted(fmt::format("timeout exceeded after {}ms", timeout_ms()));
        } else {
            return Status::OK();
        }
    }
    if (allow_partial_success()) {
        if (_status.is_aborted()) {
            // manual cancel
            // FE validation failed
            // background worker shutdown
            return _status;
        } else {
            return Status::OK();
        }
    } else {
        return _status;
    }
}

void CompactionTaskCallback::finish_task(std::unique_ptr<CompactionTaskContext>&& context) {
    std::unique_lock l(_mtx);

    if (!context->status.ok()) {
        _response->add_failed_tablets(context->tablet_id);
    } else {
        _success_compaction_input_file_size += context->stats->input_file_size;
    }

    // process compact stat
    auto compact_stat = _response->add_compact_stats();
    compact_stat->set_tablet_id(context->tablet_id);
    compact_stat->set_read_time_remote(context->stats->io_ns_read_remote);
    compact_stat->set_read_bytes_remote(context->stats->io_bytes_read_remote);
    compact_stat->set_read_time_local(context->stats->io_ns_read_local_disk);
    compact_stat->set_read_bytes_local(context->stats->io_bytes_read_local_disk);
    compact_stat->set_read_segment_count(context->stats->read_segment_count);
    compact_stat->set_write_segment_count(context->stats->write_segment_count);
    compact_stat->set_write_segment_bytes(context->stats->write_segment_bytes);
    compact_stat->set_write_time_remote(context->stats->io_ns_write_remote);
    compact_stat->set_in_queue_time_sec(context->stats->in_queue_time_sec);
    compact_stat->set_sub_task_count(context->subtask_count);
    compact_stat->set_total_compact_input_file_size(context->stats->input_file_size);
    if (context->skip_write_txnlog && context->txn_log != nullptr) {
        // context->txn_log could be nullptr if the task is failed before writing txn log.
        _response->add_txn_logs()->CopyFrom(*context->txn_log);
    }
    DCHECK(_request != nullptr);
    _status.update(context->status);

    // For parallel merged context, register it to the scheduler's _contexts list
    // so that it's visible in list_tasks() until RPC response is sent.
    if (context->is_parallel_merged && _scheduler != nullptr) {
        std::lock_guard ctx_lock(_scheduler->_contexts_lock);
        _scheduler->_contexts.Append(context.get());
    }

    // Keep the context for a while until the RPC request is finished processing so that we can see the detailed
    // and complete progress of the RPC request by calling `CompactionScheduler::list_tasks()`.
    _contexts.emplace_back(std::move(context));
    //                     ^^^^^^^^^^^^^^^^^ Do NOT touch "context" since here, it has been `move`ed.

    if (_contexts.size() == _request->tablet_ids_size()) { // All tasks finished, send RPC response to FE
        _status.to_protobuf(_response->mutable_status());
        _response->set_success_compaction_input_file_size(_success_compaction_input_file_size);
        if (_done != nullptr) {
            _done->Run();
            _done = nullptr;
        }
        _request = nullptr;
        _response = nullptr;

        std::vector<std::unique_ptr<CompactionTaskContext>> tmp;
        tmp.swap(_contexts);

        l.unlock();
        if (_scheduler != nullptr) {
            _scheduler->remove_states(tmp);
        }
        tmp.clear();
        TEST_SYNC_POINT("lake::CompactionTaskCallback::finish_task:finish_task");
    }
}

Status CompactionTaskCallback::is_txn_still_valid() {
    RETURN_IF_ERROR(has_error());
    auto check_interval_seconds = 60L * config::lake_compaction_check_valid_interval_minutes;
    if (check_interval_seconds <= 0) {
        return Status::OK();
    }
    // try_lock failed means other thread is checking txn
    if (!_txn_valid_check_mutex.try_lock()) {
        return Status::OK();
    }
    DeferOp defer([&]() { _txn_valid_check_mutex.unlock(); });
    // check again after acquired lock
    auto now = time(nullptr);
    if (now <= _last_check_time || (now - _last_check_time) < check_interval_seconds) {
        return Status::OK();
    }
    // ask FE whether this compaction transaction is still valid
#ifndef BE_TEST
    TNetworkAddress master_addr = get_master_address();
    if (master_addr.hostname.size() > 0 && master_addr.port > 0) {
        TReportLakeCompactionRequest request;
        request.__set_txn_id(_request->txn_id());
        TReportLakeCompactionResponse result;
        auto status = ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr.hostname, master_addr.port,
                [&request, &result](FrontendServiceConnection& client) {
                    client->reportLakeCompaction(result, request);
                },
                3000 /* timeout 3 seconds */);
        if (status.ok()) {
            if (!result.valid) {
                // notify all tablets in this compaction request
                LOG(WARNING) << "abort invalid compaction transaction " << _request->txn_id();
                Status rs = Status::Aborted("compaction validation failed");
                update_status(rs);
                return rs; // should cancel compaction
            } else {
                // everything is fine
            }
        } else {
            LOG(WARNING) << "fail to validate compaction transaction " << _request->txn_id() << ", error: " << status;
        }
    } else {
        LOG(WARNING) << "fail to validate compaction transaction " << _request->txn_id()
                     << ", error: leader FE address not found";
    }
#endif
    _last_check_time = time(nullptr);
    return Status::OK();
}

CompactionScheduler::CompactionScheduler(TabletManager* tablet_mgr)
        : _tablet_mgr(tablet_mgr),
          _limiter(config::compact_threads),
          _contexts_lock(),
          _contexts(),
          _task_queues(config::compact_threads) {
    CHECK_GT(_task_queues.task_queue_size(), 0);
    auto st = ThreadPoolBuilder("cloud_native_compact")
                      .set_min_threads(0)
                      .set_max_threads(INT_MAX)
                      .set_max_queue_size(INT_MAX)
                      .build(&_threads);
    CHECK(st.ok()) << st;

    for (int i = 0; i < _task_queues.task_queue_size(); i++) {
        CHECK(_threads->submit_func([this, id = i]() { this->thread_task(id); }).ok());
    }

    // Initialize per-tablet parallel compaction manager
    _parallel_mgr = std::make_unique<TabletParallelCompactionManager>(tablet_mgr);
}

CompactionScheduler::~CompactionScheduler() {
    stop();
}

void CompactionScheduler::stop() {
    bool expected = false;
    auto changed = false;
    {
        // hold the lock to exclude new tasks entering the task queue in compact() interface
        std::unique_lock lock(_mutex);
        changed = _stopped.compare_exchange_strong(expected, true);
    }
    if (changed) {
        _threads->shutdown();
        abort_all();
    }
}

void CompactionScheduler::compact(::google::protobuf::RpcController* controller, const CompactRequest* request,
                                  CompactResponse* response, ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    // when FE request a compaction, CN may not have any key cached yet, so pass an encryption_meta to refresh cache
    if (!request->encryption_meta().empty()) {
        Status st = KeyCache::instance().refresh_keys(request->encryption_meta());
        if (!st.ok()) {
            LOG(WARNING) << fmt::format("refresh keys using encryption_meta in PTabletWriterOpenRequest failed {}",
                                        st.detailed_message());
        }
    }

    // Check if parallel compaction is enabled
    bool has_parallel_config = request->has_parallel_config();
    bool enable_parallel = has_parallel_config && request->parallel_config().enable_parallel();

    // By default, all the tablet compaction tasks with the same txn id will be executed in the same
    // thread to avoid blocking other transactions, but if there are idle threads, they will steal
    // tasks from busy threads to execute.
    auto cb = std::make_shared<CompactionTaskCallback>(this, request, response, done);

    std::vector<std::unique_ptr<CompactionTaskContext>> contexts_vec;
    for (auto tablet_id : request->tablet_ids()) {
        auto context = std::make_unique<CompactionTaskContext>(request->txn_id(), tablet_id, request->version(),
                                                               request->force_base_compaction(),
                                                               request->skip_write_txnlog(), cb);
        contexts_vec.push_back(std::move(context));
        // DO NOT touch `context` from here!
    }

    // initialize last check time, compact request is received right after FE sends it, so consider it valid now
    cb->set_last_check_time(time(nullptr));

    std::unique_lock lock(_mutex);
    // make changes under lock
    // perform the check again under lock, so the _stopped and _task_queues operation is atomic
    if (_stopped) {
        reject_request(controller, request, response);
        return;
    }

    // Handle parallel compaction mode
    if (enable_parallel && _parallel_mgr != nullptr) {
        lock.unlock();
        guard.release();
        process_parallel_compaction(request, response, cb);
        return;
    }

    // Original non-parallel mode
    {
        std::lock_guard l(_contexts_lock);
        for (auto& ctx : contexts_vec) {
            _contexts.Append(ctx.get());
        }
    }
    _task_queues.put_by_txn_id(request->txn_id(), contexts_vec);
    // DO NOT touch `contexts_vec` from here!
    // release the done guard, let CompactionTaskCallback take charge.
    guard.release();

    TEST_SYNC_POINT("CompactionScheduler::compact:return");
}

void CompactionScheduler::process_parallel_compaction(const CompactRequest* request, CompactResponse* response,
                                                      const std::shared_ptr<CompactionTaskCallback>& callback) {
    VLOG(1) << "Processing parallel compaction request. txn_id: " << request->txn_id()
            << ", tablet_ids size: " << request->tablet_ids_size()
            << ", max_parallel: " << request->parallel_config().max_parallel_per_tablet()
            << ", max_bytes: " << request->parallel_config().max_bytes_per_subtask();

    int total_subtasks = 0;
    int successful_tablets = 0;

    // Create limiter callbacks for parallel compaction
    AcquireTokenFunc acquire_token = [this]() { return _limiter.acquire(); };
    ReleaseTokenFunc release_token = [this](bool mem_limit_exceeded) {
        if (mem_limit_exceeded) {
            _limiter.memory_limit_exceeded();
        } else {
            _limiter.no_memory_limit_exceeded();
        }
    };

    for (auto tablet_id : request->tablet_ids()) {
        auto result = _parallel_mgr->create_parallel_tasks(
                tablet_id, request->txn_id(), request->version(), request->parallel_config(), callback,
                request->force_base_compaction(), _threads.get(), acquire_token, release_token);

        if (result.ok() && result.value() > 0) {
            // Parallel compaction tasks created successfully
            total_subtasks += result.value();
            successful_tablets++;
            VLOG(1) << "Created " << result.value() << " parallel subtasks for tablet " << tablet_id;
        } else {
            // Fall back to non-parallel mode for this tablet if:
            // 1. create_parallel_tasks failed (result.status() is not OK)
            // 2. create_parallel_tasks returned 0 (indicates fallback, e.g., data size too small)
            if (!result.ok()) {
                VLOG(1) << "Failed to create parallel tasks for tablet " << tablet_id << ": " << result.status()
                        << ", falling back to normal compaction";
            } else {
                VLOG(1) << "Parallel compaction not applicable for tablet " << tablet_id
                        << ", falling back to normal compaction";
            }
            auto context = std::make_unique<CompactionTaskContext>(request->txn_id(), tablet_id, request->version(),
                                                                   request->force_base_compaction(),
                                                                   request->skip_write_txnlog(), callback);
            context->enqueue_time_sec = ::time(nullptr);

            {
                std::lock_guard l(_contexts_lock);
                _contexts.Append(context.get());
            }

            std::unique_lock lock(_mutex);
            _task_queues.put_by_txn_id(request->txn_id(), context);
        }
    }

    VLOG(1) << "Parallel compaction request processed. txn_id: " << request->txn_id()
            << ", total_subtasks: " << total_subtasks << ", successful_tablets: " << successful_tablets;
}

void CompactionScheduler::list_tasks(std::vector<CompactionTaskInfo>* infos) {
    // List regular (non-parallel) compaction tasks
    {
        std::lock_guard l(_contexts_lock);
        for (butil::LinkNode<CompactionTaskContext>* node = _contexts.head(); node != _contexts.end();
             node = node->next()) {
            CompactionTaskContext* context = node->value();
            auto& info = infos->emplace_back();
            info.txn_id = context->txn_id;
            info.tablet_id = context->tablet_id;
            info.version = context->version;
            info.skipped = context->skipped.load(std::memory_order_relaxed);
            info.runs = context->runs.load(std::memory_order_relaxed);
            info.start_time = context->start_time.load(std::memory_order_relaxed);
            info.progress = context->progress.value();
            // Load "finish_time" with memory_order_acquire and check its value before reading the "status" to avoid
            // the race condition between this thread and the `CompactionScheduler::thread_task` threads.
            info.finish_time = context->finish_time.load(std::memory_order_acquire);
            if (info.runs > 0) {
                info.profile = context->stats->to_json_stats();
            }
            if (info.finish_time > 0) {
                info.status = context->status;
            }
        }
    }

    // List parallel compaction tasks
    if (_parallel_mgr != nullptr) {
        _parallel_mgr->list_tasks(infos);
    }
}

// Pay special attentions to the following statements order with different new and old val
void CompactionScheduler::update_compact_threads(int32_t new_val) {
    if (_task_queues.modifying()) {
        LOG(ERROR) << "Failed to update compact_threads to " << new_val
                   << " due to concurrency update, reset it back to " << _task_queues.target_size();
        config::compact_threads = _task_queues.target_size();
        return;
    }

    if (new_val == _task_queues.task_queue_size()) {
        return;
    } else if (new_val <= 0) {
        LOG(ERROR) << "compact_threads can't be set to " << new_val << ", reset it back to "
                   << _task_queues.target_size();
        config::compact_threads = _task_queues.target_size();
    }

    _task_queues.set_target_size(new_val);
    if (_task_queues.target_size() != new_val) {
        LOG(ERROR) << "Failed to update compact_threads to " << new_val
                   << " due to concurrency update, bereset it back to " << _task_queues.target_size();
        config::compact_threads = _task_queues.target_size();
        return;
    }

    auto old_val = _task_queues.task_queue_size();
    if (new_val > old_val) {
        // increase queue count
        _task_queues.resize_if_needed(_limiter);
        for (int i = old_val; i < new_val; i++) {
            CHECK(_threads->submit_func([this, id = i]() { this->thread_task(id); }).ok());
        }
    } else {
        // In order to prevent exceptions due to concurrent modifications of the task queues,
        // reducing the queue length will be completed asynchronously.
    }
}

void CompactionScheduler::remove_states(const std::vector<std::unique_ptr<CompactionTaskContext>>& states) {
    std::lock_guard l(_contexts_lock);
    for (auto& context : states) {
        context->RemoveFromList();
    }

    // Cleanup parallel compaction states for merged contexts.
    // This is deferred from on_subtask_complete to ensure parallel compaction tasks
    // remain visible in list_tasks until RPC response is sent.
    if (_parallel_mgr != nullptr) {
        for (auto& context : states) {
            if (context->is_parallel_merged) {
                _parallel_mgr->cleanup_tablet(context->tablet_id, context->txn_id);
            }
        }
    }
}

void CompactionScheduler::thread_task(int id) {
    while (!_stopped.load(std::memory_order_acquire)) {
        if (reschedule_task_if_needed(id)) {
            break;
        }
        if (!_limiter.acquire()) {
            nap_sleep(1, [&] { return _stopped.load(); });
            continue;
        }

        CompactionContextPtr context;
        if (!_task_queues.try_get(id, &context)) {
            _task_queues.steal_task(id + 1, &context);
        }

        if (context != nullptr) {
            auto st = do_compaction(std::move(context));
            if (st.is_mem_limit_exceeded()) {
                _limiter.memory_limit_exceeded();
            } else {
                _limiter.no_memory_limit_exceeded();
            }
        } else {
            _limiter.no_memory_limit_exceeded();
            nap_sleep(1, [&] { return _stopped.load(); });
        }
    }
}

Status compaction_should_cancel(CompactionTaskContext* context) {
    return context->callback->is_txn_still_valid();
}

Status CompactionScheduler::do_compaction(std::unique_ptr<CompactionTaskContext> context) {
    const auto start_time = ::time(nullptr);
    const auto tablet_id = context->tablet_id;
    const auto txn_id = context->txn_id;
    const auto version = context->version;

    int64_t in_queue_time_sec = start_time > context->enqueue_time_sec ? (start_time - context->enqueue_time_sec) : 0;
    context->stats->in_queue_time_sec += in_queue_time_sec;
    context->start_time.store(start_time, std::memory_order_relaxed);
    context->runs.fetch_add(1, std::memory_order_relaxed);

    auto status = Status::OK();
    auto task_or = _tablet_mgr->compact(context.get());
    if (task_or.ok()) {
        auto should_cancel = [&]() { return compaction_should_cancel(context.get()); };
        TEST_SYNC_POINT("CompactionScheduler::do_compaction:before_execute_task");
        ThreadPool* flush_pool = nullptr;
        if (config::lake_enable_compaction_async_write) {
            // CAUTION: we reuse delta writer's memory table flush pool here
            flush_pool = StorageEngine::instance()->lake_memtable_flush_executor()->get_thread_pool();
            if (UNLIKELY(flush_pool == nullptr)) {
                return Status::InternalError("Get memory table flush pool failed");
            }
        }
        status.update(task_or.value()->execute(std::move(should_cancel), flush_pool));
    } else {
        status.update(task_or.status());
    }

    auto finish_time = std::max<int64_t>(::time(nullptr), start_time);
    auto cost = finish_time - start_time;

    // Task failure due to memory limitations allows for retries, more threads allow for more retries.
    // If allow partial success, do not retry, task result should be reported to FE as soon as possible.
    if (!context->callback->allow_partial_success() && status.is_mem_limit_exceeded() &&
        context->runs.load(std::memory_order_relaxed) < _task_queues.task_queue_size() + 1) {
        LOG(WARNING) << "Memory limit exceeded, will retry later. tablet_id=" << tablet_id << " version=" << version
                     << " txn_id=" << txn_id << " cost=" << cost << "s";
        context->progress.update(0);
        // reset start time and re-schedule the compaction task
        context->start_time.store(0, std::memory_order_relaxed);
        _task_queues.put_by_txn_id(context->txn_id, context);
    } else {
        VLOG_IF(3, status.ok()) << "Compacted tablet " << tablet_id << ". version=" << version << " txn_id=" << txn_id
                                << " cost=" << cost << "s";

        LOG_IF(ERROR, !status.ok()) << "Fail to compact tablet " << tablet_id << ". version=" << version
                                    << " txn_id=" << txn_id << " cost=" << cost << "s : " << status;

        context->status = status;

        // Here we update "finish_time" after "status" and use "memory_order_release" to prevent concurrent read&write
        // on "status", other threads should read "finish_time" with "memory_order_acquire" and check whether its value
        // is greater than zero before reading "status".
        context->finish_time.store(finish_time, std::memory_order_release);

        auto cb = context->callback;
        cb->finish_task(std::move(context));
    }

    return status;
}

void CompactionScheduler::abort_compaction(std::unique_ptr<CompactionTaskContext> context) {
    const auto start_time = ::time(nullptr);
    const auto tablet_id = context->tablet_id;
    const auto txn_id = context->txn_id;
    const auto version = context->version;

    int64_t in_queue_time_sec = start_time > context->enqueue_time_sec ? (start_time - context->enqueue_time_sec) : 0;
    context->stats->in_queue_time_sec += in_queue_time_sec;
    context->status = Status::Aborted("Compaction task aborted due to BE/CN shutdown!");
    LOG(WARNING) << "Fail to compact tablet " << tablet_id << ". version=" << version << " txn_id=" << txn_id << " : "
                 << context->status;
    // make sure every task can be finished no matter it is succeeded or failed.
    context->callback->finish_task(std::move(context));
}

Status CompactionScheduler::abort(int64_t txn_id) {
    std::unique_lock l(_contexts_lock);
    for (butil::LinkNode<CompactionTaskContext>* node = _contexts.head(); node != _contexts.end();
         node = node->next()) {
        CompactionTaskContext* context = node->value();
        if (context->txn_id == txn_id) {
            auto cb = context->callback;
            l.unlock();
            // Do NOT touch |context| since here, it may have been destroyed.
            TEST_SYNC_POINT("lake::CompactionScheduler::abort:unlock:1");
            TEST_SYNC_POINT("lake::CompactionScheduler::abort:unlock:2");
            cb->update_status(Status::Aborted("aborted on demand"));
            return Status::OK();
        }
    }
    return Status::NotFound(fmt::format("no compaction task with txn id {}", txn_id));
}

void CompactionScheduler::abort_all() {
    for (int i = 0; i < _task_queues.task_queue_size(); ++i) {
        // drain _task_queues, ensure every tasks in queue are properly aborted
        bool done = false;
        while (!done) {
            CompactionContextPtr context;
            if (_task_queues.try_get(i, &context)) {
                abort_compaction(std::move(context));
            } else {
                done = true;
            }
        }
    }
}

// If `lake_compaction_max_concurrency` is reduced during runtime, `id` may exceed it.
// Reschedule all the tasks in _task_queues where idx ranges from [new_val, old_val-1].
// return true means current thread id is beyond target size, current thread shoud exist.
bool CompactionScheduler::reschedule_task_if_needed(int id) {
    if (id >= _task_queues.target_size()) {
        CompactionContextPtr context;
        while (_task_queues.try_get(id, &context)) {
            _task_queues.put_by_txn_id(context->txn_id, context);
        }

        _task_queues.resize_if_needed(_limiter);
        return true;
    }
    return false;
}

// Shrink _task_queues if `id` exceeds _target_size, provided that all tasks
// from superfluous threads have been rescheduled.
// Expanding the queue can be executed immediately.
void CompactionScheduler::WrapTaskQueues::resize_if_needed(Limiter& limiter) {
    std::lock_guard<std::mutex> lock(_task_queues_mutex);
    for (int i = _target_size; i < _internal_task_queues.size(); i++) {
        if (_internal_task_queues[i]->get_size() > 0) {
            return;
        }
    }
    resize(_target_size);
    limiter.adapt_to_task_queue_size(_target_size);
}

} // namespace starrocks::lake
