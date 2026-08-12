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

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "common/config_compaction_fwd.h"
#include "common/config_storage_fwd.h"
#include "common/status.h"
#include "common/system/master_info.h"
#include "common/thread/threadpool.h"
#include "common/util/misc.h"
#include "common/util/thrift_client_cache.h"
#include "fs/fs.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/lake_service.pb.h"
#include "gutil/stl_util.h"
#include "platform/key_cache.h"
#include "platform/thrift_rpc_helper.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/lake_proto_normalizer.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_parallel_compaction_manager.h"
#include "storage/memtable_flush_executor.h"
#include "storage/storage_engine.h"

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
        : _scheduler(scheduler), _mtx(), _request(request), _response(response), _done(done) {
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

bool CompactionTaskCallback::skip_write_txnlog() const {
    return _request->has_skip_write_txnlog() && _request->skip_write_txnlog();
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
        // Dual-write the legacy arrays into the RPC payload so an aggregator without the segment_metas
        // refactor (not-yet-upgraded or rolled-back) persists old-readable metadata. Normalize a temp
        // copy and only return it on success; never put a non-dual-written / bad txn log in the response.
        TxnLogPB normalized(*context->txn_log);
        if (auto st = normalize_txn_log_before_save(&normalized); st.ok()) {
            _response->add_txn_logs()->Swap(&normalized);
        } else {
            LOG(WARNING) << "Fail to normalize aggregate-compact txn log: " << st
                         << " tablet_id=" << context->tablet_id;
            _response->add_failed_tablets(context->tablet_id);
            _status.update(st);
        }
    }
    DCHECK(_request != nullptr);
    _status.update(context->status);

    // Register a parallel merged context so remove_states() can defer cleanup of
    // its individual subtask rows until the RPC response is sent. list_tasks()
    // deliberately hides this aggregation-only context.
    if (context->is_parallel_merged && _scheduler != nullptr) {
        std::lock_guard ctx_lock(_scheduler->_contexts_lock);
        _scheduler->_contexts.Append(context.get());
    }

    // Keep the context until the RPC request finishes. Regular contexts remain
    // visible through list_tasks(); a merged context anchors parallel-state cleanup.
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
    auto st = ThreadPoolBuilder("lake_compact")
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
        // Snapshot the parallel-compaction request here, on the bthread. The worker that later plans the
        // subtasks reads these instead of `request`, which it must not touch: `request`/`response` are only
        // guaranteed to outlive the worker while some tablet still has an unfinished context, and once the
        // last finish_task() runs `done` brpc frees them.
        if (enable_parallel && _parallel_mgr != nullptr) {
            context->parallel_requested = true;
            context->parallel_max_parallel_per_tablet = request->parallel_config().max_parallel_per_tablet();
            context->parallel_max_bytes_per_subtask = request->parallel_config().max_bytes_per_subtask();
        }
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

    // Both modes publish their contexts the same way from here on. Planning the parallel subtasks needs
    // blocking StarOS/Starlet filesystem IO, which must not run on this brpc bthread -- a pthread rwlock
    // held across a bthread yield (StarOSWorker::_cache_mtx while an evicted FileSystem is destroyed)
    // makes pthread_rwlock_wrlock return EDEADLK, which std::shared_mutex turns into an uncaught
    // std::system_error and which aborts the CN (issue #76882). So the planning happens later, in
    // do_compaction(), on the resident thread_task() worker that already runs every other bit of
    // compaction IO. Publishing all contexts here, before any worker can dequeue one, is also what keeps
    // CompactionTaskCallback::finish_task() from completing the RPC before every tablet has a context --
    // the worker reads `request`/`response` through the callback, so an early completion would leave it
    // dereferencing memory brpc has already freed.
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

void CompactionScheduler::list_tasks(std::vector<CompactionTaskInfo>* infos) {
    // List regular (non-parallel) compaction tasks
    {
        std::lock_guard l(_contexts_lock);
        for (butil::LinkNode<CompactionTaskContext>* node = _contexts.head(); node != _contexts.end();
             node = node->next()) {
            CompactionTaskContext* context = node->value();
            // A merged parallel context is an RPC aggregation artifact rather than
            // an executed compaction unit. The individual subtasks are exposed by
            // TabletParallelCompactionManager::list_tasks().
            if (context->is_parallel_merged) {
                continue;
            }
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
                const bool profile_final = info.finish_time > 0;
                info.profile = context->stats_snapshot(!profile_final).to_json_stats(profile_final);
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

    // Cleanup parallel compaction states for merged contexts. This is deferred
    // from on_subtask_complete so individual subtask rows remain visible through
    // list_tasks() until the RPC response is sent.
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
            // do_compaction() may hand this tablet to parallel subtasks, which take over the token; it then
            // reports that this worker is no longer holding one and there is nothing here to credit back.
            bool token_given_up = false;
            auto st = do_compaction(std::move(context), &token_given_up);
            if (token_given_up) {
                // Deliberately no credit: the token belongs to whoever took it over.
            } else if (st.is_mem_limit_exceeded()) {
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

// Tries to replace this tablet's serial compaction with parallel subtasks.
//
// Returns true only if subtasks were submitted, in which case they own the tablet's single finish_task()
// and |context| has been unlinked and destroyed -- the caller must not touch it, nor the callback, again.
// Returns false if parallel compaction does not apply (or planning failed), leaving |context| untouched so
// the caller can compact the tablet serially with it.
//
// Sets |*token_given_up| when this worker no longer holds its limiter token, so that thread_task() does not
// credit back a token it does not have.
bool CompactionScheduler::try_hand_off_to_parallel(std::unique_ptr<CompactionTaskContext>& context,
                                                   bool* token_given_up) {
    const auto tablet_id = context->tablet_id;
    const auto txn_id = context->txn_id;

    // Don't start subtasks we cannot finish. ThreadPool::shutdown() drops queued tasks and calls the
    // no-op FunctionRunnable::cancel() on them, so a subtask queued during shutdown never reports back,
    // its tablet never drains, and nothing would complete the RPC. Compacting serially instead keeps the
    // context in the queue, where abort_all() finds and aborts it.
    if (_stopped.load(std::memory_order_acquire)) {
        return false;
    }

    // A txn that FE already cancelled, or one whose deadline has passed, would spawn subtasks that cannot
    // be cancelled (abort() does not reach them) and would run to completion for nothing. The serial path
    // notices the same condition through should_cancel and fails fast.
    if (context->callback != nullptr && !context->callback->has_error().ok()) {
        return false;
    }

    // This worker holds one of the limiter's tokens for as long as do_compaction() runs. Hand it back
    // before planning, for two reasons:
    //
    //  - the subtasks are reserved all-or-nothing, so holding onto it would make parallel compaction need
    //    max_parallel_per_tablet + 1 free tokens; with the defaults (3 subtasks, compact_threads=4) a
    //    single other compaction anywhere on the CN would then be enough to quietly downgrade every
    //    tablet to serial compaction;
    //  - keeping it and settling up later cannot work: a handed-off do_compaction() returns while the
    //    subtasks are still running, so thread_task() would credit the token back and leave a phantom
    //    free token -- letting compactions exceed the configured (or memory-reduced) concurrency for as
    //    long as the subtasks run.
    //
    // The subtasks now simply acquire and release tokens like any other task. What is left is telling
    // thread_task() not to credit a token it no longer holds, which |token_returned| below does.
    _limiter.return_token();
    bool handed_off = false;
    DeferOp settle_token([&]() {
        if (handed_off) {
            // The subtasks hold the tokens now and release them as they finish; this worker has none.
            *token_given_up = true;
            return;
        }
        // Not handing off after all: this worker compacts the tablet serially, so take a token back for
        // thread_task() to credit. If none is free -- someone claimed it while we were planning -- carry on
        // without one rather than blocking, and tell thread_task() not to credit what we do not hold.
        if (!_limiter.acquire()) {
            *token_given_up = true;
        }
    });

    AcquireTokenFunc acquire_token = [this]() { return _limiter.acquire(); };
    ReleaseTokenFunc release_token = [this](bool mem_limit_exceeded) {
        if (mem_limit_exceeded) {
            _limiter.memory_limit_exceeded();
        } else {
            _limiter.no_memory_limit_exceeded();
        }
    };

    TabletParallelConfig parallel_config;
    parallel_config.set_enable_parallel(true);
    parallel_config.set_max_parallel_per_tablet(context->parallel_max_parallel_per_tablet);
    parallel_config.set_max_bytes_per_subtask(context->parallel_max_bytes_per_subtask);

    auto result = [&]() -> StatusOr<int> {
        try {
            TEST_SYNC_POINT("CompactionScheduler::try_hand_off_to_parallel:create_parallel_tasks");
            // Pass on the queue wait already recorded on this context: it is destroyed at the hand-off,
            // so the merged context has to inherit it or CompactResponse under-reports the queue time by
            // exactly the wait this hand-off introduces.
            return _parallel_mgr->create_parallel_tasks(
                    tablet_id, txn_id, context->version, parallel_config, context->callback,
                    context->force_base_compaction, _threads.get(), acquire_token, release_token,
                    context->stats->in_queue_time_sec, context->stats->queue_wait_ns);
        } catch (const std::exception& e) {
            LOG(WARNING) << "Exception while planning parallel compaction, compacting serially instead. tablet_id="
                         << tablet_id << ", txn_id=" << txn_id << ": " << e.what();
            return Status::InternalError(fmt::format("exception in create_parallel_tasks: {}", e.what()));
        } catch (...) {
            LOG(WARNING) << "Unknown exception while planning parallel compaction, compacting serially instead. "
                            "tablet_id="
                         << tablet_id << ", txn_id=" << txn_id;
            return Status::InternalError("unknown exception in create_parallel_tasks");
        }
    }();

    if (!result.ok() || result.value() <= 0) {
        if (!result.ok()) {
            // Planning reserves one limiter token per subtask, all or nothing, so a busy scheduler can
            // leave too few for the whole set. That is a silent downgrade to serial compaction.
            LOG_IF(WARNING, result.status().is_resource_busy())
                    << "Not enough compaction limiter tokens to run tablet " << tablet_id
                    << " in parallel out of a concurrency of " << _limiter.concurrency()
                    << "; compacting it serially. Other compactions are holding tokens -- parallel "
                       "compaction wants compact_threads to be at least max_parallel_per_tablet.";
            VLOG(1) << "Parallel compaction planning failed for tablet " << tablet_id << ": " << result.status()
                    << ", compacting serially";
        } else {
            VLOG(1) << "Parallel compaction not applicable for tablet " << tablet_id << ", compacting serially";
        }
        return false;
    }

    VLOG(1) << "Created " << result.value() << " parallel subtasks for tablet " << tablet_id << ", txn_id=" << txn_id;

    // The subtasks are running and will complete this tablet, so retire our context. It has to leave
    // _contexts before it is destroyed: the list holds a bare pointer that list_tasks() and abort() walk
    // under _contexts_lock, and CompactionTaskContext's debug destructor asserts the node was unlinked.
    // Destroy it outside the lock -- it may drop the last reference to the callback.
    {
        std::lock_guard l(_contexts_lock);
        context->RemoveFromList();
    }
    context.reset();
    handed_off = true;
    return true;
}

Status CompactionScheduler::do_compaction(std::unique_ptr<CompactionTaskContext> context, bool* token_given_up) {
    const auto start_time = ::time(nullptr);
    const auto start_time_ns = MonotonicNanos();
    const auto tablet_id = context->tablet_id;
    const auto txn_id = context->txn_id;
    const auto version = context->version;

    // Each retry is a new execution attempt. The previous attempt has already been
    // emitted to the slow log, so the context only keeps stats for the latest attempt.
    if (context->runs.load(std::memory_order_relaxed) > 0) {
        context->reset_attempt_stats();
    }
    context->task_attempt_start_ns.store(start_time_ns, std::memory_order_release);

    int64_t in_queue_time_sec = start_time > context->enqueue_time_sec ? (start_time - context->enqueue_time_sec) : 0;
    context->stats->in_queue_time_sec += in_queue_time_sec;
    if (context->enqueue_time_ns > 0 && start_time_ns > context->enqueue_time_ns) {
        context->stats->queue_wait_ns += start_time_ns - context->enqueue_time_ns;
    }
    context->start_time.store(start_time, std::memory_order_relaxed);
    const int attempt = context->runs.fetch_add(1, std::memory_order_relaxed) + 1;
    context->stats->task_attempt_count = attempt;
    context->publish_stats_snapshot();

    // Plan the parallel subtasks here rather than in compact(): this runs on a resident thread_task()
    // worker, so the blocking StarOS/Starlet filesystem IO it needs is fine, whereas on compact()'s brpc
    // bthread it aborts the CN (issue #76882). Done before _tablet_mgr->compact() because that would
    // otherwise load the tablet and pick rowsets only for the work to be thrown away.
    if (context->parallel_requested && _parallel_mgr != nullptr) {
        auto handed_off = try_hand_off_to_parallel(context, token_given_up);
        if (handed_off) {
            // The subtasks own this tablet's single finish_task() from here on, so `context` must not
            // complete it. It has already been unlinked and destroyed; nothing below may touch it.
            return Status::OK();
        }
        // Not applicable, or planning failed: fall through and compact this tablet serially with the very
        // same context. Reusing it -- instead of creating a second one, as the old dispatcher did -- is
        // what makes a duplicate finish_task() for one tablet impossible by construction.
    }

    auto status = Status::OK();
    auto task_prepare_start_ns = MonotonicNanos();
    auto task_or = _tablet_mgr->compact(context.get());
    context->stats->task_prepare_ns += MonotonicNanos() - task_prepare_start_ns;
    context->publish_stats_snapshot();
    if (task_or.ok()) {
        auto should_cancel = [&]() { return compaction_should_cancel(context.get()); };
        TEST_SYNC_POINT("CompactionScheduler::do_compaction:before_execute_task");
        ThreadPool* flush_pool = nullptr;
        if (config::lake_enable_compaction_async_write) {
            // CAUTION: we reuse delta writer's memory table flush pool here
            flush_pool = StorageEngine::instance()->lake_memtable_flush_executor()->get_thread_pool();
            if (UNLIKELY(flush_pool == nullptr)) {
                status.update(Status::InternalError("Get memory table flush pool failed"));
            }
        }
        if (status.ok()) {
            auto task_execute_start_ns = MonotonicNanos();
            context->task_execute_start_ns.store(task_execute_start_ns, std::memory_order_release);
            status.update(task_or.value()->execute(std::move(should_cancel), flush_pool));
            context->stats->task_execute_ns += MonotonicNanos() - task_execute_start_ns;
            context->task_execute_start_ns.store(0, std::memory_order_release);
        }
    } else {
        status.update(task_or.status());
    }
    context->stats->task_total_ns += MonotonicNanos() - start_time_ns;
    context->task_attempt_start_ns.store(0, std::memory_order_release);
    context->publish_stats_snapshot();

    auto finish_time = std::max<int64_t>(::time(nullptr), start_time);
    auto cost = finish_time - start_time;

    if (context->stats->is_slow(config::lake_compact_slow_log_ms)) {
        LOG(INFO) << "Compaction task attempt finished. tablet_id=" << tablet_id << " version=" << version
                  << " txn_id=" << txn_id << " attempt=" << attempt << " status=" << status
                  << " profile=" << context->stats->to_json_stats() << " table_id=" << context->table_id
                  << " partition_id=" << context->partition_id;
    }

    // Task failure due to memory limitations allows for retries, more threads allow for more retries.
    // If allow partial success, do not retry, task result should be reported to FE as soon as possible.
    const bool should_retry = !context->callback->allow_partial_success() && status.is_mem_limit_exceeded() &&
                              attempt < _task_queues.task_queue_size() + 1;
    if (should_retry) {
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
    const auto start_time_ns = MonotonicNanos();
    const auto tablet_id = context->tablet_id;
    const auto txn_id = context->txn_id;
    const auto version = context->version;

    int64_t in_queue_time_sec = start_time > context->enqueue_time_sec ? (start_time - context->enqueue_time_sec) : 0;
    context->stats->in_queue_time_sec += in_queue_time_sec;
    if (context->enqueue_time_ns > 0 && start_time_ns > context->enqueue_time_ns) {
        context->stats->queue_wait_ns += start_time_ns - context->enqueue_time_ns;
    }
    context->status = Status::Aborted("Compaction task aborted due to BE/CN shutdown!");
    LOG(WARNING) << "Fail to compact tablet " << tablet_id << ". version=" << version << " txn_id=" << txn_id << " : "
                 << context->status << " table_id=" << context->table_id << " partition_id=" << context->partition_id;
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
    l.unlock();

    // A tablet handed off to parallel subtasks has no node in _contexts between the hand-off and its
    // merged context being appended, so the walk above misses it and would report the whole txn as not
    // found. Ask the parallel manager instead; its subtasks poll the callback through their cancel_func,
    // so marking it is what actually stops them.
    //
    // The lock was released first on purpose: the existing order is _contexts_lock -> _states_mutex
    // (remove_states() -> cleanup_tablet()), and update_status() takes the callback's own mutex, so
    // neither is held here.
    if (_parallel_mgr != nullptr) {
        auto callbacks = _parallel_mgr->collect_callbacks_for_txn(txn_id);
        for (auto& cb : callbacks) {
            cb->update_status(Status::Aborted("aborted on demand"));
        }
        if (!callbacks.empty()) {
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
