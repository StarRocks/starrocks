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
#include "common/status.h"
#include "fs/fs.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gutil/stl_util.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "service/service_be/lake_service.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/tablet_manager.h"
#include "storage/memtable_flush_executor.h"
#include "storage/storage_engine.h"
#include "testutil/sync_point.h"
#include "util/threadpool.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks::lake {

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

void CompactionTaskCallback::finish_task(std::unique_ptr<CompactionTaskContext>&& context) {
    std::unique_lock l(_mtx);

    if (!context->status.ok()) {
        // Add failed tablet for upgrade compatibility: older version FE relies on the failed tablet to determine
        // whether the job is successful.
        _response->add_failed_tablets(context->tablet_id);
    }

    DCHECK(_request != nullptr);
    _status.update(context->status);

    // Keep the context for a while until the RPC request is finished processing so that we can see the detailed
    // and complete progress of the RPC request by calling `CompactionScheduler::list_tasks()`.
    _contexts.emplace_back(std::move(context));
    //                     ^^^^^^^^^^^^^^^^^ Do NOT touch "context" since here, it has been `move`ed.

    if (_contexts.size() == _request->tablet_ids_size()) { // All tasks finished, send RPC response to FE
        _status.to_protobuf(_response->mutable_status());
        if (_done != nullptr) {
            _done->Run();
            _done = nullptr;
        }
        _request = nullptr;
        _response = nullptr;

        std::vector<std::unique_ptr<CompactionTaskContext>> tmp;
        tmp.swap(_contexts);

        l.unlock();
        _scheduler->remove_states(tmp);
        tmp.clear();
        TEST_SYNC_POINT("lake::CompactionTaskCallback::finish_task:finish_task");
    }
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
}

CompactionScheduler::~CompactionScheduler() {
    _stopped.store(true, std::memory_order_relaxed);
    _threads->shutdown();
}

void CompactionScheduler::compact(::google::protobuf::RpcController* controller, const CompactRequest* request,
                                  CompactResponse* response, ::google::protobuf::Closure* done) {
    // By default, all the tablet compaction tasks with the same txn id will be executed in the same
    // thread to avoid blocking other transactions, but if there are idle threads, they will steal
    // tasks from busy threads to execute.
    auto cb = std::make_shared<CompactionTaskCallback>(this, request, response, done);
    bool is_checker = true; // make the first tablet as checker
    std::vector<std::unique_ptr<CompactionTaskContext>> contexts_vec;
    for (auto tablet_id : request->tablet_ids()) {
        auto context = std::make_unique<CompactionTaskContext>(request->txn_id(), tablet_id, request->version(),
                                                               is_checker, cb);
        {
            std::lock_guard l(_contexts_lock);
            _contexts.Append(context.get());
        }
        contexts_vec.push_back(std::move(context));
        // DO NOT touch `context` from here!
        is_checker = false;
    }
    _task_queues.put_by_txn_id(request->txn_id(), contexts_vec);
    // DO NOT touch `contexts_vec` from here!
    TEST_SYNC_POINT("CompactionScheduler::compact:return");
}

void CompactionScheduler::list_tasks(std::vector<CompactionTaskInfo>* infos) {
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
        if (info.finish_time > 0) {
            info.status = context->status;
        }
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
}

void CompactionScheduler::thread_task(int id) {
    while (!_stopped.load(std::memory_order_acquire)) {
        if (reschedule_task_if_needed(id)) {
            break;
        }
        if (!_limiter.acquire()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
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
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}

bool compaction_should_cancel(CompactionTaskContext* context) {
    if (context->callback->has_error() || context->callback->timeout_exceeded()) {
        return true;
    }

    int64_t check_interval_seconds = 60LL * config::lake_compaction_check_valid_interval_minutes;
    if (!context->is_checker || check_interval_seconds <= 0) {
        return false;
    }

    int64_t now = time(nullptr);
    if (now > context->last_check_time && (now - context->last_check_time) >= check_interval_seconds) {
        // ask FE whether this compaction transaction is still valid
#ifndef BE_TEST
        TNetworkAddress master_addr = get_master_address();
        if (master_addr.hostname.size() > 0 && master_addr.port > 0) {
            TReportLakeCompactionRequest request;
            request.__set_txn_id(context->txn_id);
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
                    LOG(WARNING) << "validate compaction transaction " << context->txn_id << " for tablet "
                                 << context->tablet_id << ", abort invalid compaction";
                    context->callback->update_status(Status::Aborted("compaction validation failed"));
                    return true; // should cancel compaction
                } else {
                    // everything is fine
                }
            } else {
                LOG(WARNING) << "fail to validate compaction transaction " << context->txn_id << " for tablet "
                             << context->tablet_id << ", error: " << status;
            }
        } else {
            LOG(WARNING) << "fail to validate compaction transaction " << context->txn_id << " for tablet "
                         << context->tablet_id << ", error: leader FE address not found";
        }
#endif
        // update check time, if check rpc failed, wait next round
        context->last_check_time = now;
    }
    return false;
}

Status CompactionScheduler::do_compaction(std::unique_ptr<CompactionTaskContext> context) {
    const auto start_time = ::time(nullptr);
    const auto tablet_id = context->tablet_id;
    const auto txn_id = context->txn_id;
    const auto version = context->version;

    context->start_time.store(start_time, std::memory_order_relaxed);
    context->runs.fetch_add(1, std::memory_order_relaxed);
    if (context->is_checker) {
        context->last_check_time = start_time;
    }

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

    // Task failure due to memory limitations allows for retries. more threads allow for more retries.
    if (status.is_mem_limit_exceeded() &&
        context->runs.load(std::memory_order_relaxed) < _task_queues.task_queue_size() + 1) {
        LOG(WARNING) << "Memory limit exceeded, will retry later. tablet_id=" << tablet_id << " version=" << version
                     << " txn_id=" << txn_id << " cost=" << cost << "s";
        context->progress.update(0);
        // re-schedule the compaction task
        _task_queues.put_by_txn_id(context->txn_id, context);
    } else {
        VLOG_IF(3, status.ok()) << "Compacted tablet " << tablet_id << ". version=" << version << " txn_id=" << txn_id
                                << " cost=" << cost << "s";

        if (status.is_cancelled()) {
            if (context->callback->has_error()) {
                auto cause = context->callback->error();
                status = Status::Cancelled(fmt::format("Cancelled due to another error: {}", cause.message()));
            } else if (context->callback->timeout_exceeded()) {
                auto timeout = context->callback->timeout_ms();
                status = Status::Cancelled(fmt::format("Cancelled due to timeout exceeded: {}ms", timeout));
            }
        }
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
