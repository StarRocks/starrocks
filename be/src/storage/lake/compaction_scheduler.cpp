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

#include "common/status.h"
#include "fs/fs.h"
#include "gutil/stl_util.h"
#include "runtime/exec_env.h"
#include "service/service_be/lake_service.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/tablet_manager.h"
#include "testutil/sync_point.h"
#include "util/threadpool.h"

namespace starrocks::lake {

CompactionTaskCallback::~CompactionTaskCallback() = default;

CompactionTaskCallback::CompactionTaskCallback(CompactionScheduler* scheduler, const lake::CompactRequest* request,
                                               lake::CompactResponse* response, ::google::protobuf::Closure* done)
        : _scheduler(scheduler), _mtx(), _request(request), _response(response), _done(done) {
    CHECK(_request != nullptr);
    CHECK(_response != nullptr);
    _contexts.reserve(request->tablet_ids_size());
}

void CompactionTaskCallback::finish_task(std::unique_ptr<CompactionTaskContext>&& context) {
    std::lock_guard l(_mtx);

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

        _scheduler->remove_states(_contexts);
        STLClearObject(&_contexts);
    }
}

CompactionScheduler::CompactionScheduler(TabletManager* tablet_mgr)
        : _tablet_mgr(tablet_mgr),
          _limiter(config::compact_task_queue_count),
          _contexts_lock(),
          _contexts(),
          _task_queue_count(config::compact_task_queue_count),
          _task_queues(config::compact_task_queue_count) {
    CHECK_GT(_task_queue_count, 0);
    auto st = ThreadPoolBuilder("clound_native_compact")
                      .set_min_threads(0)
                      .set_max_threads(INT_MAX)
                      .set_max_queue_size(INT_MAX)
                      .build(&_threads);
    CHECK(st.ok()) << st;

    for (int i = 0; i < _task_queue_count; i++) {
        CHECK(_threads->submit_func([this, id = i]() { this->thread_task(id); }).ok());
    }
}

CompactionScheduler::~CompactionScheduler() {
    _stopped.store(true, std::memory_order_relaxed);
    _threads->wait();
    delete[] _task_queues;
}

void CompactionScheduler::compact(::google::protobuf::RpcController* controller, const CompactRequest* request,
                                  CompactResponse* response, ::google::protobuf::Closure* done) {
    // By default, all the tablet compaction tasks with the same txn id will be executed in the same
    // thread to avoid blocking other transactions, but if there are idle threads, they will steal
    // tasks from busy threads to execute.
    auto idx = choose_task_queue_by_txn_id(request->txn_id());
    auto cb = std::make_shared<CompactionTaskCallback>(this, request, response, done);
    for (auto tablet_id : request->tablet_ids()) {
        auto context = std::make_unique<CompactionTaskContext>(request->txn_id(), tablet_id, request->version(), cb);
        {
            std::lock_guard l(_contexts_lock);
            _contexts.Append(context.get());
        }
        {
            std::lock_guard<std::mutex> lock(_task_queues_mutex);
            _task_queues[idx].put(std::move(context));
        }
    }
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
void CompactionScheduler::update_compact_task_queue_count(int32_t new_val) {
    CHECK_GT(new_val, 0);
    auto old_val = _task_queue_count;
    if (new_val > old_val) {
        {
            std::lock_guard<std::mutex> lock(_task_queues_mutex);
            // increase queue count
            _task_queues.resize(new_val);
        }
        _limiter.adapt_to_task_queue_count(new_val);
        _task_queue_count.store(new_val);
        for (int i = old_val; i < new_val; i++) {
            CHECK(_threads->submit_func([this, id = i]() { this->thread_task(id); }).ok());
        }
    } else {
        _task_queue_count.store(new_val);

        // wait and make sure tasks in _task_queues[idx] all have been re-scheduled,
        // idx is in range [new_val, ola_val-1]
        // todo can we ignore lock here? can we allow multiple threads to update the same config item at the same time?
        // todo using something like Semaphore here might be better?
        for (int i = new_val; i < old_val; i++) {
            for (auto _task_queues[i].get_size() > 0) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }
        }

        {
            std::lock_guard<std::mutex> lock(_task_queues_mutex);
            // shrink queues count
            _task_queues.resize(new_val);
        }
        _limiter.adapt_to_task_queue_count(new_val);
        // we don't explicitly shrink _threads pool here.
        // instead we try to abort thread in `CompactionScheduler::thread_task(int id)` function,
        // where we compare the value of `id` with this comming smaller new_val
    }
}

void CompactionScheduler::remove_states(const std::vector<std::unique_ptr<CompactionTaskContext>>& states) {
    std::lock_guard l(_contexts_lock);
    for (auto& context : states) {
        context->RemoveFromList();
    }
}

void CompactionScheduler::steal_task(int start_index, std::unique_ptr<CompactionTaskContext>* context) {
    auto queue_count = _task_queue_count.load();
    std::lock_guard<std::mutex> lock(_task_queues_mutex);
    for (int i = 0; i < queue_count; i++) {
        if (_task_queues[(start_index + i) % queue_count].try_get(context)) {
            return;
        }
    }
    DCHECK(*context == nullptr);
}

void CompactionScheduler::thread_task(int id) {
    while (!_stopped.load(std::memory_order_acquire)) {
        // When `_task_queue_count` is decreased at runtime, `id` might be greater than `_task_queue_count`.
        // we should abort the thread that associated with this id.
        // But before doing that, the remaining tasks in `_task_queues[id]` should re-schedule to new queue
        if (id >= _task_queue_count.load()) {
            CompactionContextPtr context;
            auto ret = _task_queues[id].try_get(&context));
            if (!ret) {
                // no remaining tasks in `task_queues[id]`
                return;
            }
            auto idx = choose_task_queue_by_txn_id(context->txn_id);
            {
                std::lock_guard<std::mutex> lock(_task_queues_mutex);
                // re-schedule the compaction task
                _task_queues[idx].put(std::move(context));
            }
        }

        if (!_limiter.acquire()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        CompactionContextPtr context;
        {
            std::lock_guard<std::mutex> lock(_task_queues_mutex);
            if (!_task_queues[id].try_get(&context)) {
                steal_task(id + 1, &context);
            }
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

Status CompactionScheduler::do_compaction(std::unique_ptr<CompactionTaskContext> context) {
    const auto start_time = ::time(nullptr);
    const auto tablet_id = context->tablet_id;
    const auto txn_id = context->txn_id;
    const auto version = context->version;

    if (context->start_time.load(std::memory_order_relaxed) == 0) {
        context->start_time.store(start_time, std::memory_order_relaxed);
    }
    const auto runs = context->runs.fetch_add(1, std::memory_order_relaxed);

    auto status = Status::OK();
    if (config::lake_compaction_check_txn_log_first && runs == 0 && txn_log_exists(tablet_id, txn_id)) {
        context->skipped.store(true, std::memory_order_relaxed);
        context->progress.update(100);
        VLOG(2) << "Skipped already succeeded compaction task. tablet_id=" << tablet_id << " txn_id=" << txn_id
                << " version=" << version;
    } else {
        auto task_or = _tablet_mgr->compact(tablet_id, version, txn_id);
        if (task_or.ok()) {
            auto should_cancel = [&]() { return context->callback->has_error(); };
            TEST_SYNC_POINT("CompactionScheduler::do_compaction:before_execute_task");
            status.update(task_or.value()->execute(&context->progress, std::move(should_cancel)));
        } else {
            status.update(task_or.status());
        }
    }

    auto finish_time = std::max<int64_t>(::time(nullptr), start_time);
    auto cost = finish_time - start_time;

    // Task failure due to memory limitations allows for retries. more threads allow for more retries.
    if (status.is_mem_limit_exceeded() && context->runs.load(std::memory_order_relaxed) < _task_queue_count.load() + 1) {
        LOG(WARNING) << "Memory limit exceeded, will retry later. tablet_id=" << tablet_id << " version=" << version
                     << " txn_id=" << txn_id << " cost=" << cost << "s";
        context->progress.update(0);
        auto idx = choose_task_queue_by_txn_id(context->txn_id);
        {
            std::lock_guard<std::mutex> lock(_task_queues_mutex);
            // re-schedule the compaction task
            _task_queues[idx].put(std::move(context));
        }
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

bool CompactionScheduler::txn_log_exists(int64_t tablet_id, int64_t txn_id) const {
    auto txn_log = _tablet_mgr->txn_log_location(tablet_id, txn_id);
    auto fs_or = FileSystem::CreateSharedFromString(txn_log);
    return fs_or.ok() && fs_or.value()->path_exists(txn_log).ok();
}

Status CompactionScheduler::abort(int64_t txn_id) {
    std::lock_guard l(_contexts_lock);
    for (butil::LinkNode<CompactionTaskContext>* node = _contexts.head(); node != _contexts.end();
         node = node->next()) {
        CompactionTaskContext* context = node->value();
        if (context->txn_id == txn_id) {
            context->callback->update_status(Status::Aborted("aborted on demand"));
            return Status::OK();
        }
    }
    return Status::NotFound(fmt::format("no compaction task with txn id {}", txn_id));
}

} // namespace starrocks::lake
