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

#include <bthread/mutex.h>
#include <butil/containers/linked_list.h>

#include <memory>

#include "common/status.h"
#include "gutil/macros.h"
#include "storage/lake/compaction_task.h"
#include "util/blocking_queue.hpp"

namespace google::protobuf {
class RpcController;
class Closure;
} // namespace google::protobuf

namespace starrocks {
class ThreadPool;
} // namespace starrocks

namespace starrocks::lake {

class CompactRequest;
class CompactResponse;
class CompactionScheduler;
struct CompactionTaskContext;
class TabletManager;

// For every `CompactRequest` a new `CompactionTaskCallback` instance will be created.
// A single `CompactRequest` may have multiple tablets to be compacted, each time a tablet compaction
// task finished，either success or fail, `CompactionTaskCallback::finish_task()` will be invoked and
// when the last tablet compaction task finished and `CompactionTaskCallback` is invoked, the
// `CompactResponse` will be sent to the FE.
class CompactionTaskCallback {
public:
    explicit CompactionTaskCallback(CompactionScheduler* scheduler, const lake::CompactRequest* request,
                                    lake::CompactResponse* response, ::google::protobuf::Closure* done);

    ~CompactionTaskCallback();

    DISALLOW_COPY_AND_MOVE(CompactionTaskCallback);

    void finish_task(std::unique_ptr<CompactionTaskContext>&& context);

    bool has_error() const {
        std::lock_guard l(_mtx);
        return !_status.ok();
    }

    void update_status(const Status& st) {
        std::lock_guard l(_mtx);
        _status.update(st);
    }

private:
    CompactionScheduler* _scheduler;
    mutable bthread::Mutex _mtx;
    const lake::CompactRequest* _request;
    lake::CompactResponse* _response;
    ::google::protobuf::Closure* _done;
    Status _status;
    std::vector<std::unique_ptr<CompactionTaskContext>> _contexts;
};

// Context of a single tablet compaction task.
struct CompactionTaskContext : public butil::LinkNode<CompactionTaskContext> {
    explicit CompactionTaskContext(int64_t txn_id_, int64_t tablet_id_, int64_t version_,
                                   std::shared_ptr<CompactionTaskCallback> cb_)
            : txn_id(txn_id_), tablet_id(tablet_id_), version(version_), callback(std::move(cb_)) {}

#ifndef NDEBUG
    ~CompactionTaskContext() {
        CHECK(next() == this && previous() == this) << "Must remove CompactionTaskContext from list before destructor";
    }
#endif

    const int64_t txn_id;
    const int64_t tablet_id;
    const int64_t version;
    std::atomic<int64_t> start_time{0};
    std::atomic<int64_t> finish_time{0};
    std::atomic<bool> skipped{false};
    std::atomic<int> runs{0};
    Status status;
    lake::CompactionTask::Progress progress;
    std::shared_ptr<CompactionTaskCallback> callback;
};

struct CompactionTaskInfo {
    int64_t txn_id;
    int64_t tablet_id;
    int64_t version;
    int64_t start_time;
    int64_t finish_time;
    Status status;
    int runs;     // How many times the compaction task has been executed
    int progress; // 0-100
    bool skipped;
};

class CompactionScheduler {
    // Limiter is used to control the maximum compaction concurrency based on the memory usage limitation:
    //  - The initial maximum compaction concurrency is config::compact_task_queue_count
    //  - Once Status::MemoryLimitExceeded is encountered, reduce the maximum concurrency by one until the
    //    concurrency is reduced to 1
    //  - If no Status::MemoryLimitExceeded is encountered for "kConcurrencyRestoreTimes" consecutive time,
    //    increase the maximum concurrency by one until config::compact_task_queue_count is reached or
    //    Status::MemoryLimitExceeded is encountered again.
    class Limiter {
    public:
        constexpr const static int16_t kConcurrencyRestoreTimes = 2;

        explicit Limiter(int16_t total) : _total(total), _free(total), _reserved(0), _success(0) {}

        // Acquire a token for doing compaction task. returns true on success and false otherwise.
        // No new compaction task should be scheduled to run if the method returned false.
        bool acquire();

        // Compaction task finished without Status::MemoryLimitExceeded error.
        void no_memory_limit_exceeded();

        // Compaction task finished with Status::MemoryLimitExceeded error.
        void memory_limit_exceeded();

        int16_t concurrency() const;

        void adapt_to_task_queue_count(int16_t new_val);

    private:
        mutable std::mutex _mtx;
        int16_t _total;
        // The number of tokens can be assigned to compaction tasks.
        int64_t _free;
        // The number of reserved tokens. reserved tokens cannot be assigned to compaction task.
        int16_t _reserved;
        // The number of tasks that didn't encounter the Status::MemoryLimitExceeded error.
        int64_t _success;
    };

public:
    explicit CompactionScheduler(TabletManager* tablet_mgr);

    ~CompactionScheduler();

    DISALLOW_COPY_AND_MOVE(CompactionScheduler);

    void compact(::google::protobuf::RpcController* controller, const CompactRequest* request,
                 CompactResponse* response, ::google::protobuf::Closure* done);

    // Finds all compaction tasks for the given |txn_id| and aborts them.
    // Marks the tasks as aborted and returns immediately, without waiting
    // for the tasks to exit.
    // Returns Status::NotFound if no task with the given txn_id is found,
    // otherwise returns Status::OK.
    Status abort(int64_t txn_id);

    void list_tasks(std::vector<CompactionTaskInfo>* infos);

    int16_t concurrency() const { return _limiter.concurrency(); }

    // update at runtime
    void update_compact_task_queue_count(int32_t new_val);

private:
    friend class CompactionTaskCallback;

    using CompactionContextPtr = std::unique_ptr<CompactionTaskContext>;

    // Using unbounded queue for simplicity and rely on the FE to limit the number of compaction tasks.
    //
    // To developers: if you change this queue to bounded queue, normally you should replace std::mutex
    // with bthread::Mutex and replace std::condition_variable with bthread::ConditionVariable:
    // ```
    // using TaskQueue = BlockingQueue<CompactionStatePtr,
    //                                 std::deque<CompactionStatePtr>,
    //                                 bthread::Mutex,
    //                                 bthread::ConditionVariable>;
    // ```
    using TaskQueue = UnboundedBlockingQueue<CompactionContextPtr>;

    void remove_states(const std::vector<std::unique_ptr<CompactionTaskContext>>& contexes);

    void thread_task(int id);

    Status do_compaction(std::unique_ptr<CompactionTaskContext> context);

    void steal_task(int start_index, std::unique_ptr<CompactionTaskContext>* context);

    int choose_task_queue_by_txn_id(int64_t txn_id) const { return txn_id % _task_queue_count.load(); }

    bool txn_log_exists(int64_t tablet_id, int64_t txn_id) const;

    TabletManager* _tablet_mgr;
    Limiter _limiter;
    bthread::Mutex _contexts_lock;
    butil::LinkedList<CompactionTaskContext> _contexts;

    std::atomic<int> _task_queue_count;
    // use a mutex to protect the resize operation of _task_queues at runtime
    std::mutex _task_queues_mutex;
    std::vector<TaskQueue> _task_queues;
    std::unique_ptr<ThreadPool> _threads;
    std::atomic<bool> _stopped{false};
};

inline bool CompactionScheduler::Limiter::acquire() {
    std::lock_guard l(_mtx);
    if (_free > 0) {
        _free--;
        return true;
    } else {
        return false;
    }
}

inline void CompactionScheduler::Limiter::no_memory_limit_exceeded() {
    std::lock_guard l(_mtx);
    _free++;
    if (_reserved > 0 && ++_success == kConcurrencyRestoreTimes) {
        --_reserved;
        ++_free;
        _success = 0;
        LOG(INFO) << "Increased maximum compaction concurrency to " << (_total - _reserved);
    }
}

inline void CompactionScheduler::Limiter::memory_limit_exceeded() {
    std::lock_guard l(_mtx);
    _success = 0;
    if (_reserved + 1 < _total) { // Cannot reduce the concurrency to zero.
        _reserved++;
        LOG(INFO) << "Decreased maximum compaction concurrency to " << (_total - _reserved);
    } else {
        _free++;
    }
}

inline int16_t CompactionScheduler::Limiter::concurrency() const {
    std::lock_guard l(_mtx);
    return _total - _reserved;
}

inline void CompactionScheduler::Limiter::adapt_to_task_queue_count(int16_t new_val) {
    std::lock_guard l(_mtx);
    if (new_val > _total) {
        auto diff = new_val - _total;
        _free += diff;
        _total += diff;
    } else {
        auto diff = _total - new_val;
        _total = new_val;
        // ensure `concurrency > 0`, which means `_total - _reserved > 0`
        if (_total <= _free) {
            _free = _total;
            _reserved = 0;
        } else if (_total < _reserved) {
            _reserved -= diff;
            if (_reserved < 0) {
                _reserved = 0;
            }
        }
    }
    LOG(INFO) << "Update Limiter's _total value to " << _total << ", _free value to "
              << _free << ", and _reserved value to " << _reserved;
}

} // namespace starrocks::lake
