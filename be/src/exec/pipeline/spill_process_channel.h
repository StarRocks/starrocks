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

#include <atomic>
#include <functional>
#include <memory>
#include <utility>

// On macOS, system headers may define a macro named current_task(),
// which conflicts with the method name below. Undefine to avoid collisions.
#ifdef __APPLE__
#ifdef current_task
#undef current_task
#endif
#endif

#include "base/concurrency/blocking_queue.hpp"
#include "base/utility/defer_op.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "compute_env/spill/spiller.h"
#include "runtime/runtime_fwd.h"

namespace starrocks {
class SpillProcessChannel;
class SpillProcessTasksBuilder;
namespace spill {
class Spiller;
}

class SpillProcessTask {
public:
    SpillProcessTask() = default;

    SpillProcessTask(std::function<StatusOr<ChunkPtr>()> task) : _task(std::move(task)) {}

    StatusOr<ChunkPtr> operator()() const { return _task(); }

    operator bool() const { return !!_task; }

    SpillProcessTask& operator=(std::function<StatusOr<ChunkPtr>()> task) {
        _task = std::move(task);
        return *this;
    }

    void reset();

private:
    std::function<StatusOr<ChunkPtr>()> _task;
};

using SpillProcessChannelPtr = std::shared_ptr<SpillProcessChannel>;

class SpillProcessChannelFactory {
public:
    //
    SpillProcessChannelFactory(size_t degree) : _channels(degree) {}

    SpillProcessChannelPtr get_or_create(int32_t sequence);

private:
    std::vector<SpillProcessChannelPtr> _channels;
};
using SpillProcessChannelFactoryPtr = std::shared_ptr<SpillProcessChannelFactory>;

// SpillProcessOperator
class SpillProcessChannel {
public:
    SpillProcessChannel() = default;

    // add_spill_task / add_last_task are called from the writer pipeline thread. They bump _task_count
    // (queued + current) before the new task becomes visible in the queue, so the source wakeup
    // happens-after the enqueue and the woken pump sees the task. add_last_task also sets the terminal flag
    // here, so the enqueue and the finishing flag become visible together, before the notify. These public
    // wrappers take the channel lock, mutate through the *_locked helpers, copy the spiller, and notify the
    // source list outside the lock. They are not reentrant: a caller that already holds _mutex (execute())
    // must call the *_locked helpers directly -- the public add_*/execute/close are never called from under
    // _mutex.
    bool add_spill_task(SpillProcessTask&& task);

    bool add_last_task(SpillProcessTask&& task);

    // Non-blocking. has_output() gates the pump, so the pump's pull_chunk only runs when a task is
    // available, and an empty queue is only transient.
    bool acquire_spill_task() { return _spill_tasks.try_get(&_current_task); }

    bool has_spill_task() { return !_spill_tasks.empty(); }

    // The terminal handshake. External callers (operator set_finishing paths) do not hold the channel lock,
    // and close() may race a _spiller.reset(). So take the lock to copy the spiller and publish the flag,
    // then notify outside the lock (try_schedule re-evaluates predicates; keeping the channel lock off the
    // notify path keeps the lock-inversion surface small).
    void set_finishing() {
        std::shared_ptr<spill::Spiller> spiller_copy;
        {
            std::lock_guard guard(_mutex);
            _is_finishing.store(true);
            spiller_copy = _spiller;
        }
        if (spiller_copy != nullptr) {
            spiller_copy->notify_source_observers();
        }
    }

    bool is_finishing() { return _is_finishing.load(); }
    bool is_working() { return _is_working.load(); }

    SpillProcessTask& current_task() { return _current_task; }

    // Called by the spill-process source after a task drains (hits EOF): drop it from the count and wake the
    // sink list (the writer is blocked on has_task()). Runs on the pump thread without the channel lock.
    void on_current_task_finished() {
        _current_task.reset();
        _task_count.fetch_sub(1);
        if (_spiller != nullptr) {
            _spiller->notify_sink_observers();
        }
    }

    bool has_output() {
        if (is_finished()) {
            return false;
        }
        // Check _spiller for null explicitly, instead of assuming has_task() implies a live spiller:
        // close() may reset _spiller, and the other accessors (on_current_task_finished, set_finishing)
        // already null-check it. Do the deref after an explicit check, do not rely on && evaluation order.
        if (_spiller == nullptr) {
            return false;
        }
        return has_task() && !_spiller->is_full();
    }

    // queued + current, read as a single atomic, so has_task() stays consistent with the queue and
    // _current_task under one-shot notify.
    bool has_task() { return _task_count.load() > 0; }

    bool is_finished() { return is_finishing() && !has_task(); }

    void set_spiller(std::shared_ptr<spill::Spiller> spiller) { _spiller = std::move(spiller); }
    const std::shared_ptr<spill::Spiller>& spiller() { return _spiller; }

    Status execute(SpillProcessTasksBuilder& task_builder);

    void close();

private:
    // Mutation half of add_spill_task / add_last_task: enqueue and bump the count (add_last_task also
    // publishes the terminal flag) under an already-held _mutex, with no notify. execute() holds _mutex for
    // its whole queueing branch and calls these directly (calling the public wrappers would re-enter the
    // non-reentrant _mutex and deadlock); the public wrappers wrap these in lock + post-lock notify.
    bool _add_spill_task_locked(SpillProcessTask&& task);
    bool _add_last_task_locked(SpillProcessTask&& task);

    std::atomic_bool _is_finishing = false;
    std::atomic_bool _is_working = false;
    bool _is_closed = false;
    std::shared_ptr<spill::Spiller> _spiller;
    UnboundedBlockingQueue<SpillProcessTask> _spill_tasks;
    SpillProcessTask _current_task;
    // queued spill tasks + the one currently pulled into _current_task.
    std::atomic<int32_t> _task_count{0};
    std::mutex _mutex;
};

class SpillProcessTasksBuilder {
public:
    SpillProcessTasksBuilder(RuntimeState* state) : _runtime_state(state) {}

    template <class Task>
    SpillProcessTasksBuilder& then(Task task) {
        auto runtime_state = this->_runtime_state;
        std::function<StatusOr<ChunkPtr>()> spill_task = [runtime_state, task]() -> StatusOr<ChunkPtr> {
            RETURN_IF_ERROR(task(runtime_state));
            return Status::EndOfFile("eos");
        };
        _spill_tasks.emplace_back(std::move(spill_task));
        return *this;
    }

    template <class Task>
    void finally(Task task) {
        auto runtime_state = this->_runtime_state;
        _final_task = [runtime_state, task]() -> StatusOr<ChunkPtr> {
            RETURN_IF_ERROR(task(runtime_state));
            return Status::EndOfFile("eos");
        };
    }

    std::vector<SpillProcessTask>& tasks() { return _spill_tasks; }
    SpillProcessTask& final_task() { return _final_task; }

private:
    RuntimeState* _runtime_state;
    std::vector<SpillProcessTask> _spill_tasks;
    SpillProcessTask _final_task;
};

} // namespace starrocks
