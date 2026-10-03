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

#include "exec/pipeline/spill_process_channel.h"

#include "compute_env/spill/spiller.h"
#include "exec/pipeline/context_with_dependency.h"

namespace starrocks {

bool SpillProcessChannel::_add_spill_task_locked(SpillProcessTask&& task) {
    // _mutex is held by the caller. Mutate only; the notify is the caller's job
    // (the public wrapper after it releases the lock, or execute() after its own
    // single lock). The channel lock orders enqueues against close(): a concurrent
    // close drains the queue and zeroes the count under the same lock, so an
    // enqueue cannot slip between the drain and the zeroing (a count with no
    // consumer would park a sink on WAIT_CHANNEL forever).
    if (_is_closed) {
        // The channel is already torn down (cancel path); the task is dropped,
        // nothing was published.
        return false;
    }
    DCHECK(!_is_finishing.load());
    _is_working.store(true);
    _task_count.fetch_add(1);
    if (!_spill_tasks.put(std::move(task))) {
        _task_count.fetch_sub(1);
        return false;
    }
    return true;
}

bool SpillProcessChannel::_add_last_task_locked(SpillProcessTask&& task) {
    // _mutex is held by the caller; mutate only, no notify (see _add_spill_task_locked).
    if (_is_closed) {
        return false;
    }
    DCHECK(!_is_finishing.load());
    _is_working.store(true);
    _task_count.fetch_add(1);
    if (!_spill_tasks.put(std::move(task))) {
        _task_count.fetch_sub(1);
        return false;
    }
    // Terminal flag is published under the lock so a source woken by the post-lock
    // notify observes is_finishing.
    _is_finishing.store(true);
    return true;
}

bool SpillProcessChannel::add_spill_task(SpillProcessTask&& task) {
    std::shared_ptr<spill::Spiller> spiller_copy;
    bool ok;
    {
        std::lock_guard guard(_mutex);
        ok = _add_spill_task_locked(std::move(task));
        if (!ok) {
            return false;
        }
        spiller_copy = this->spiller();
    }
    // Callers enqueue directly (aggregator, sorter, hash joiner) without going
    // through execute(), so the enqueue itself must wake the spill-process
    // source: it sleeps on an empty channel and no spiller IO exists yet to
    // wake it. Count and task are published before the notify; the notify runs
    // outside the lock (try_schedule re-evaluates predicates).
    if (spiller_copy != nullptr) {
        spiller_copy->notify_source_observers();
    }
    return true;
}

bool SpillProcessChannel::add_last_task(SpillProcessTask&& task) {
    std::shared_ptr<spill::Spiller> spiller_copy;
    bool ok;
    {
        std::lock_guard guard(_mutex);
        ok = _add_last_task_locked(std::move(task));
        if (!ok) {
            return false;
        }
        spiller_copy = this->spiller();
    }
    if (spiller_copy != nullptr) {
        spiller_copy->notify_source_observers();
    }
    return true;
}

void SpillProcessTask::reset() {
    _task = {};
}

SpillProcessChannelPtr SpillProcessChannelFactory::get_or_create(int32_t sequence) {
    DCHECK_LT(sequence, _channels.size());
    if (_channels[sequence] == nullptr) {
        _channels[sequence] = std::make_shared<SpillProcessChannel>();
    }
    return _channels[sequence];
}

void SpillProcessChannel::set_guarded_context(pipeline::ContextWithDependency* context) {
    std::lock_guard guard(_mutex);
    if (_is_closed) {
        return;
    }
    DCHECK(_guarded_context == nullptr);
    DCHECK(context != nullptr);
    context->ref();
    _guarded_context = context;
}

std::shared_ptr<spill::Spiller> SpillProcessChannel::spiller() const {
    // Event callbacks inspect the spiller, including from tasks run under the channel mutex.
    // Pin its lifetime without acquiring that mutex or racing delayed producer initialization.
    return std::atomic_load_explicit(&_spiller, std::memory_order_acquire);
}

void SpillProcessChannel::set_spiller(std::shared_ptr<spill::Spiller> spiller) {
    RuntimeState* state = nullptr;
    pipeline::PipelineObserver* observer = nullptr;
    {
        std::lock_guard guard(_mutex);
        if (_is_closed) {
            return;
        }
        DCHECK(this->spiller() == nullptr);
        std::atomic_store_explicit(&_spiller, spiller, std::memory_order_release);
        state = _source_state;
        observer = _source_observer;
    }
    // Do not nest the channel mutex and observable lock: a notification can inspect this channel.
    // The producer cannot enqueue work until set_spiller() and its operator preparation return.
    if (state != nullptr) {
        spiller->observable().subscribe_source(state, observer);
    }
}

void SpillProcessChannel::prepare_source(RuntimeState* state, pipeline::PipelineObserver* observer) {
    std::shared_ptr<spill::Spiller> spiller_copy;
    {
        std::lock_guard guard(_mutex);
        if (_is_closed) {
            return;
        }
        DCHECK(_source_state == nullptr);
        _source_state = state;
        _source_observer = observer;
        spiller_copy = this->spiller();
    }
    if (spiller_copy != nullptr) {
        spiller_copy->observable().subscribe_source(state, observer);
    }
}

Status SpillProcessChannel::execute(SpillProcessTasksBuilder& task_builder) {
    Status res;
    std::shared_ptr<spill::Spiller> notify_spiller;
    {
        std::lock_guard guard(_mutex);
        if (_is_closed) {
            // Inline branch: the channel is already torn down, run the final
            // task synchronously on this thread. No queueing, no notify.
            auto st = task_builder.final_task()();
            if (!st.status().is_ok_or_eof()) {
                res = st.status();
            }
        } else if (is_working()) {
            // We already hold _mutex; call the *_locked mutators directly. Calling
            // the public add_*/add_last_task here would re-enter the non-reentrant
            // _mutex and deadlock. The single source notify fires once below, after
            // the lock is released.
            for (auto&& task : task_builder.tasks()) {
                _add_spill_task_locked(std::move(task));
            }
            _add_last_task_locked(std::move(task_builder.final_task()));
            // Enqueue (and the terminal flag) are now published under the lock;
            // wake the pump via a spiller copy, notifying outside the lock.
            notify_spiller = spiller();
        } else {
            // Inline branch: not working yet, run tasks synchronously. No
            // queueing, no notify.
            for (auto& task : task_builder.tasks()) {
                auto st = task();
                if (!st.status().is_ok_or_eof()) {
                    res = st.status();
                    break;
                }
            }
            auto st = task_builder.final_task()();
            if (!st.status().is_ok_or_eof()) {
                res = st.status();
            }
        }
    }
    // The writer enqueued tasks; wake source-side sleepers (the pump's
    // has_output reads has_task()).
    if (notify_spiller != nullptr) {
        notify_spiller->notify_source_observers();
    }
    return res;
}

void SpillProcessChannel::close(RuntimeState* state) {
    pipeline::ContextWithDependency* context = nullptr;
    std::shared_ptr<spill::Spiller> spiller_copy;
    {
        std::lock_guard guard(_mutex);
        if (_is_closed) {
            return;
        }

        // Drain every queued task under the lock. Popping leaves the last task in `task` for the
        // orderly-finishing handoff below; the earlier tasks are released as `task` is overwritten, running
        // their destructors so the operator and spiller they captured are freed even on a cancel close.
        SpillProcessTask task;
        while (!_spill_tasks.empty()) {
            if (_spill_tasks.try_get(&task)) {
                _task_count.fetch_sub(1);
            }
        }

        // Run the last task only on the orderly finishing path; on a cancel close there is no terminal
        // handoff to perform.
        if (_is_finishing && task) {
            (void)task();
        }
        // Account for any task still pinned in _current_task as well; the
        // channel is closing, so the aggregate must land at zero. This must
        // happen on the cancel path too: a sink driver in another pipeline
        // parks on has_task() through its WAIT_CHANNEL block, and nobody else
        // clears the count once this source is torn down.
        _current_task.reset();
        _task_count.store(0);

        spiller_copy =
                std::atomic_exchange_explicit(&_spiller, std::shared_ptr<spill::Spiller>{}, std::memory_order_acq_rel);
        _is_closed = true;
        context = std::exchange(_guarded_context, nullptr);
    }
    // Wake the sink-side waiters outside the lock: the count is already
    // published at zero, so an OUTPUT_FULL driver woken here observes
    // has_task() false and resumes.
    if (spiller_copy != nullptr) {
        spiller_copy->notify_sink_observers();
        spiller_copy->notify_source_observers();
    }
    // Context close may free the aggregator/join state and reenter other spill paths.
    // All task captures have been released, and the channel lock must not be held here.
    if (context != nullptr) {
        context->unref(state);
    }
}

} // namespace starrocks