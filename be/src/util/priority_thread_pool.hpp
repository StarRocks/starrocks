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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/priority_thread_pool.hpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <algorithm>
#include <atomic>
#include <boost/thread.hpp>
#include <functional>

#include "common/logging.h"
#include "util/blocking_priority_queue.hpp"
#include "util/thread.h"

namespace starrocks {

// Simple threadpool which processes items (of type T) in parallel which were placed on a
// blocking queue by Offer(). Each item is processed by a single user-supplied method.
class PriorityThreadPool {
public:
    // Signature of a work-processing function. Takes the integer id of the thread which is
    // calling it (ids run from 0 to num_threads - 1) and a reference to the item to
    // process.
    typedef std::function<void()> WorkFunction;

    struct Task {
    public:
        int priority = 0;
        WorkFunction work_function;
        bool operator<(const Task& o) const { return priority < o.priority; }

        Task& operator++() {
            priority += 2;
            return *this;
        }
    };

    // Creates a new thread pool and start num_threads threads.
    //  -- num_threads: how many threads are part of this pool
    //  -- queue_size: the maximum size of the queue on which work items are offered. If the
    //     queue exceeds this size, subsequent calls to Offer will block until there is
    //     capacity available.
    //  -- work_function: the function to run every time an item is consumed from the queue
    PriorityThreadPool(std::string name, uint32_t num_threads, uint32_t queue_size)
            : _name(std::move(name)), _work_queue(queue_size), _shutdown(false) {
        for (int i = 0; i < num_threads; ++i) {
            new_thread(++_current_thread_id);
        }
    }

    // Destructor ensures that all threads are terminated before this object is freed
    // (otherwise they may continue to run and reference member variables)
    ~PriorityThreadPool() noexcept {
        shutdown();
        join();
    }

    // Blocking operation that puts a work item on the queue. If the queue is full, blocks
    // until there is capacity available.
    //
    // 'work' is copied into the work queue, but may be referenced at any time in the
    // future. Therefore the caller needs to ensure that any data referenced by work (if T
    // is, e.g., a pointer type) remains valid until work has been processed, and it's up to
    // the caller to provide their own signalling mechanism to detect this (or to wait until
    // after DrainAndshutdown returns).
    //
    // Returns true if the work item was successfully added to the queue, false otherwise
    // (which typically means that the thread pool has already been shut down).
    bool offer(const Task& task) { return _work_queue.blocking_put(task); }

    bool try_offer(const Task& task) { return _work_queue.try_put(task); }

    bool offer(WorkFunction func) {
        PriorityThreadPool::Task task = {0, std::move(func)};
        return _work_queue.blocking_put(std::move(task));
    }

    bool try_offer(WorkFunction func) {
        PriorityThreadPool::Task task = {0, std::move(func)};
        return _work_queue.try_put(std::move(task));
    }

    // Shuts the thread pool down, causing the work queue to cease accepting offered work
    // and the worker threads to terminate once they have processed their current work item.
    // Returns once the shutdown flag has been set, does not wait for the threads to
    // terminate.
    void shutdown() {
        {
            std::lock_guard<std::mutex> l(_lock);
            _shutdown = true;
        }
        _work_queue.shutdown();
    }

    // Blocks until all threads are finished. shutdown does not need to have been called,
    // since it may be called on a separate thread.
    void join() { _threads.join_all(); }

    size_t get_queue_capacity() const { return _work_queue.get_capacity(); }

    uint32_t get_queue_size() const { return _work_queue.get_size(); }

    // Blocks until the work queue is empty, and then calls shutdown to stop the worker
    // threads and Join to wait until they are finished.
    // Any work Offer()'ed during DrainAndshutdown may or may not be processed.
    void drain_and_shutdown() {
        {
            std::unique_lock<std::mutex> l(_lock);
            while (_work_queue.get_size() != 0) {
                _empty_cv.wait(l);
            }
        }
        shutdown();
        join();
    }

    void set_num_thread(int num_thread) {
        size_t num_thread_in_pool = _threads.size();
        if (num_thread > num_thread_in_pool) {
            increase_thr(num_thread - num_thread_in_pool);
        } else if (num_thread < num_thread_in_pool) {
            decrease_thr(num_thread_in_pool - num_thread);
        }
    }

private:
    void increase_thr(int num_thread) {
        std::lock_guard<std::mutex> l(_lock);
        for (int i = 0; i < num_thread; ++i) {
            new_thread(++_current_thread_id);
        }
    }

    void decrease_thr(int num_thread) {
        _should_decrease += num_thread;

        for (int i = 0; i < num_thread; ++i) {
            PriorityThreadPool::Task empty_task = {0, []() {}};
            _work_queue.try_put(empty_task);
        }
    }

    // not thread safe
    // we need acquire _lock before call this function
    void new_thread(int tid) {
        auto* thr = _threads.create_thread(std::bind<void>(std::mem_fn(&PriorityThreadPool::work_thread), this, tid));
        Thread::set_thread_name(thr->native_handle(), _name);
        _threads_holder.emplace_back(thr, tid);
    }

    void remove_thread(int tid) {
        std::lock_guard<std::mutex> l(_lock);
        auto res = std::find_if(_threads_holder.begin(), _threads_holder.end(),
                                [=](const auto& val) { return tid == val.second; });
        if (res != _threads_holder.end()) {
            _threads.remove_thread(res->first);
            _threads_holder.erase(res);
        }
    }

    // Driver method for each thread in the pool. Continues to read work from the queue
    // until the pool is shutdown.
    void work_thread(int thread_id) {
        while (!is_shutdown()) {
            Task task;
            if (_work_queue.blocking_get(&task)) {
                task.work_function();
            }
            if (_work_queue.get_size() == 0) {
                _empty_cv.notify_all();
            }
            if (_should_decrease) {
                bool need_destroy = true;
                int32_t expect;
                int32_t target;
                do {
                    expect = _should_decrease;
                    target = expect - 1;
                    if (expect == 0) {
                        need_destroy = false;
                        break;
                    }
                } while (!_should_decrease.compare_exchange_weak(expect, target));
                if (need_destroy) {
                    remove_thread(thread_id);
                    break;
                }
            }
        }
    }

    // Returns value of _shutdown under a lock, forcing visibility to threads in the pool.
    bool is_shutdown() {
        std::lock_guard<std::mutex> l(_lock);
        return _shutdown;
    }

    const std::string _name;

    // thread pointer
    // tid
    std::vector<std::pair<boost::thread*, int>> _threads_holder;

    // Queue on which work items are held until a thread is available to process them in
    // FIFO order.
    BlockingPriorityQueue<Task> _work_queue;

    // Collection of worker threads that process work from the queue.
    boost::thread_group _threads;

    // Guards _shutdown and _empty_cv
    std::mutex _lock;

    // Set to true when threads should stop doing work and terminate.
    bool _shutdown;

    // Signalled when the queue becomes empty
    std::condition_variable _empty_cv;

    std::atomic<int32_t> _should_decrease = 0;

    std::atomic<int32_t> _current_thread_id = 0;
};

} // namespace starrocks
