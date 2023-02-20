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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/blocking_priority_queue.hpp

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
#include <condition_variable>
#include <mutex>
#include <vector>

#include "common/config.h"
#include "gutil/port.h"

namespace starrocks {

// Fixed capacity FIFO queue, where both blocking_get and blocking_put operations block
// if the queue is empty or full, respectively.
template <typename T>
class BlockingPriorityQueue {
public:
    explicit BlockingPriorityQueue(size_t max_elements) : _max_element(max_elements) {}
    ~BlockingPriorityQueue() { shutdown(); }

    // Return false iff has been shutdown.
    bool blocking_get(T* out) {
        std::unique_lock<std::mutex> unique_lock(_lock);
        _get_cv.wait(unique_lock, [this]() { return !_heap.empty() || _shutdown; });
        if (!_heap.empty()) {
            _adjust_priority_if_needed();
            std::pop_heap(_heap.begin(), _heap.end());
            if constexpr (std::is_move_assignable_v<T>) {
                *out = std::move(_heap.back());
            } else {
                *out = _heap.back();
            }
            _heap.pop_back();
            ++_upgrade_counter;
            unique_lock.unlock();
            _put_cv.notify_one();
            return true;
        }
        return false;
    }

    // Return false iff has been shutdown or empty.
    bool non_blocking_get(T* out) {
        std::unique_lock<std::mutex> unique_lock(_lock);
        if (!_heap.empty()) {
            _adjust_priority_if_needed();
            std::pop_heap(_heap.begin(), _heap.end());
            if constexpr (std::is_move_assignable_v<T>) {
                *out = std::move(_heap.back());
            } else {
                *out = _heap.back();
            }
            _heap.pop_back();
            ++_upgrade_counter;
            unique_lock.unlock();
            _put_cv.notify_one();
            return true;
        }
        return false;
    }

    // Return false iff has been shutdown.
    bool blocking_put(const T& val) {
        std::unique_lock<std::mutex> unique_lock(_lock);
        _put_cv.wait(unique_lock, [this]() { return _heap.size() < _max_element || _shutdown; });
        if (!_shutdown) {
            DCHECK_LT(_heap.size(), _max_element);
            _heap.emplace_back(val);
            std::push_heap(_heap.begin(), _heap.end());
            unique_lock.unlock();
            _get_cv.notify_one();
            return true;
        }
        return false;
    }

    // Return false iff has been shutdown.
    bool blocking_put(T&& val) {
        std::unique_lock<std::mutex> unique_lock(_lock);
        _put_cv.wait(unique_lock, [this]() { return _heap.size() < _max_element || _shutdown; });
        if (!_shutdown) {
            DCHECK_LT(_heap.size(), _max_element);
            _heap.emplace_back(std::move(val));
            std::push_heap(_heap.begin(), _heap.end());
            unique_lock.unlock();
            _get_cv.notify_one();
            return true;
        }
        return false;
    }

    // Return false if queue full or has been shutdown.
    bool try_put(const T& val) {
        std::unique_lock<std::mutex> unique_lock(_lock);
        if (_heap.size() < _max_element && !_shutdown) {
            _heap.emplace_back(val);
            std::push_heap(_heap.begin(), _heap.end());
            unique_lock.unlock();
            _get_cv.notify_one();
            return true;
        }
        return false;
    }

    // Return false if queue full or has been shutdown.
    bool try_put(T&& val) {
        std::unique_lock<std::mutex> unique_lock(_lock);
        if (_heap.size() < _max_element && !_shutdown) {
            _heap.emplace_back(std::move(val));
            std::push_heap(_heap.begin(), _heap.end());
            unique_lock.unlock();
            _get_cv.notify_one();
            return true;
        }
        return false;
    }

    // Shut down the queue. Wakes up all threads waiting on blocking_get or blocking_put.
    void shutdown() {
        {
            std::lock_guard<std::mutex> guard(_lock);
            if (_shutdown) return;
            _shutdown = true;
        }

        _get_cv.notify_all();
        _put_cv.notify_all();
    }

    size_t get_capacity() const { return _max_element; }
    uint32_t get_size() const {
        std::unique_lock<std::mutex> l(_lock);
        return _heap.size();
    }

private:
    // REQUIRES: _lock has been acquired.
    ALWAYS_INLINE void _adjust_priority_if_needed() {
        if (_upgrade_counter <= config::priority_queue_remaining_tasks_increased_frequency) {
            return;
        }
        const size_t n = _heap.size();
        for (size_t i = 0; i < n; i++) {
            ++_heap[i];
        }
        std::make_heap(_heap.begin(), _heap.end());
        _upgrade_counter = 0;
    }

    const int _max_element;
    mutable std::mutex _lock;
    std::condition_variable _get_cv; // 'get' callers wait on this
    std::condition_variable _put_cv; // 'put' callers wait on this
    std::vector<T> _heap;
    int _upgrade_counter = 0;
    bool _shutdown = false;
};

} // namespace starrocks
