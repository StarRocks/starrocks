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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/blocking_queue.hpp

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

#include <unistd.h>

#include <condition_variable>
#include <deque>
#include <list>
#include <mutex>

#include "util/stopwatch.hpp"

namespace starrocks {

class TimedConditionVariable {
public:
    void notify_one() noexcept { _cv.notify_one(); }

    void notify_all() noexcept { _cv.notify_all(); }

    void wait(std::unique_lock<std::mutex>& lock) {
        MonotonicStopWatch t;
        t.start();
        _cv.wait(lock);
        t.stop();
        _time += t.elapsed_time();
    }

    template <class Predicate>
    void wait(std::unique_lock<std::mutex>& lock, Predicate pred) {
        MonotonicStopWatch t;
        t.start();
        _cv.wait(lock, std::move(pred));
        t.stop();
        _time += t.elapsed_time();
    }

    template <class Rep, class Period>
    std::cv_status wait_for(std::unique_lock<std::mutex>& lock, const std::chrono::duration<Rep, Period>& rel_time) {
        MonotonicStopWatch t;
        t.start();
        std::cv_status r = _cv.wait_for(lock, rel_time);
        t.stop();
        _time += t.elapsed_time();
        return r;
    }

    template <class Rep, class Period, class Predicate>
    bool wait_for(std::unique_lock<std::mutex>& lock, const std::chrono::duration<Rep, Period>& rel_time,
                  Predicate pred) {
        MonotonicStopWatch t;
        t.start();
        bool r = _cv.wait_for(lock, rel_time, std::move(pred));
        t.stop();
        _time += t.elapsed_time();
        return r;
    }

    template <class Clock, class Duration>
    std::cv_status wait_until(std::unique_lock<std::mutex>& lock,
                              const std::chrono::time_point<Clock, Duration>& timeout_time) {
        MonotonicStopWatch t;
        t.start();
        std::cv_status r = _cv.wait_until(lock, timeout_time);
        t.stop();
        _time += t.elapsed_time();
        return r;
    }

    template <class Clock, class Duration, class Pred>
    bool wait_until(std::unique_lock<std::mutex>& lock, const std::chrono::time_point<Clock, Duration>& timeout_time,
                    Pred pred) {
        MonotonicStopWatch t;
        t.start();
        bool r = _cv.wait_until(lock, timeout_time, std::move(pred));
        t.stop();
        _time += t.elapsed_time();
        return r;
    }

    uint64_t time() const { return _time; }

private:
    uint64_t _time = 0;
    std::condition_variable _cv;
};

// An bounded blocking queue.
template <typename T, typename Container = std::deque<T>, typename Lock = std::mutex,
          typename CV = std::condition_variable>
class BlockingQueue {
public:
    explicit BlockingQueue(size_t capacity) : _capacity(capacity), _shutdown(false) {}
    ~BlockingQueue() { shutdown(); }

    // Return false iff empty *AND* has been shutdown.
    bool blocking_get(T* out) {
        std::unique_lock<Lock> l(_lock);
        _not_empty.wait(l, [this]() { return !_items.empty() || _shutdown; });
        if (!_items.empty()) {
            if constexpr (std::is_move_assignable<T>::value) {
                *out = std::move(_items.front());
            } else {
                *out = _items.front();
            }
            _items.pop_front();
            _not_full.notify_one();
            return true;
        }
        return false;
    }

    // Return 1 on success;
    // Return 0 on queue empty;
    // Return -1 on shutdown;
    int try_get(T* out) {
        std::unique_lock<Lock> l(_lock);
        if (!_items.empty()) {
            if constexpr (std::is_move_assignable<T>::value) {
                *out = std::move(_items.front());
            } else {
                *out = _items.front();
            }
            _items.pop_front();
            _not_full.notify_one();
            return 1;
        }
        return _shutdown ? -1 : 0;
    }

    // Return false iff this queue has been shutdown.
    bool blocking_put(const T& val) {
        std::unique_lock<Lock> l(_lock);
        _not_full.wait(l, [this]() { return _items.size() < _capacity || _shutdown; });
        if (!_shutdown) {
            _items.emplace_back(val);
            _not_empty.notify_one();
            return true;
        }
        return false;
    }

    // Return false iff this queue has been shutdown.
    std::enable_if_t<std::is_move_constructible_v<T>, bool> blocking_put(T&& val) {
        std::unique_lock<Lock> l(_lock);
        _not_full.wait(l, [this]() { return _items.size() < _capacity || _shutdown; });
        if (!_shutdown) {
            _items.emplace_back(std::move(val));
            _not_empty.notify_one();
            return true;
        }
        return false;
    }

    bool try_put(const T& val) {
        std::unique_lock<Lock> l(_lock);
        if (_items.size() >= _capacity || _shutdown) {
            return false;
        }
        _items.emplace_back(val);
        _not_empty.notify_one();
        return true;
    }

    // Shutdown the queue, this will wake up all waiting threads.
    void shutdown() {
        std::lock_guard<Lock> guard(_lock);
        if (_shutdown) return;
        _shutdown = true;
        _not_empty.notify_all();
        _not_full.notify_all();
    }

    size_t capacity() const { return _capacity; }

    size_t get_size() const {
        std::lock_guard<Lock> l(_lock);
        return _items.size();
    }

    bool empty() const {
        std::lock_guard<Lock> l(_lock);
        return _items.empty();
    }

protected:
    const size_t _capacity;

    mutable Lock _lock;
    CV _not_empty;
    CV _not_full;
    Container _items;
    bool _shutdown;
};

// An un-bounded blocking queue.
template <typename T, typename Container = std::deque<T>, typename Lock = std::mutex,
          typename CV = std::condition_variable>
class UnboundedBlockingQueue {
public:
    UnboundedBlockingQueue() {}
    ~UnboundedBlockingQueue() { shutdown(); }

    // Return false iff empty *AND* has been shutdown.
    bool blocking_get(T* out) {
        std::unique_lock<Lock> l(_lock);
        _not_empty.wait(l, [this]() { return !_items.empty() || _shutdown; });
        if (!_items.empty()) {
            if constexpr (std::is_move_assignable<T>::value) {
                *out = std::move(_items.front());
            } else {
                *out = _items.front();
            }
            _items.pop_front();
            return true;
        }
        return false;
    }

    bool try_get(T* out) {
        std::unique_lock<Lock> l(_lock);
        if (!_items.empty()) {
            if constexpr (std::is_move_assignable<T>::value) {
                *out = std::move(_items.front());
            } else {
                *out = _items.front();
            }
            _items.pop_front();
            return true;
        }
        return false;
    }

    // Return false iff this queue has been shutdown.
    bool put(const T& val) {
        std::unique_lock<Lock> l(_lock);
        if (!_shutdown) {
            _items.emplace_back(val);
            l.unlock();
            _not_empty.notify_one();
            return true;
        }
        return false;
    }

    // Return false iff this queue has been shutdown.
    bool put(T&& val) {
        std::unique_lock<Lock> l(_lock);
        if (!_shutdown) {
            _items.emplace_back(std::move(val));
            l.unlock();
            _not_empty.notify_one();
            return true;
        }
        return false;
    }

    // Shutdown the queue, this will wake up all waiting threads.
    void shutdown() {
        std::lock_guard<Lock> guard(_lock);
        if (_shutdown) return;
        _shutdown = true;
        _not_empty.notify_all();
    }

    size_t get_size() const {
        std::lock_guard<Lock> l(_lock);
        return _items.size();
    }

    bool empty() const {
        std::lock_guard<Lock> l(_lock);
        return _items.empty();
    }

    void clear() {
        std::lock_guard<Lock> l(_lock);
        _items.clear();
    }

protected:
    mutable Lock _lock;
    CV _not_empty;
    Container _items;
    bool _shutdown{false};
};

template <typename T>
class TimedBlockingQueue : public BlockingQueue<T, std::list<T>, std::mutex, TimedConditionVariable> {
    using Base = BlockingQueue<T, std::list<T>, std::mutex, TimedConditionVariable>;

public:
    explicit TimedBlockingQueue(size_t capacity) : Base(capacity) {}

    // Returns the total amount of time threads have blocked in `blocking_get`.
    uint64_t total_get_wait_time() const {
        std::lock_guard<std::mutex> guard(this->_lock);
        return this->_not_empty.time();
    }

    // Returns the total amount of time threads have blocked in `blocking_put`.
    uint64_t total_put_wait_time() const {
        std::lock_guard<std::mutex> guard(this->_lock);
        return this->_not_full.time();
    }
};

} // namespace starrocks
