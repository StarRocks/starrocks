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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/countdown_latch.h

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

#include <condition_variable>
#include <mutex>

#include "common/logging.h"
#include "storage/olap_define.h"
#include "util/monotime.h"

namespace starrocks {

// This is a C++ implementation of the Java CountDownLatch
// class.
// See http://docs.oracle.com/javase/6/docs/api/java/util/concurrent/CountDownLatch.html
template <typename Lock, typename Cond>
class GenericCountDownLatch {
public:
    // Initialize the latch with the given initial count.
    explicit GenericCountDownLatch(int count) : count_(count) {}

    // REQUIRES: amount >= 0.
    // Decrement the count of this latch by 'amount'.
    // If the new count is less than or equal to zero, then all waiting threads are woken up.
    // If the count is already zero, this has no effect.
    void count_down(int amount) {
        DCHECK_GE(amount, 0);
        std::lock_guard lock(lock_);
        if (count_ == 0) {
            return;
        }

        if (amount >= count_) {
            count_ = 0;
        } else {
            count_ -= amount;
        }

        if (count_ == 0) {
            // Latch has triggered.
            cond_.notify_all();
        }
    }

    // Decrement the count of this latch.
    // If the new count is zero, then all waiting threads are woken up.
    // If the count is already zero, this has no effect.
    void count_down() { count_down(1); }

    // Wait until the count on the latch reaches zero.
    // If the count is already zero, this returns immediately.
    void wait() const {
        std::unique_lock lock(lock_);
        while (count_ > 0) {
            cond_.wait(lock);
        }
    }

    template <typename Duration>
    bool wait_for(const Duration& dur) const {
        std::unique_lock lock(lock_);
        return cond_.wait_for(lock, dur, [&]() { return count_ <= 0; });
    }

    // Waits for the count on the latch to reach zero, or until 'delta' time elapses.
    // Returns true if the count became zero, false otherwise.
    bool wait_for(const MonoDelta& delta) const { return wait_for(std::chrono::nanoseconds(delta.ToNanoseconds())); }

    // Reset the latch with the given count. This is equivalent to reconstructing
    // the latch. If 'count' is 0, and there are currently waiters, those waiters
    // will be triggered as if you counted down to 0.
    void reset(uint64_t count) {
        std::lock_guard lock(lock_);
        count_ = count;
        if (count_ == 0) {
            // Awake any waiters if we reset to 0.
            cond_.notify_all();
        }
    }

    uint64_t count() const {
        std::lock_guard lock(lock_);
        return count_;
    }

private:
    mutable Lock lock_;
    mutable Cond cond_;

    int64_t count_;
    GenericCountDownLatch(const GenericCountDownLatch&) = delete;
    const GenericCountDownLatch& operator=(const GenericCountDownLatch&) = delete;
};

using CountDownLatch = GenericCountDownLatch<std::mutex, std::condition_variable>;

// Utility class which calls latch->CountDown() in its destructor.
template <typename T>
class CountDownOnScopeExit {
public:
    explicit CountDownOnScopeExit(T* latch) : latch_(latch) {}
    ~CountDownOnScopeExit() { latch_->count_down(); }

private:
    CountDownOnScopeExit(const CountDownOnScopeExit&) = delete;
    const CountDownOnScopeExit& operator=(const CountDownOnScopeExit&) = delete;

    T* latch_;
};

} // namespace starrocks
