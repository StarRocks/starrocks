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

#include <mutex>
#include <unordered_map>

#include "testutil/sync_point.h"
#include "util/bthreads/future.h"

namespace starrocks::bthreads::singleflight {

// Inspired by Golang's SingleFlight: https://github.com/golang/sync/blob/master/singleflight/singleflight.go
template <typename K, typename R>
class Group {
public:
    // Do executes and returns the results of the given function, making
    // sure that only one execution is in-flight for a given key at a
    // time. If a duplicate comes in, the duplicate caller waits for the
    // original to complete and receives the same results.
    template <typename Func, typename... Args>
    R Do(K key, Func&& func, Args&&... args) {
        auto f = DoFuture(key, std::forward<Func>(func), std::forward<Args>(args)...);
        return f.get();
    }

    template <typename Func, typename... Args>
    SharedFuture<R> DoFuture(K key, Func&& func, Args&&... args) {
        std::unique_lock lock(_doing_mtx);

        auto it = _doing.find(key);
        if (it != _doing.end()) {
            auto f = it->second;
            lock.unlock();
            TEST_SYNC_POINT("singleflight::Group::Do:1");
            return f;
        }

        auto promise = bthreads::Promise<R>();
        auto future = promise.get_future().share();
        _doing.emplace(key, future);
        lock.unlock();

        TEST_SYNC_POINT("singleflight::Group::Do:2");
        try {
            promise.set_value(func(std::forward<Args>(args)...));
        } catch (...) {
            promise.set_exception(std::current_exception());
        }
        TEST_SYNC_POINT("singleflight::Group::Do:3");

        lock.lock();
        it = _doing.find(key);
        if (it != _doing.end() && it->second == future) {
            _doing.erase(it);
        }
        lock.unlock();

        return future;
    }

    // Forget tells the singleflight to forget about a key.  Future calls
    // to Do for this key will call the function rather than waiting for
    // an earlier call to complete.
    void Forget(K key) {
        TEST_SYNC_POINT("singleflight::Group::Forget:1");
        {
            std::lock_guard l(_doing_mtx);
            _doing.erase(key);
        }
        TEST_SYNC_POINT("singleflight::Group::Forget:2");
    }

private:
    bthread::Mutex _doing_mtx;
    std::unordered_map<K, SharedFuture<R>> _doing;
};

} // namespace starrocks::bthreads::singleflight
