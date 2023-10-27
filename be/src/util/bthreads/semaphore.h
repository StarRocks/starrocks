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

#include <bthread/butex.h>
#include <bthread/processor.h> // cpu_relax()

#include <chrono>
#include <climits>

#include "util/time.h"

namespace starrocks::bthreads {

// Modeled from std::counting_semaphore.
// A CountingSemaphore contains an internal counter initialized by the constructor.
// This counter is decremented by calls to acquire() and related methods, and
// is incremented by calls to release(). When the counter is zero, acquire()
// blocks current bthread until the counter is incremented, but try_acquire()
// does not block; try_acquire_for() and try_acquire_until() block until the
// counter is incremented or a timeout is reached.
template <int least_max_value = INT_MAX>
class CountingSemaphore {
    static_assert(least_max_value >= 0);
    static_assert(least_max_value <= INT_MAX);

public:
    // Constructs an object of type `CountingSemaphore` with the internal counter initialized to |desired|.
    explicit CountingSemaphore(int desired) {
        _counter = bthread::butex_create_checked<butil::atomic<int>>();
        _counter->store(desired, butil::memory_order_release);
    }

    ~CountingSemaphore() { bthread::butex_destroy(_counter); }

    CountingSemaphore(const CountingSemaphore&) = delete;
    CountingSemaphore& operator=(const CountingSemaphore&) = delete;

    static constexpr int max() noexcept { return least_max_value; }

    // Atomically increments the internal counter by the value of |update|. Any
    // (b)thread(s) waiting for the counter to be greater than zero, such as due
    // to being blocked in acquire, will subsequently be unblocked.
    //
    // Preconditions:
    //  - Both |update| >= 0 and |update| <= max() - counter are true, where
    //    counter is the value of the internal counter.
    // Parameters:
    //   - update the amount to increment the internal counter by
    void release(int update = 1) {
        if (_counter->fetch_add(update, butil::memory_order_release) > 0) {
            return;
        }
        // else
        if (update == 1) {
            (void)bthread::butex_wake(_counter);
        } else {
            (void)bthread::butex_wake_all(_counter);
        }
    }

    // Atomically decrements the internal counter by 1 if it is greater than zero; otherwise
    // blocks until it is greater than zero and can successfully decrement the internal counter.
    void acquire() { CHECK(do_try_acquire_until(nullptr)); }

    // Tries to atomically decrement the internal counter by 1 if it is greater than zero; no
    // blocking occurs regardless.
    //
    // Note:
    //  Similar to std::counting_semaphore::try_acquire(), CountingSemaphore's try_acquire()
    //  is allowed to fail to decrement the counter even if it was greater than zero - i.e.,
    //  they are allowed to spuriously fail and return false.
    bool try_acquire() {
        constexpr int kMaxRetry = 12;
        for (int i = 0; i < kMaxRetry; i++) {
            if (do_try_acquire()) {
                return true;
            }
            cpu_relax();
        }
        return false;
    }

    // Tries to atomically decrement the internal counter by 1 if it is greater than zero; otherwise
    // blocks until it is greater than zero and can successfully decrement the internal counter, or
    // the |dur| duration has been exceeded.
    //
    // Parameters:
    //  - dur the minimum duration the function must wait for it to fail.
    //
    // Return:
    //  - true if it decremented the internal counter, otherwise false.
    //
    // Note:
    //  In practice the function may take longer than |dur| to fail.
    template <typename Rep, typename Period>
    bool try_acquire_for(const std::chrono::duration<Rep, Period>& dur) {
        return try_acquire_until(std::chrono::system_clock::now() +
                                 std::chrono::ceil<std::chrono::system_clock::duration>(dur));
    }

    // Tries to atomically decrement the internal counter by 1 if it is greater than zero; otherwise
    // blocks until it is greater than zero and can successfully decrement the internal counter, or
    // the |abs| time point has been passed.
    //
    // Parameters:
    //  - abs the earliest time the function must wait until in order to fail
    // Return:
    //  - true if it decremented the internal counter, otherwise false.
    //
    // Note:
    //  In practice the function may take longer than |abs| to fail.
    template <typename Clock, typename Duration>
    bool try_acquire_until(const std::chrono::time_point<Clock, Duration>& abs) {
        timespec ts = TimespecFromTimePoint(abs);
        return do_try_acquire_until(&ts);
    }

private:
    bool do_try_acquire() {
        auto old = _counter->load(butil::memory_order_acquire);
        if (old == 0) return false;
        return _counter->compare_exchange_strong(old, old - 1, butil::memory_order_acquire,
                                                 butil::memory_order_relaxed);
    }

    bool do_try_acquire_until(const timespec* ts) {
        while (true) {
            auto old = _counter->load(butil::memory_order_acquire);
            if (old == 0) {
                if (bthread::butex_wait(_counter, old, ts) < 0) {
                    if (errno == ETIMEDOUT) {
                        return false;
                    } else if (errno != EWOULDBLOCK && errno != EINTR) {
                        PLOG(WARNING) << "Fail to wait butex";
                    }
                }
                continue;
            }
            DCHECK(old > 0);
            if (_counter->compare_exchange_strong(old, old - 1, butil::memory_order_acquire,
                                                  butil::memory_order_relaxed)) {
                return true;
            }
        }
    }

    butil::atomic<int>* _counter;
};

using BinarySemaphore = CountingSemaphore<1>;

} // namespace starrocks::bthreads
