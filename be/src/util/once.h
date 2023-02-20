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

#include <bthread/sys_futex.h>

#include <atomic>
#include <functional>

#include "common/statusor.h"

namespace starrocks {

struct OnceFlag {
    std::atomic<int> flag{0};
};

inline bool invoked(const OnceFlag& once) {
    return once.flag.load(std::memory_order_acquire) == 2;
}

// Executes the Callable object f exactly once, even if called concurrently, from several threads.
//
// If concurrent calls to invoke_once pass different functions f, it is unspecified which f will be called.
// The selected function runs in the same thread as the invoke_once invocation it was passed to.
//
// Return value
//  true: |fn| is invoked
//  false: |fn| has been invoked before
template <typename Callable, typename... Args>
bool invoke_once(OnceFlag& once, Callable&& f, Args&&... args) {
    while (true) {
        auto curr_flag = once.flag.load(std::memory_order_acquire);
        if (curr_flag == 2) {
            return false;
        }
        if (curr_flag == 0 && once.flag.compare_exchange_weak(curr_flag, 1, std::memory_order_release)) {
            std::invoke(std::forward<Callable>(f), std::forward<Args>(args)...);
            once.flag.store(2, std::memory_order_release);
            (void)bthread::futex_wake_private(&once.flag, INT_MAX);
            return true;
        }
        if (curr_flag == 1) {
            (void)bthread::futex_wait_private(&once.flag, curr_flag, nullptr);
        }
    }
}

// Executes the Callable object f *successfully* exactly once, even if called concurrently, from several threads.
//
// Whether the execution is successul is determinted by the return value of f: any non-OK value indicates the
// execution is failed, in which case the Callable object f will be called again next time success_once invoked.
//
// If concurrent calls to invoke_once pass different functions f, it is unspecified which f will be called.
// The selected function runs in the same thread as the invoke_once invocation it was passed to.
//
// Return value
//  true: |fn| is invoked and returned an OK status.
//  false: |fn| has been invoked before and returned an OK status.
//  non-OK status: |fn| is invoked and returned a non-OK status.
template <typename Callable, typename... Args>
StatusOr<bool> success_once(OnceFlag& once, Callable&& f, Args&&... args) {
    while (true) {
        auto curr_flag = once.flag.load(std::memory_order_acquire);
        if (curr_flag == 2) {
            return false;
        }
        if (curr_flag == 0 && once.flag.compare_exchange_weak(curr_flag, 1, std::memory_order_release)) {
            auto&& st = std::invoke(std::forward<Callable>(f), std::forward<Args>(args)...);
            if (st.ok()) {
                once.flag.store(2, std::memory_order_release);
                (void)bthread::futex_wake_private(&once.flag, INT_MAX);
                return true;
            } else {
                once.flag.store(0, std::memory_order_release);
                (void)bthread::futex_wake_private(&once.flag, 1);
                return std::move(st);
            }
        }
        if (curr_flag == 1) {
            (void)bthread::futex_wait_private(&once.flag, curr_flag, nullptr);
        }
    }
}

} // namespace starrocks
