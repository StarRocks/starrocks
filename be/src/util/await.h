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

#include <functional>

namespace starrocks {

// Convenient function to archive wait-until ability
// Awaitility()
// .timeout(1000 * 1000) // one second
// .interval(1000)  // check every 1000 microseconds
// .until(() -> {}); // until the condition is true or timedout
class Awaitility {
public:
    typedef std::function<bool()> condition_fun;
    typedef std::function<void(int)> interval_callback_t;

public:
    explicit Awaitility() = default;

    // overall timeout
    Awaitility& timeout(int64_t timeout) {
        _timeout = timeout;
        return *this;
    }

    // check interval
    // if not provided, set to 10ms by default, or equal to the timeout value if _timeout < 10ms
    Awaitility& interval(int64_t interval) {
        _interval = interval;
        return *this;
    }

    // when the condition is false during the check, the cb will be called if provided.
    Awaitility& interval_callback(interval_callback_t cb) {
        _cb = cb;
        return *this;
    }

    // block until the cond is true or timed out.
    // return value:
    // * true: condition is true
    // * false: condition is still false, timed out
    bool until(condition_fun cond);

private:
    int64_t _timeout = 0;
    int64_t _interval = 0;
    condition_fun _cond;
    interval_callback_t _cb;
};
} // namespace starrocks
