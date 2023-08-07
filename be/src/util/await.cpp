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

#include "util/await.h"

#include <bthread/bthread.h>

#include <chrono>

namespace starrocks {

static const int64_t kDefaultMinInterval = 10 * 1000; // 10ms

bool Awaitility::until(condition_fun cond) {
    if (_timeout <= 0) {
        return cond();
    }
    if (_interval == 0) {
        _interval = std::min(kDefaultMinInterval, _timeout);
    }
    auto start = std::chrono::steady_clock::now();
    int count = 0;
    while (!cond()) {
        if (_cb && count > 0) {
            _cb(count);
        }
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::microseconds>(now - start).count() >= _timeout) {
            return false;
        }
        bthread_usleep(_interval);
        ++count;
    }
    return true;
}

} // namespace starrocks
