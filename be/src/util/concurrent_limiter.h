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

#include <atomic>

namespace starrocks {

class ConcurrentLimiter {
public:
    explicit ConcurrentLimiter(int64_t limit_count) : _limit_count(limit_count) {}

    ~ConcurrentLimiter() = default;

    bool inc() {
        int64_t old_cnt = 0;
        do {
            old_cnt = _counter.load();
            if (reach_limit(old_cnt)) {
                return false;
            }
        } while (!_counter.compare_exchange_strong(old_cnt, old_cnt + 1));
        return true;
    }
    void dec() { _counter.fetch_add(-1); }
    bool reach_limit(int64_t cnt) const { return cnt >= _limit_count; }

private:
    int64_t _limit_count = 0;
    std::atomic<int64_t> _counter{0};
};

class ConcurrentLimiterGuard {
public:
    explicit ConcurrentLimiterGuard() {}

    bool set_limiter(ConcurrentLimiter* limiter) {
        if (limiter->inc()) {
            _limiter = limiter;
            return true;
        } else {
            return false;
        }
    }

    ~ConcurrentLimiterGuard() {
        if (_limiter != nullptr) {
            _limiter->dec();
        }
    }

private:
    ConcurrentLimiter* _limiter = nullptr;
};

} // namespace starrocks