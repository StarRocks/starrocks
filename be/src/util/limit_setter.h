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
#define LIMIT_SETTER_ACTUAL_NUM(n) ((int32_t)((n)&0xffff'fffful))
#define LIMIT_SETTER_EXPECT_NUM(n) ((int32_t)(((n)&0xffff'ffff'0000'0000ul) >> 32))
#define LIMIT_SETTER_MERGE(expect_num, real_num) ((((int64_t)(expect_num)) << 32) | ((int64_t)(real_num)))
// LimitSetter is used to control maximum number of threads in thread pool dynamically
class LimitSetter {
public:
    void set_actual_num(int32_t n) {
        int64_t old_value = _value.load(std::memory_order_relaxed);
        int64_t new_value = LIMIT_SETTER_MERGE(n, n);
        _value.compare_exchange_strong(old_value, new_value);
    }

    bool adjust_expect_num(int32_t expect_num, int32_t* old_expect_num) {
        int64_t old_value = _value.load(std::memory_order_relaxed);
        *old_expect_num = LIMIT_SETTER_EXPECT_NUM(old_value);
        int32_t old_num = LIMIT_SETTER_ACTUAL_NUM(old_value);
        int64_t new_value = LIMIT_SETTER_MERGE(expect_num, old_num);
        auto success = _value.compare_exchange_strong(old_value, new_value);
        if (!success) {
            *old_expect_num = LIMIT_SETTER_EXPECT_NUM(old_value);
        }
        return success;
    }

    bool should_shrink() {
        int64_t old_value = _value.load(std::memory_order_relaxed);
        int32_t expect_num = LIMIT_SETTER_EXPECT_NUM(old_value);
        int32_t actual_num = LIMIT_SETTER_ACTUAL_NUM(old_value);
        if (expect_num >= actual_num) {
            return false;
        }
        int64_t new_value = LIMIT_SETTER_MERGE(expect_num, actual_num - 1);
        return _value.compare_exchange_strong(old_value, new_value);
    }

private:
    std::atomic<int64_t> _value;
};
} // namespace starrocks
