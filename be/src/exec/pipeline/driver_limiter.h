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
#include <memory>

#include "common/statusor.h"
#include "gutil/strings/substitute.h"

namespace starrocks::pipeline {

class DriverLimiter {
public:
    struct Token {
    public:
        Token(std::atomic<int>& num_total_drivers, int num_drivers)
                : num_total_drivers(num_total_drivers), num_drivers(num_drivers) {}
        ~Token() { num_total_drivers -= num_drivers; }

        // Disable copy/move ctor and assignment.
        Token(const Token&) = delete;
        Token& operator=(const Token&) = delete;
        Token(Token&&) = delete;
        Token& operator=(Token&&) = delete;

    private:
        std::atomic<int>& num_total_drivers;
        int num_drivers;
    };
    using TokenPtr = std::unique_ptr<Token>;

    explicit DriverLimiter(int max_num_drivers) : _max_num_drivers(max_num_drivers) {}
    ~DriverLimiter() = default;

    // Return non-ok status, if it cannot acquire `num_drivers` drivers from the limiter.
    // Otherwise, return the token, the destructor of which will release the acquired
    // `num_drivers` drivers back to the limiter.
    StatusOr<TokenPtr> try_acquire(int num_drivers);

    int num_total_drivers() const { return _num_total_drivers; }

private:
    const int _max_num_drivers;
    std::atomic<int> _num_total_drivers{0};
};

} // namespace starrocks::pipeline
