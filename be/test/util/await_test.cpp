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

#include <gtest/gtest.h>

#include <chrono>

namespace starrocks {

class AwaitilityTest : public testing::Test {
public:
    AwaitilityTest() = default;
};

namespace {
struct SilientOperator {
public:
    SilientOperator(int64_t silient) : _silient(silient), _start(std::chrono::steady_clock::now()) {}

    bool operator()() const {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(now - _start).count() > _silient;
    }

public:
    int64_t _silient;
    std::chrono::time_point<std::chrono::steady_clock> _start;
};
} // namespace

TEST_F(AwaitilityTest, await) {
    { // no parameter set
        Awaitility await;
        EXPECT_TRUE(await.until([] { return true; }));
    }
    { // no parameter set
        Awaitility await;
        EXPECT_FALSE(await.until([] { return false; }));
    }
    { // silient for 100ms, timeout in 50ms
        Awaitility await;
        SilientOperator op(100 * 1000);
        EXPECT_FALSE(await.timeout(50 * 1000).until(op));
    }
    { // silient in 10ms, timeout in 50ms
        Awaitility await;
        SilientOperator op(10 * 1000);
        EXPECT_TRUE(await.timeout(50 * 1000).until(op));
    }
}

} // namespace starrocks
