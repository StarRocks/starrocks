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

#include "exec/pipeline/scan/split_morsel_ticket_checker.h"

#include <gtest/gtest.h>

#include <memory>

namespace starrocks::pipeline {

TEST(SplitMorselTicketCheckerTest, waits_until_all_split_morsels_leave) {
    auto test_func = [](int64_t id, int n) {
        auto ticket_checker = std::make_shared<SplitMorselTicketChecker>();
        for (auto i = 0; i < n; ++i) {
            ticket_checker->enter(id, i + 1 == n);
            ASSERT_EQ(ticket_checker->are_all_ready(id), i + 1 == n);
        }
        for (auto i = 0; i < n; ++i) {
            ASSERT_EQ(ticket_checker->leave(id), i + 1 == n);
        }
    };

    test_func(1L, 1);
    test_func(1L, 10);
    test_func(1L, 100);
}

TEST(SplitMorselTicketCheckerTest, handles_interleaved_enter_and_leave) {
    auto test_func = [](int64_t id, int n) {
        auto ticket_checker = std::make_shared<SplitMorselTicketChecker>();
        for (auto i = 0; i < n; ++i) {
            ticket_checker->enter(id, i + 1 == n);
            ASSERT_EQ(ticket_checker->are_all_ready(id), i + 1 == n);
            ASSERT_EQ(ticket_checker->leave(id), i + 1 == n);
        }
    };

    test_func(1L, 1);
    test_func(1L, 10);
    test_func(1L, 100);
}

} // namespace starrocks::pipeline
