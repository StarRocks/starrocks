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

#include "util/stack_util.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <utility>

#include "testutil/sync_point.h"
#include "util/defer_op.h"

namespace starrocks {

using SymbolizeTuple = std::tuple<void*, char*, size_t>;

void test_get_stack_trace_for_all_threads(const std::string& line_prefix) {
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("StackTraceTask::symbolize");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    int32_t num_symbolize = 0;
    SyncPoint::GetInstance()->SetCallBack("StackTraceTask::symbolize", [&](void* arg) {
        SymbolizeTuple* tuple = (SymbolizeTuple*)arg;
        std::snprintf(std::get<1>(*tuple), std::get<2>(*tuple), "mock_frame_%d", num_symbolize);
        num_symbolize += 1;
    });
    std::string stack_trace = get_stack_trace_for_all_threads(line_prefix);

    std::vector<std::string> lines;
    std::istringstream stream(stack_trace);
    std::string buf;
    while (std::getline(stream, buf, '\n')) {
        lines.push_back(buf);
    }
    int32_t num_frame = 0;
    for (auto& line : lines) {
        if (line.empty()) {
            continue;
        }
        ASSERT_TRUE(line_prefix.size() <= line.size());
        ASSERT_TRUE(line.compare(0, line_prefix.size(), line_prefix) == 0);
        if (line.find("mock_frame_") == std::string::npos) {
            continue;
        }
        ASSERT_TRUE(line.find(fmt::format("mock_frame_{}", num_frame)) != std::string::npos);
        num_frame += 1;
    }
    ASSERT_EQ(num_frame, num_symbolize);
}

TEST(StackUtilTest, get_stack_trace_for_all_threads) {
    test_get_stack_trace_for_all_threads("");
    test_get_stack_trace_for_all_threads("MOCK_PREFIX - ");
}

} // namespace starrocks
