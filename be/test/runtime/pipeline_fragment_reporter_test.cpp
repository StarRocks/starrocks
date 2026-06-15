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

#include "runtime/pipeline_fragment_reporter.h"

#include <gtest/gtest.h>

#include <vector>

#include "base/testutil/assert.h"
#include "exec/pipeline/query_context_manager.h"

namespace starrocks {

namespace {

TUniqueId make_id(int64_t hi, int64_t lo) {
    TUniqueId id;
    id.__set_hi(hi);
    id.__set_lo(lo);
    return id;
}

void expect_same_task(const PipeLineReportTaskKey& actual, const PipeLineReportTaskKey& expected) {
    EXPECT_EQ(actual.query_id, expected.query_id);
    EXPECT_EQ(actual.fragment_instance_id, expected.fragment_instance_id);
}

} // namespace

TEST(PipelineFragmentReporterTest, MissingQueryReturnsTaskToUnregister) {
    pipeline::QueryContextManager query_context_mgr(6);
    ASSERT_OK(query_context_mgr.init());

    PipeLineReportTaskKey task(make_id(1, 2), make_id(3, 4));
    std::vector<PipeLineReportTaskKey> tasks{task};

    auto tasks_to_unregister = report_pipeline_fragments(&query_context_mgr, tasks);

    ASSERT_EQ(tasks_to_unregister.size(), 1);
    expect_same_task(tasks_to_unregister[0], task);
}

TEST(PipelineFragmentReporterTest, MissingFragmentReturnsTaskToUnregister) {
    pipeline::QueryContextManager query_context_mgr(6);
    ASSERT_OK(query_context_mgr.init());

    auto query_id = make_id(10, 20);
    auto fragment_instance_id = make_id(30, 40);
    PipeLineReportTaskKey task(query_id, fragment_instance_id);
    ASSERT_OK(query_context_mgr.get_or_register(query_id).status());

    std::vector<PipeLineReportTaskKey> tasks{task};
    auto tasks_to_unregister = report_pipeline_fragments(&query_context_mgr, tasks);

    ASSERT_EQ(tasks_to_unregister.size(), 1);
    expect_same_task(tasks_to_unregister[0], task);
}

} // namespace starrocks
