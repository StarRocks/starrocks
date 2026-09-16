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

#include "compute_env/profile_report_worker.h"

#include <gtest/gtest.h>

#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "common/config_exec_flow_fwd.h"
#include "compute_env/compute_env.h"

namespace starrocks {

namespace {

TUniqueId make_id(int64_t hi, int64_t lo) {
    TUniqueId id;
    id.__set_hi(hi);
    id.__set_lo(lo);
    return id;
}

ProfileReportWorkerOptions make_noop_options() {
    ProfileReportWorkerOptions options;
    options.start_worker_thread = false;
    options.report_non_pipeline_fragments = [](const std::vector<TUniqueId>&) { return std::vector<TUniqueId>(); };
    options.report_pipeline_fragments = [](const std::vector<PipeLineReportTaskKey>&) {
        return std::vector<PipeLineReportTaskKey>();
    };
    return options;
}

} // namespace

TEST(ProfileReportWorkerTest, ReportsViaCallbacksAndUnregistersReturnedTasks) {
    const auto old_interval = config::profile_report_interval;
    DeferOp restore_interval([&] { config::profile_report_interval = old_interval; });
    config::profile_report_interval = 0;

    std::vector<std::vector<TUniqueId>> non_pipeline_batches;
    std::vector<std::vector<PipeLineReportTaskKey>> pipeline_batches;

    ProfileReportWorkerOptions options;
    options.start_worker_thread = false;
    options.report_non_pipeline_fragments = [&](const std::vector<TUniqueId>& ids) {
        non_pipeline_batches.push_back(ids);
        return ids;
    };
    options.report_pipeline_fragments = [&](const std::vector<PipeLineReportTaskKey>& tasks) {
        pipeline_batches.push_back(tasks);
        return tasks;
    };

    ProfileReportWorker worker(std::move(options));
    const auto query_id = make_id(100, 200);
    const auto fragment_instance_id = make_id(300, 400);

    ASSERT_OK(worker.register_non_pipeline_load(fragment_instance_id));
    ASSERT_OK(worker.register_pipeline_load(query_id, fragment_instance_id));

    worker._start_report_profile();

    ASSERT_EQ(non_pipeline_batches.size(), 1);
    ASSERT_EQ(non_pipeline_batches[0].size(), 1);
    EXPECT_EQ(non_pipeline_batches[0][0], fragment_instance_id);
    EXPECT_TRUE(worker._non_pipeline_report_tasks.empty());

    ASSERT_EQ(pipeline_batches.size(), 1);
    ASSERT_EQ(pipeline_batches[0].size(), 1);
    EXPECT_EQ(pipeline_batches[0][0].query_id, query_id);
    EXPECT_EQ(pipeline_batches[0][0].fragment_instance_id, fragment_instance_id);
    EXPECT_TRUE(worker._pipeline_report_tasks.empty());

    worker.close();
    worker.close();
}

TEST(ProfileReportWorkerTest, RejectsDuplicateRegistrations) {
    ProfileReportWorker worker(make_noop_options());
    const auto query_id = make_id(101, 201);
    const auto fragment_instance_id = make_id(301, 401);

    ASSERT_OK(worker.register_non_pipeline_load(fragment_instance_id));
    EXPECT_TRUE(worker.register_non_pipeline_load(fragment_instance_id).is_internal_error());

    ASSERT_OK(worker.register_pipeline_load(query_id, fragment_instance_id));
    EXPECT_TRUE(worker.register_pipeline_load(query_id, fragment_instance_id).is_internal_error());

    worker.close();
}

TEST(ComputeEnvTest, ProfileReportWorkerLifecycle) {
    ComputeEnv env;

    ASSERT_EQ(env.profile_report_worker(), nullptr);
    ASSERT_OK(env.init_profile_report_worker(make_noop_options()));
    ASSERT_NE(env.profile_report_worker(), nullptr);

    env.stop_profile_report_worker();
    env.destroy_profile_report_worker();
    EXPECT_EQ(env.profile_report_worker(), nullptr);
}

} // namespace starrocks
