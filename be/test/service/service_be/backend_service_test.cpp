// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "service/service_be/backend_service.h"

#include <gtest/gtest.h>

#include "base/utility/defer_op.h"
#include "exec/exec_env.h"
#include "storage/storage_metrics.h"

namespace starrocks {

TEST(BackendServiceTest, get_tablets_info_returns_max_compaction_score) {
    auto* metrics = StorageMetrics::instance();
    const auto old_cumulative_score = metrics->tablet_cumulative_max_compaction_score.value();
    const auto old_base_score = metrics->tablet_base_max_compaction_score.value();
    const auto old_update_score = metrics->tablet_update_max_compaction_score.value();
    DeferOp restore_metrics([&] {
        metrics->tablet_cumulative_max_compaction_score.set_value(old_cumulative_score);
        metrics->tablet_base_max_compaction_score.set_value(old_base_score);
        metrics->tablet_update_max_compaction_score.set_value(old_update_score);
    });

    BackendService service(ExecEnv::GetInstance(), nullptr);
    TGetTabletsInfoRequest request;

    metrics->tablet_cumulative_max_compaction_score.set_value(17);
    metrics->tablet_base_max_compaction_score.set_value(11);
    metrics->tablet_update_max_compaction_score.set_value(99);
    TGetTabletsInfoResult cumulative_result;
    service.get_tablets_info(cumulative_result, request);
    ASSERT_EQ(TStatusCode::OK, cumulative_result.status.status_code);
    ASSERT_TRUE(cumulative_result.__isset.tablet_max_compaction_score);
    EXPECT_EQ(17, cumulative_result.tablet_max_compaction_score);

    metrics->tablet_cumulative_max_compaction_score.set_value(7);
    metrics->tablet_base_max_compaction_score.set_value(23);
    TGetTabletsInfoResult base_result;
    service.get_tablets_info(base_result, request);
    ASSERT_EQ(TStatusCode::OK, base_result.status.status_code);
    ASSERT_TRUE(base_result.__isset.tablet_max_compaction_score);
    EXPECT_EQ(23, base_result.tablet_max_compaction_score);
}

} // namespace starrocks
