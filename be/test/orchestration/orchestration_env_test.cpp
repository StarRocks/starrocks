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

#include "orchestration/orchestration_env.h"

#include <gtest/gtest.h>

#include "base/utility/defer_op.h"
#include "common/process_exit.h"
#include "common/system/cpu_info.h"
#include "compute_env/load/stream_load_metrics.h"

namespace starrocks {

extern std::atomic<bool> k_starrocks_quick_exit;

namespace orchestration {

class OrchestrationEnvTest : public testing::Test {
protected:
    static void SetUpTestSuite() { CpuInfo::init(); }

    void TearDown() override { k_starrocks_quick_exit.store(false); }
};

TEST_F(OrchestrationEnvTest, quick_exit_ignores_idle_load_state) {
    OrchestrationEnv env;
    auto* metrics = StreamLoadMetrics::instance();
    const auto stream_before = metrics->streaming_load_current_processing.value();
    const auto txn_before = metrics->transaction_streaming_load_current_processing.value();
    const size_t work_before = shutdown_work_inflight();

    metrics->streaming_load_current_processing.increment(1);
    metrics->transaction_streaming_load_current_processing.increment(1);
    DeferOp restore([&] {
        metrics->streaming_load_current_processing.set_value(stream_before);
        metrics->transaction_streaming_load_current_processing.set_value(txn_before);
        k_starrocks_quick_exit.store(false);
    });

    ASSERT_TRUE(set_process_quick_exit());
    EXPECT_EQ(work_before, env.get_running_fragments_count_for_test());

    k_starrocks_quick_exit.store(false);
    EXPECT_GE(env.get_running_fragments_count_for_test(), work_before + 2);
}

TEST_F(OrchestrationEnvTest, quick_exit_counts_shutdown_work) {
    OrchestrationEnv env;
    const size_t work_before = shutdown_work_inflight();
    inc_shutdown_work();
    bool owns = true;
    DeferOp restore([&] {
        if (owns) {
            dec_shutdown_work();
        }
        k_starrocks_quick_exit.store(false);
    });

    ASSERT_TRUE(set_process_quick_exit());
    EXPECT_GE(env.get_running_fragments_count_for_test(), work_before + 1);

    owns = false;
    dec_shutdown_work();
    EXPECT_EQ(work_before, env.get_running_fragments_count_for_test());
}

} // namespace orchestration
} // namespace starrocks
