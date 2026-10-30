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

#include "compute_env/load/load_stream_mgr.h"

#include <gtest/gtest.h>

#include <memory>

#include "base/metrics.h"
#include "base/testutil/assert.h"

namespace starrocks {

TEST(LoadStreamMgrTest, PutGetRemove) {
    LoadStreamMgr mgr;
    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<StreamLoadPipe>();

    ASSERT_OK(mgr.put(load_id, pipe));
    EXPECT_EQ(pipe, mgr.get(load_id));

    mgr.remove(load_id);
    EXPECT_EQ(nullptr, mgr.get(load_id));
}

TEST(LoadStreamMgrTest, DuplicatePutReturnsError) {
    LoadStreamMgr mgr;
    auto load_id = UniqueId::gen_uid();

    ASSERT_OK(mgr.put(load_id, std::make_shared<StreamLoadPipe>()));
    ASSERT_FALSE(mgr.put(load_id, std::make_shared<StreamLoadPipe>()).ok());
}

TEST(LoadStreamMgrTest, CloseClearsPipes) {
    LoadStreamMgr mgr;
    auto load_id = UniqueId::gen_uid();

    ASSERT_OK(mgr.put(load_id, std::make_shared<StreamLoadPipe>()));
    mgr.close();

    EXPECT_EQ(nullptr, mgr.get(load_id));
}

TEST(LoadStreamMgrTest, InstallMetricsReportsPipeCount) {
    MetricRegistry registry("load_stream_mgr_test");
    LoadStreamMgr mgr;
    mgr.install_metrics(&registry);

    auto load_id = UniqueId::gen_uid();
    ASSERT_OK(mgr.put(load_id, std::make_shared<StreamLoadPipe>()));

    registry.trigger_hook();
    auto* metric = registry.get_metric("stream_load_pipe_count");
    ASSERT_NE(nullptr, metric);
    EXPECT_EQ("1", metric->to_string());

    mgr.remove(load_id);
    registry.trigger_hook();
    EXPECT_EQ("0", metric->to_string());
}

} // namespace starrocks
