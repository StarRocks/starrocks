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

#include "cache/block_cache/datacache_utils.h"

#include <gtest/gtest.h>

#include "gen_cpp/DataCache_types.h"

namespace starrocks {
class DataCacheUtilsTest : public ::testing::Test {};

TEST_F(DataCacheUtilsTest, test_set_metrics_from_thrift) {
    TDataCacheMetrics t_metrics{};
    DataCacheMetrics metrics{};
    metrics.status = DataCacheStatus::NORMAL;
    DataCacheUtils::set_metrics_from_thrift(t_metrics, metrics);
    ASSERT_EQ(t_metrics.status, TDataCacheStatus::NORMAL);

    metrics.status = DataCacheStatus::UPDATING;
    DataCacheUtils::set_metrics_from_thrift(t_metrics, metrics);
    ASSERT_EQ(t_metrics.status, TDataCacheStatus::UPDATING);

    metrics.status = DataCacheStatus::LOADING;
    DataCacheUtils::set_metrics_from_thrift(t_metrics, metrics);
    ASSERT_EQ(t_metrics.status, TDataCacheStatus::LOADING);

    metrics.status = DataCacheStatus::ABNORMAL;
    DataCacheUtils::set_metrics_from_thrift(t_metrics, metrics);
    ASSERT_EQ(t_metrics.status, TDataCacheStatus::ABNORMAL);
}

} // namespace starrocks
