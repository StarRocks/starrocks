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

#include "connector/connector.h"
#include "connector/connector_registry.h"
#include "gtest/gtest.h"
#include "module/connector_bootstrap.h"

namespace starrocks::connector {

TEST(ModuleBootstrapTest, BootstrapBuiltinConnectorsInstallsCacheStatsIdempotently) {
    auto* registry = ConnectorRegistry::default_instance();
    ASSERT_NE(nullptr, registry);

    auto status = bootstrap_builtin_connectors();
    ASSERT_TRUE(status.ok()) << status;
    const auto* cache_stats = registry->get(Connector::CACHE_STATS);
    ASSERT_NE(nullptr, cache_stats);
    EXPECT_EQ(ConnectorType::CACHE_STATS, cache_stats->connector_type());

    status = bootstrap_builtin_connectors();
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(cache_stats, registry->get(Connector::CACHE_STATS));
}

} // namespace starrocks::connector
