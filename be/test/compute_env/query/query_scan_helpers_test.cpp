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

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "compute_env/query/connector_scan_mem_share_arbitrator.h"
#include "compute_env/query/global_late_materialization_context.h"

namespace starrocks {

namespace {

class DummyGlobalLateMaterializationContext final : public GlobalLateMaterilizationContext {};

} // namespace

TEST(ConnectorScanMemShareArbitratorTest, InitializesWithInjectedDataSourceMemBytes) {
    pipeline::ConnectorScanOperatorMemShareArbitrator arbitrator(100, 2, 10);

    EXPECT_EQ(100, arbitrator.query_mem_limit);
    EXPECT_EQ(100, arbitrator.scan_mem_limit);
    EXPECT_EQ(20, arbitrator.total_chunk_source_mem_bytes.load());
}

TEST(ConnectorScanMemShareArbitratorTest, UpdatesChunkSourceMemoryShare) {
    pipeline::ConnectorScanOperatorMemShareArbitrator arbitrator(100, 2, 10);

    EXPECT_EQ(50, arbitrator.set_scan_mem_ratio(0.5));
    EXPECT_EQ(37, arbitrator.update_chunk_source_mem_bytes(10, 30));
    EXPECT_EQ(40, arbitrator.total_chunk_source_mem_bytes.load());
    EXPECT_EQ(0, arbitrator.update_chunk_source_mem_bytes(30, 0));
    EXPECT_EQ(10, arbitrator.total_chunk_source_mem_bytes.load());
}

TEST(GlobalLateMaterializationContextMgrTest, GetOrCreateCreatesOnlyOncePerScanNode) {
    GlobalLateMaterilizationContextMgr manager;
    std::vector<std::unique_ptr<GlobalLateMaterilizationContext>> contexts;
    int create_count = 0;

    auto* first = manager.get_or_create_ctx(7, [&]() {
        ++create_count;
        contexts.emplace_back(std::make_unique<DummyGlobalLateMaterializationContext>());
        return contexts.back().get();
    });
    auto* second = manager.get_or_create_ctx(7, [&]() {
        ++create_count;
        contexts.emplace_back(std::make_unique<DummyGlobalLateMaterializationContext>());
        return contexts.back().get();
    });

    EXPECT_EQ(1, create_count);
    EXPECT_EQ(first, second);
    EXPECT_EQ(first, manager.get_ctx(7));
}

} // namespace starrocks
