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
    // Half of the 50 byte budget is reserved as equal floors (12 per node for 2 nodes), and the
    // remaining 26 is split in proportion to this node's 30 out of a total of 40:
    // 12 + 26 * 0.75 == 31. The purely proportional split returned 50 * 0.75 == 37.
    EXPECT_EQ(31, arbitrator.update_chunk_source_mem_bytes(10, 30));
    EXPECT_EQ(40, arbitrator.total_chunk_source_mem_bytes.load());
    EXPECT_EQ(0, arbitrator.update_chunk_source_mem_bytes(30, 0));
    EXPECT_EQ(10, arbitrator.total_chunk_source_mem_bytes.load());
}

TEST(ConnectorScanMemShareArbitratorTest, SingleScanNodeGetsWholeScanMemLimit) {
    pipeline::ConnectorScanOperatorMemShareArbitrator arbitrator(100, 1, 10);

    // With one scan node the floor and the proportional remainder add back up to the whole budget,
    // so reserving floors cannot regress the single scan node plan.
    EXPECT_EQ(100, arbitrator.update_chunk_source_mem_bytes(10, 64));
    EXPECT_EQ(100, arbitrator.update_chunk_source_mem_bytes(64, 4096));
}

TEST(ConnectorScanMemShareArbitratorTest, EqualDemandsSplitEvenly) {
    pipeline::ConnectorScanOperatorMemShareArbitrator arbitrator(100, 2, 10);
    // Both nodes have reported the same per-chunk-source cost of 30.
    arbitrator.total_chunk_source_mem_bytes.store(60);

    // The floor split and the proportional split agree once every node has reported, so each node
    // still gets exactly half. Reserving floors only changes the outcome when the costs are skewed.
    EXPECT_EQ(50, arbitrator.update_chunk_source_mem_bytes(30, 30));
    EXPECT_EQ(50, arbitrator.update_chunk_source_mem_bytes(30, 30));
    EXPECT_EQ(60, arbitrator.total_chunk_source_mem_bytes.load());
}

TEST(ConnectorScanMemShareArbitratorTest, FinishedNodeReturnsItsShareToTheNodeStillScanning) {
    pipeline::ConnectorScanOperatorMemShareArbitrator arbitrator(100, 2, 10);
    // Two nodes reporting 30 and 70; the total reflects both.
    arbitrator.total_chunk_source_mem_bytes.store(100);
    EXPECT_EQ(60, arbitrator.update_chunk_source_mem_bytes(70, 70));

    // The cheap node finishes and hands its share back.
    EXPECT_EQ(0, arbitrator.update_chunk_source_mem_bytes(30, 0));
    EXPECT_EQ(70, arbitrator.total_chunk_source_mem_bytes.load());

    // The node still scanning must now be able to use the whole budget: its own contribution is
    // all that is left in the total, and the finished node no longer holds a floor. This property
    // came for free with the purely proportional split, so reserving floors must not break it.
    EXPECT_EQ(100, arbitrator.update_chunk_source_mem_bytes(70, 70));
}

TEST(ConnectorScanMemShareArbitratorTest, CheapNodeKeepsItsFloorAndSharesSumToLimit) {
    pipeline::ConnectorScanOperatorMemShareArbitrator arbitrator(100, 2, 10);
    // Drive the reported total to 100: one expensive node at 90 and one cheap node at 10.
    arbitrator.total_chunk_source_mem_bytes.store(100);

    const int64_t expensive_share = arbitrator.update_chunk_source_mem_bytes(90, 90);
    const int64_t cheap_share = arbitrator.update_chunk_source_mem_bytes(10, 10);

    // The purely proportional split handed the cheap node 100 * 0.1 == 10, one tenth of what the
    // expensive node got, even though it is the node that would finish early and give its share
    // back. The floor lifts it to 25 + 50 * 0.1 == 30.
    EXPECT_EQ(70, expensive_share);
    EXPECT_EQ(30, cheap_share);
    // The budget is still fully accounted for and never over-allocated.
    EXPECT_EQ(arbitrator.scan_mem_limit, expensive_share + cheap_share);
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
