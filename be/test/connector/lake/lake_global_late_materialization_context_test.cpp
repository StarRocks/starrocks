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

#include "connector/lake/lake_global_late_materialization_context.h"

#include <memory>
#include <string>
#include <vector>

#include "common/object_pool.h"
#include "gtest/gtest.h"
#include "storage/lake/rowset.h"

namespace starrocks {

namespace {

lake::RowsetPtr make_lake_rowset(int num_segments, ObjectPool* pool) {
    static int64_t next_id = 10000;
    auto meta = pool->add(new RowsetMetadataPB());
    meta->set_num_rows(100);
    for (int i = 0; i < num_segments; ++i) {
        meta->add_segment_metas()->set_filename("seg_" + std::to_string(i) + ".dat");
    }
    return std::make_shared<lake::Rowset>(nullptr, next_id++, std::move(meta), 0, nullptr);
}

std::vector<BaseRowsetSharedPtr> as_base(const std::vector<lake::RowsetPtr>& rowsets) {
    std::vector<BaseRowsetSharedPtr> result;
    for (const auto& rowset : rowsets) {
        result.emplace_back(std::static_pointer_cast<BaseRowset>(rowset));
    }
    return result;
}

} // namespace

TEST(LakeScanLazyMaterializationContextTest, ScanNodeSetOnlyOnce) {
    LakeScanLazyMaterializationContext ctx;
    TLakeScanNode first;
    first.__set_next_uniq_id(7);
    TLakeScanNode second;
    second.__set_next_uniq_id(9);

    ctx.set_scan_node(first);
    ctx.set_scan_node(second);

    EXPECT_EQ(7, ctx.scan_node().next_uniq_id);
}

TEST(LakeScanLazyMaterializationContextTest, CaptureRowsetsStoresVersion) {
    LakeScanLazyMaterializationContext ctx;
    ObjectPool pool;
    auto rowset = make_lake_rowset(2, &pool);
    ctx.capture_rowsets(100, 42, as_base({rowset}));
    EXPECT_EQ(42, ctx.get_rowsets_version(100));
}

TEST(LakeScanLazyMaterializationContextTest, VersionsIsolatedPerTablet) {
    LakeScanLazyMaterializationContext ctx;
    ObjectPool pool;
    ctx.capture_rowsets(1, 10, as_base({make_lake_rowset(1, &pool)}));
    ctx.capture_rowsets(2, 20, as_base({make_lake_rowset(1, &pool)}));
    ctx.capture_rowsets(3, 30, as_base({make_lake_rowset(1, &pool)}));

    EXPECT_EQ(10, ctx.get_rowsets_version(1));
    EXPECT_EQ(20, ctx.get_rowsets_version(2));
    EXPECT_EQ(30, ctx.get_rowsets_version(3));
}

TEST(LakeScanLazyMaterializationContextTest, GetRowsetFirstSegment) {
    LakeScanLazyMaterializationContext ctx;
    ObjectPool pool;
    auto rowset = make_lake_rowset(2, &pool);
    ctx.capture_rowsets(100, 1, as_base({rowset}));

    int32_t segment_idx = -1;
    auto result = ctx.get_rowset(100, 0, &segment_idx);
    ASSERT_NE(nullptr, result);
    EXPECT_EQ(rowset.get(), result.get());
    EXPECT_EQ(0, segment_idx);
}

} // namespace starrocks
