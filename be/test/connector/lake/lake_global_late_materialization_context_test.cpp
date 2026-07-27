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

// Build a lake::RowsetPtr with an explicit rowset id and explicit per-segment segment_idx values,
// so the rowset's rssids are `rowset_id + segment_idx` (not the dense positional index). This mirrors
// what a tablet split produces on a child that keeps only a subset of a rowset's segments while
// preserving each kept segment's ORIGINAL segment_idx (issue #75993).
lake::RowsetPtr make_lake_rowset_with_seg_idxs(uint32_t rowset_id, const std::vector<uint32_t>& seg_idxs,
                                               ObjectPool* pool) {
    auto meta = pool->add(new RowsetMetadataPB());
    meta->set_id(rowset_id);
    meta->set_num_rows(100);
    for (uint32_t idx : seg_idxs) {
        auto* sm = meta->add_segment_metas();
        sm->set_filename("seg_" + std::to_string(idx) + ".dat");
        sm->set_segment_idx(idx);
    }
    return std::make_shared<lake::Rowset>(nullptr, /*tablet_id=*/1, std::move(meta), 0, nullptr);
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

// Sparse segment_idx after a tablet split: a child keeps segments with segment_idx {1, 2}
// (physical positions 0, 1) while segment_metas_size()==2. get_rowset must resolve each rssid by
// its real (rowset.id + segment_idx) and return the segment's PHYSICAL position. The pre-fix dense
// logic (`rssid_base <= rssid < rssid_base + num_segments`, segment = rssid - rssid_base) would miss
// rssid id+2 (falls outside [id, id+2)) -> "not found lake rssid" -- and resolve id+1 to the wrong
// physical position. Regression guard for issue #75993.
TEST(LakeScanLazyMaterializationContextTest, GetRowsetSparseSegmentIdx) {
    LakeScanLazyMaterializationContext ctx;
    ObjectPool pool;
    // rowset id=1000, kept segments segment_idx {1, 2} (low-key segment 0 dropped by the split).
    auto rs = make_lake_rowset_with_seg_idxs(1000, {1, 2}, &pool);
    ctx.capture_rowsets(7, /*version=*/1, as_base({rs}));

    int32_t seg_idx = -1;
    // rssid id+1 -> physical position 0.
    auto got = ctx.get_rowset(7, 1001, &seg_idx);
    ASSERT_NE(nullptr, got);
    EXPECT_EQ(rs.get(), got.get());
    EXPECT_EQ(0, seg_idx);

    // rssid id+2 -> physical position 1 (the case the pre-fix code failed to resolve).
    seg_idx = -1;
    got = ctx.get_rowset(7, 1002, &seg_idx);
    ASSERT_NE(nullptr, got);
    EXPECT_EQ(rs.get(), got.get());
    EXPECT_EQ(1, seg_idx);

    // rssid id+0: segment_idx 0 was dropped, so no segment owns it -> unresolved.
    seg_idx = -1;
    EXPECT_EQ(nullptr, ctx.get_rowset(7, 1000, &seg_idx));

    // rssid beyond the rowset's owned span [id, id + max_segment_idx + 1) -> unresolved.
    EXPECT_EQ(nullptr, ctx.get_rowset(7, 1003, &seg_idx));
}

// Multiple rowsets, one dense and one sparse: each rssid resolves to the correct rowset + position.
TEST(LakeScanLazyMaterializationContextTest, GetRowsetMixedDenseAndSparse) {
    LakeScanLazyMaterializationContext ctx;
    ObjectPool pool;
    auto dense = make_lake_rowset_with_seg_idxs(2000, {0, 1}, &pool);  // span [2000, 2002)
    auto sparse = make_lake_rowset_with_seg_idxs(3000, {2, 3}, &pool); // span [3000, 3004)
    ctx.capture_rowsets(9, /*version=*/1, as_base({dense, sparse}));

    int32_t seg_idx = -1;
    EXPECT_EQ(dense.get(), ctx.get_rowset(9, 2000, &seg_idx).get());
    EXPECT_EQ(0, seg_idx);
    EXPECT_EQ(dense.get(), ctx.get_rowset(9, 2001, &seg_idx).get());
    EXPECT_EQ(1, seg_idx);

    EXPECT_EQ(sparse.get(), ctx.get_rowset(9, 3002, &seg_idx).get());
    EXPECT_EQ(0, seg_idx);
    EXPECT_EQ(sparse.get(), ctx.get_rowset(9, 3003, &seg_idx).get());
    EXPECT_EQ(1, seg_idx);

    // Gaps between/around the owned spans resolve to nothing.
    EXPECT_EQ(nullptr, ctx.get_rowset(9, 2002, &seg_idx));
    EXPECT_EQ(nullptr, ctx.get_rowset(9, 3000, &seg_idx));
    EXPECT_EQ(nullptr, ctx.get_rowset(9, 3001, &seg_idx));
}

} // namespace starrocks
