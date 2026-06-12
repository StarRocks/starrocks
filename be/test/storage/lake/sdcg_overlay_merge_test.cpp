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

#include "storage/lake/sdcg_overlay_merge.h"

#include <gtest/gtest.h>

#include <roaring/roaring.hh>

#include "column/datum.h"
#include "column/fixed_length_column.h"

namespace starrocks::lake {

static ColumnPtr make_int32_col(const std::vector<int32_t>& vals) {
    auto c = Int32Column::create();
    for (int32_t v : vals) {
        c->append_datum(Datum(v));
    }
    ColumnPtr ret = std::move(c);
    return ret;
}

static int32_t int32_at(const MutableColumnPtr& col, size_t i) {
    return col->get(i).get_int32();
}

// Find the merged presence/value-column index for a given uid.
static int find_uid(const MergedOverlay& m, int32_t uid) {
    for (size_t i = 0; i < m.column_uids.size(); ++i) {
        if (m.column_uids[i] == uid) return static_cast<int>(i);
    }
    return -1;
}

static const MergedColumnPresence* find_presence(const MergedOverlay& m, int32_t uid) {
    for (const auto& p : m.presences) {
        if (p.column_uid == uid) return &p;
    }
    return nullptr;
}

// Layers:
//   A v1: cols {10,20}, rowids {0,5}, col10=[100,105], col20=[200,205]
//   B v2: cols {10},    rowids {5,7}, col10=[1105,1107]   (rid5/col10 overrides A)
//   C v3: cols {30},    rowids {0},   col30=[300]
// Union uids first-seen: [10,20,30]; union rowids {0,5,7} -> ordinals 0,1,2.
TEST(SdcgOverlayMergeTest, LastWriteWinsAcrossLayers) {
    std::vector<OverlaySparseLayer> layers;
    layers.push_back(OverlaySparseLayer{
            /*version=*/1, /*column_uids=*/{10, 20}, /*source_rowids=*/{0, 5},
            /*values=*/{make_int32_col({100, 105}), make_int32_col({200, 205})}});
    layers.push_back(OverlaySparseLayer{
            /*version=*/2, /*column_uids=*/{10}, /*source_rowids=*/{5, 7},
            /*values=*/{make_int32_col({1105, 1107})}});
    layers.push_back(OverlaySparseLayer{
            /*version=*/3, /*column_uids=*/{30}, /*source_rowids=*/{0}, /*values=*/{make_int32_col({300})}});

    auto res = merge_overlay_layers(layers);
    ASSERT_TRUE(res.ok()) << res.status();
    const MergedOverlay& m = res.value();

    EXPECT_EQ(3, m.num_rows);
    EXPECT_EQ(0, m.min_source_rowid);
    EXPECT_EQ(7, m.max_source_rowid);
    ASSERT_EQ(3u, m.column_uids.size());
    EXPECT_EQ(10, m.column_uids[0]);
    EXPECT_EQ(20, m.column_uids[1]);
    EXPECT_EQ(30, m.column_uids[2]);

    // column 0 = ascending union source_rowids [0,5,7]
    ASSERT_EQ(3u, m.source_rowid_column->size());
    EXPECT_EQ(0, m.source_rowid_column->get(0).get_int64());
    EXPECT_EQ(5, m.source_rowid_column->get(1).get_int64());
    EXPECT_EQ(7, m.source_rowid_column->get(2).get_int64());

    // col 10: rid0->100 (A), rid5->1105 (B wins over A), rid7->1107 (B); covered {0,5,7}
    int c10 = find_uid(m, 10);
    ASSERT_GE(c10, 0);
    EXPECT_EQ(100, int32_at(m.value_columns[c10], 0));
    EXPECT_EQ(1105, int32_at(m.value_columns[c10], 1));
    EXPECT_EQ(1107, int32_at(m.value_columns[c10], 2));
    const auto* p10 = find_presence(m, 10);
    ASSERT_NE(nullptr, p10);
    EXPECT_EQ(3, p10->count);
    EXPECT_EQ(0, p10->min_source_rowid);
    EXPECT_EQ(7, p10->max_source_rowid);

    // col 20: rid0->200, rid5->205; covered {0,5} (rid7 uncovered placeholder)
    int c20 = find_uid(m, 20);
    ASSERT_GE(c20, 0);
    EXPECT_EQ(200, int32_at(m.value_columns[c20], 0));
    EXPECT_EQ(205, int32_at(m.value_columns[c20], 1));
    const auto* p20 = find_presence(m, 20);
    ASSERT_NE(nullptr, p20);
    EXPECT_EQ(2, p20->count);
    EXPECT_EQ(0, p20->min_source_rowid);
    EXPECT_EQ(5, p20->max_source_rowid);

    // col 30: rid0->300; covered {0}
    int c30 = find_uid(m, 30);
    ASSERT_GE(c30, 0);
    EXPECT_EQ(300, int32_at(m.value_columns[c30], 0));
    const auto* p30 = find_presence(m, 30);
    ASSERT_NE(nullptr, p30);
    EXPECT_EQ(1, p30->count);
    EXPECT_EQ(0, p30->min_source_rowid);
    EXPECT_EQ(0, p30->max_source_rowid);

    // roaring round-trips to the exact covered set for col 10.
    roaring::Roaring r10 = roaring::Roaring::read(p10->roaring.data(), /*portable=*/true);
    EXPECT_TRUE(r10.contains(0));
    EXPECT_TRUE(r10.contains(5));
    EXPECT_TRUE(r10.contains(7));
    EXPECT_FALSE(r10.contains(1));
}

TEST(SdcgOverlayMergeTest, SingleLayerIsIdentity) {
    std::vector<OverlaySparseLayer> layers;
    layers.push_back(OverlaySparseLayer{
            /*version=*/7, /*column_uids=*/{42}, /*source_rowids=*/{3, 9, 11},
            /*values=*/{make_int32_col({30, 90, 110})}});
    auto res = merge_overlay_layers(layers);
    ASSERT_TRUE(res.ok()) << res.status();
    const MergedOverlay& m = res.value();
    EXPECT_EQ(3, m.num_rows);
    ASSERT_EQ(1u, m.column_uids.size());
    EXPECT_EQ(42, m.column_uids[0]);
    EXPECT_EQ(30, int32_at(m.value_columns[0], 0));
    EXPECT_EQ(90, int32_at(m.value_columns[0], 1));
    EXPECT_EQ(110, int32_at(m.value_columns[0], 2));
    ASSERT_EQ(1u, m.presences.size());
    EXPECT_EQ(3, m.presences[0].count);
}

TEST(SdcgOverlayMergeTest, EmptyLayersError) {
    std::vector<OverlaySparseLayer> layers;
    EXPECT_FALSE(merge_overlay_layers(layers).ok());
}

TEST(SdcgOverlayMergeTest, SizeMismatchError) {
    std::vector<OverlaySparseLayer> layers;
    // 2 source_rowids but value column has 1 row -> inconsistent.
    layers.push_back(OverlaySparseLayer{
            /*version=*/1, /*column_uids=*/{10}, /*source_rowids=*/{0, 1}, /*values=*/{make_int32_col({100})}});
    EXPECT_FALSE(merge_overlay_layers(layers).ok());
}

} // namespace starrocks::lake
