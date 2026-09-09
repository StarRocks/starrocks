// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "storage/lake/shard_write_txn_log.h"

#include <gtest/gtest.h>

#include <limits>

#include "base/testutil/assert.h"
#include "fmt/format.h"

namespace starrocks::lake {

namespace {

constexpr int64_t kTabletId = 1001;
constexpr int64_t kTxnId = 77;
constexpr int64_t kPartitionId = 55;
constexpr uint32_t kUnknownDelOpOffset = std::numeric_limits<uint32_t>::max();

// One shard-write node's contribution: |segments| segments, each with its own sst when |with_sst|.
TxnLogPB make_log(const std::string& node, int segments, bool with_sst, int64_t rows_per_segment) {
    TxnLogPB log;
    log.set_tablet_id(kTabletId);
    log.set_txn_id(kTxnId);
    log.set_partition_id(kPartitionId);
    auto* op_write = log.mutable_op_write();
    auto* rowset = op_write->mutable_rowset();
    for (int i = 0; i < segments; i++) {
        auto* seg = rowset->add_segment_metas();
        seg->set_filename(fmt::format("{}_seg{}.dat", node, i));
        seg->set_size(1024 * (i + 1));
        seg->set_num_rows(rows_per_segment);
        seg->set_segment_idx(i);
        if (with_sst) {
            op_write->add_ssts()->set_name(fmt::format("{}_sst{}.sst", node, i));
            op_write->add_sst_ranges()->set_start_key(fmt::format("{}{}", node, i));
        }
    }
    rowset->set_num_rows(static_cast<int64_t>(segments) * rows_per_segment);
    rowset->set_data_size(1024 * segments);
    rowset->set_overlapped(segments > 1);
    return log;
}

} // namespace

TEST(ShardWriteTxnLogTest, merge_appends_segments_and_renumbers) {
    auto dst = make_log("a", 2, true, 10);
    auto src = make_log("b", 3, true, 7);

    ASSERT_OK(merge_shard_write_txn_log(&dst, &src));

    const auto& rowset = dst.op_write().rowset();
    ASSERT_EQ(5, rowset.segment_metas_size());
    EXPECT_EQ("a_seg0.dat", rowset.segment_metas(0).filename());
    EXPECT_EQ("a_seg1.dat", rowset.segment_metas(1).filename());
    EXPECT_EQ("b_seg0.dat", rowset.segment_metas(2).filename());
    EXPECT_EQ("b_seg2.dat", rowset.segment_metas(4).filename());
    // The position of a segment IS its rowset-local id, so the appended ones are renumbered.
    for (int i = 0; i < rowset.segment_metas_size(); i++) {
        EXPECT_EQ(i, rowset.segment_metas(i).segment_idx()) << "segment " << i;
    }
    EXPECT_EQ(2 * 10 + 3 * 7, rowset.num_rows());
    EXPECT_EQ(1024 * 2 + 1024 * 3, rowset.data_size());
    EXPECT_TRUE(rowset.overlapped());

    // ssts stay positionally aligned with the segments: publish indexes them by segment id.
    ASSERT_EQ(5, dst.op_write().ssts_size());
    EXPECT_EQ("a_sst0.sst", dst.op_write().ssts(0).name());
    EXPECT_EQ("b_sst0.sst", dst.op_write().ssts(2).name());
    ASSERT_EQ(5, dst.op_write().sst_ranges_size());
    EXPECT_EQ("b0", dst.op_write().sst_ranges(2).start_key());
}

TEST(ShardWriteTxnLogTest, merge_shifts_del_op_offsets_past_earlier_segments) {
    auto dst = make_log("a", 2, false, 10);
    dst.mutable_op_write()->add_dels_meta()->set_name("a.del");
    dst.mutable_op_write()->add_del_op_offsets(1);
    dst.mutable_op_write()->add_del_num_rows(3);

    auto src = make_log("b", 3, false, 10);
    src.mutable_op_write()->add_dels_meta()->set_name("b0.del");
    src.mutable_op_write()->add_del_op_offsets(0);
    src.mutable_op_write()->add_del_num_rows(5);
    src.mutable_op_write()->add_dels_meta()->set_name("b1.del");
    src.mutable_op_write()->add_del_op_offsets(2);
    src.mutable_op_write()->add_del_num_rows(6);

    ASSERT_OK(merge_shard_write_txn_log(&dst, &src));

    const auto& op_write = dst.op_write();
    ASSERT_EQ(3, op_write.dels_meta_size());
    ASSERT_EQ(3, op_write.del_op_offsets_size());
    ASSERT_EQ(3, op_write.del_num_rows_size());
    EXPECT_EQ(1, op_write.del_op_offsets(0));
    // b's dels followed b's own segments, which now start at index 2.
    EXPECT_EQ(0 + 2, op_write.del_op_offsets(1));
    EXPECT_EQ(2 + 2, op_write.del_op_offsets(2));
    EXPECT_EQ(5, op_write.del_num_rows(1));
}

TEST(ShardWriteTxnLogTest, merge_keeps_unknown_del_op_offset_sentinel) {
    auto dst = make_log("a", 1, false, 10);
    dst.mutable_op_write()->add_dels_meta()->set_name("a.del");
    dst.mutable_op_write()->add_del_op_offsets(0);
    dst.mutable_op_write()->add_del_num_rows(1);

    auto src = make_log("b", 1, false, 10);
    src.mutable_op_write()->add_dels_meta()->set_name("b.del");
    src.mutable_op_write()->add_del_op_offsets(kUnknownDelOpOffset);
    src.mutable_op_write()->add_del_num_rows(1);

    ASSERT_OK(merge_shard_write_txn_log(&dst, &src));
    ASSERT_EQ(2, dst.op_write().del_op_offsets_size());
    EXPECT_EQ(kUnknownDelOpOffset, dst.op_write().del_op_offsets(1));
}

TEST(ShardWriteTxnLogTest, merge_drops_partial_del_op_offsets) {
    auto dst = make_log("a", 1, false, 10);
    dst.mutable_op_write()->add_dels_meta()->set_name("a.del");
    dst.mutable_op_write()->add_del_op_offsets(0);

    // A contributor that did not record offsets at all: keeping dst's array would leave it
    // misaligned with dels_meta, so it must be dropped entirely.
    auto src = make_log("b", 1, false, 10);
    src.mutable_op_write()->add_dels_meta()->set_name("b.del");

    ASSERT_OK(merge_shard_write_txn_log(&dst, &src));
    EXPECT_EQ(2, dst.op_write().dels_meta_size());
    EXPECT_EQ(0, dst.op_write().del_op_offsets_size());
}

TEST(ShardWriteTxnLogTest, merge_rejects_mixed_sst_presence) {
    auto dst = make_log("a", 2, true, 10);
    auto src = make_log("b", 2, false, 10);
    // Silently accepting this would leave 2 ssts for 4 segments and publish would stamp each sst
    // with the wrong segment id.
    EXPECT_FALSE(merge_shard_write_txn_log(&dst, &src).ok());
}

TEST(ShardWriteTxnLogTest, merge_accepts_empty_contributor) {
    auto dst = make_log("a", 2, true, 10);
    auto src = make_log("b", 0, false, 0);
    ASSERT_OK(merge_shard_write_txn_log(&dst, &src));
    EXPECT_EQ(2, dst.op_write().rowset().segment_metas_size());
    EXPECT_EQ(2, dst.op_write().ssts_size());
    EXPECT_EQ(20, dst.op_write().rowset().num_rows());
}

TEST(ShardWriteTxnLogTest, merge_into_empty_contributor) {
    auto dst = make_log("a", 0, false, 0);
    auto src = make_log("b", 2, true, 10);
    ASSERT_OK(merge_shard_write_txn_log(&dst, &src));
    ASSERT_EQ(2, dst.op_write().rowset().segment_metas_size());
    ASSERT_EQ(2, dst.op_write().ssts_size());
    EXPECT_EQ("b_seg0.dat", dst.op_write().rowset().segment_metas(0).filename());
    EXPECT_EQ(20, dst.op_write().rowset().num_rows());
}

TEST(ShardWriteTxnLogTest, merge_rejects_different_targets) {
    auto dst = make_log("a", 1, false, 10);
    auto src = make_log("b", 1, false, 10);
    src.set_tablet_id(kTabletId + 1);
    EXPECT_FALSE(merge_shard_write_txn_log(&dst, &src).ok());
}

TEST(ShardWriteTxnLogTest, merge_rejects_partial_update) {
    auto dst = make_log("a", 1, false, 10);
    auto src = make_log("b", 1, false, 10);
    src.mutable_op_write()->mutable_txn_meta();
    EXPECT_FALSE(merge_shard_write_txn_log(&dst, &src).ok());
}

TEST(ShardWriteTxnLogTest, merge_pads_seg_delvecs_of_a_contributor_without_any) {
    auto dst = make_log("a", 2, true, 10);
    auto src = make_log("b", 1, true, 10);
    src.mutable_op_write()->add_seg_delvecs()->set_data("dv");

    ASSERT_OK(merge_shard_write_txn_log(&dst, &src));
    // seg_delvecs is indexed like ssts, so the 2 slots of the contributor that emitted none are
    // padded before b's entry, which must land on segment 2.
    ASSERT_EQ(3, dst.op_write().seg_delvecs_size());
    EXPECT_TRUE(dst.op_write().seg_delvecs(0).data().empty());
    EXPECT_TRUE(dst.op_write().seg_delvecs(1).data().empty());
    EXPECT_EQ("dv", dst.op_write().seg_delvecs(2).data());
}

} // namespace starrocks::lake
