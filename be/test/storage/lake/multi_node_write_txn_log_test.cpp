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

#include "storage/lake/multi_node_write_txn_log.h"

#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "fmt/format.h"

namespace starrocks::lake {

namespace {

constexpr int64_t kTabletId = 1001;
constexpr int64_t kTxnId = 77;
constexpr int64_t kPartitionId = 55;
constexpr uint32_t kUnknownDelOpOffset = std::numeric_limits<uint32_t>::max();

// One multi-node write node's contribution: |segments| segments, each with its own sst when |with_sst|.
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

TEST(MultiNodeWriteTxnLogTest, merge_appends_segments_and_renumbers) {
    auto dst = make_log("a", 2, true, 10);
    auto src = make_log("b", 3, true, 7);

    ASSERT_OK(merge_multi_node_write_txn_log(&dst, &src));

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

TEST(MultiNodeWriteTxnLogTest, merge_shifts_del_op_offsets_past_earlier_segments) {
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

    ASSERT_OK(merge_multi_node_write_txn_log(&dst, &src));

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

TEST(MultiNodeWriteTxnLogTest, merge_keeps_unknown_del_op_offset_sentinel) {
    auto dst = make_log("a", 1, false, 10);
    dst.mutable_op_write()->add_dels_meta()->set_name("a.del");
    dst.mutable_op_write()->add_del_op_offsets(0);
    dst.mutable_op_write()->add_del_num_rows(1);

    auto src = make_log("b", 1, false, 10);
    src.mutable_op_write()->add_dels_meta()->set_name("b.del");
    src.mutable_op_write()->add_del_op_offsets(kUnknownDelOpOffset);
    src.mutable_op_write()->add_del_num_rows(1);

    ASSERT_OK(merge_multi_node_write_txn_log(&dst, &src));
    ASSERT_EQ(2, dst.op_write().del_op_offsets_size());
    EXPECT_EQ(kUnknownDelOpOffset, dst.op_write().del_op_offsets(1));
}

TEST(MultiNodeWriteTxnLogTest, merge_drops_partial_del_op_offsets) {
    auto dst = make_log("a", 1, false, 10);
    dst.mutable_op_write()->add_dels_meta()->set_name("a.del");
    dst.mutable_op_write()->add_del_op_offsets(0);

    // A contributor that did not record offsets at all: keeping dst's array would leave it
    // misaligned with dels_meta, so it must be dropped entirely.
    auto src = make_log("b", 1, false, 10);
    src.mutable_op_write()->add_dels_meta()->set_name("b.del");

    ASSERT_OK(merge_multi_node_write_txn_log(&dst, &src));
    EXPECT_EQ(2, dst.op_write().dels_meta_size());
    EXPECT_EQ(0, dst.op_write().del_op_offsets_size());
}

TEST(MultiNodeWriteTxnLogTest, merge_accepts_empty_contributor) {
    auto dst = make_log("a", 2, true, 10);
    auto src = make_log("b", 0, false, 0);
    ASSERT_OK(merge_multi_node_write_txn_log(&dst, &src));
    EXPECT_EQ(2, dst.op_write().rowset().segment_metas_size());
    EXPECT_EQ(2, dst.op_write().ssts_size());
    EXPECT_EQ(20, dst.op_write().rowset().num_rows());
}

TEST(MultiNodeWriteTxnLogTest, merge_into_empty_contributor) {
    auto dst = make_log("a", 0, false, 0);
    auto src = make_log("b", 2, true, 10);
    ASSERT_OK(merge_multi_node_write_txn_log(&dst, &src));
    ASSERT_EQ(2, dst.op_write().rowset().segment_metas_size());
    ASSERT_EQ(2, dst.op_write().ssts_size());
    EXPECT_EQ("b_seg0.dat", dst.op_write().rowset().segment_metas(0).filename());
    EXPECT_EQ(20, dst.op_write().rowset().num_rows());
}

TEST(MultiNodeWriteTxnLogTest, merge_rejects_different_targets) {
    auto dst = make_log("a", 1, false, 10);
    auto src = make_log("b", 1, false, 10);
    src.set_tablet_id(kTabletId + 1);
    EXPECT_FALSE(merge_multi_node_write_txn_log(&dst, &src).ok());
}

TEST(MultiNodeWriteTxnLogTest, merge_pads_seg_delvecs_of_a_contributor_without_any) {
    auto dst = make_log("a", 2, true, 10);
    auto src = make_log("b", 1, true, 10);
    src.mutable_op_write()->add_seg_delvecs()->set_data("dv");

    ASSERT_OK(merge_multi_node_write_txn_log(&dst, &src));
    // seg_delvecs is indexed like ssts, so the 2 slots of the contributor that emitted none are
    // padded before b's entry, which must land on segment 2.
    ASSERT_EQ(3, dst.op_write().seg_delvecs_size());
    EXPECT_TRUE(dst.op_write().seg_delvecs(0).data().empty());
    EXPECT_TRUE(dst.op_write().seg_delvecs(1).data().empty());
    EXPECT_EQ("dv", dst.op_write().seg_delvecs(2).data());
}

// Multi-node write is only offered on file-bundling tables, so the shape the fold really sees is
// bundled: every segment carries its OWN bundle filename plus a bundle_file_offset into it, and each
// node opened a different bundle file. Folding must keep every segment pointing at the bundle its
// own node wrote -- a rowset-level filename would have made this impossible -- and must leave the
// "all segments have an offset or none do" property intact, which is what publish checks before it
// merges the rowsets (NonPrimaryKeyTxnLogApplier).
TEST(MultiNodeWriteTxnLogTest, merge_keeps_each_segment_on_its_own_bundle_file) {
    auto bundled = [](const std::string& node, int segments) {
        auto log = make_log(node, segments, false, 10);
        auto* rowset = log.mutable_op_write()->mutable_rowset();
        for (int i = 0; i < rowset->segment_metas_size(); i++) {
            auto* seg = rowset->mutable_segment_metas(i);
            // One physical file per node; the segments differ only by their offset into it.
            seg->set_filename(fmt::format("{}_bundle.dat", node));
            seg->set_bundle_file_offset(4096 * (i + 1));
        }
        return log;
    };
    auto dst = bundled("a", 2);
    auto src = bundled("b", 3);

    ASSERT_OK(merge_multi_node_write_txn_log(&dst, &src));

    const auto& rowset = dst.op_write().rowset();
    ASSERT_EQ(5, rowset.segment_metas_size());
    const std::vector<std::pair<std::string, int64_t>> expected = {{"a_bundle.dat", 4096},
                                                                   {"a_bundle.dat", 8192},
                                                                   {"b_bundle.dat", 4096},
                                                                   {"b_bundle.dat", 8192},
                                                                   {"b_bundle.dat", 12288}};
    for (int i = 0; i < rowset.segment_metas_size(); i++) {
        EXPECT_EQ(expected[i].first, rowset.segment_metas(i).filename()) << "segment " << i;
        ASSERT_TRUE(rowset.segment_metas(i).has_bundle_file_offset()) << "segment " << i;
        EXPECT_EQ(expected[i].second, rowset.segment_metas(i).bundle_file_offset()) << "segment " << i;
        EXPECT_EQ(i, rowset.segment_metas(i).segment_idx()) << "segment " << i;
    }
}

// A node that received no rows still finishes its writer and hands back an EMPTY op_write (the eos
// request declares the partition sink-wide, not per node), so under local-first routing this is the
// common case rather than an edge one: only the nodes that actually got rows contribute segments.
// The empty contributor must not turn a bundled fold into a mixed one.
TEST(MultiNodeWriteTxnLogTest, merge_empty_contributor_keeps_bundle_offsets_uniform) {
    auto dst = make_log("a", 2, false, 10);
    for (int i = 0; i < 2; i++) {
        dst.mutable_op_write()->mutable_rowset()->mutable_segment_metas(i)->set_bundle_file_offset(4096 * (i + 1));
    }
    auto empty = make_log("b", 0, false, 0);

    ASSERT_OK(merge_multi_node_write_txn_log(&dst, &empty));

    const auto& rowset = dst.op_write().rowset();
    ASSERT_EQ(2, rowset.segment_metas_size());
    for (int i = 0; i < rowset.segment_metas_size(); i++) {
        EXPECT_TRUE(rowset.segment_metas(i).has_bundle_file_offset()) << "segment " << i;
    }
    EXPECT_EQ(2 * 10, rowset.num_rows());
}

// A load that writes only some columns, updates on a condition, or omits its auto-increment column
// makes BE attach RowsetTxnMetaPB. These used to be refused outright; the fold now carries them.
namespace {
void set_partial_update_meta(TxnLogPB* log, const std::string& condition) {
    auto* meta = log->mutable_op_write()->mutable_txn_meta();
    meta->set_partial_update_mode(PartialUpdateMode::ROW_MODE);
    meta->add_partial_update_column_ids(0);
    meta->add_partial_update_column_ids(3);
    meta->set_merge_condition(condition);
    // A proto map: its serialization order is unspecified, which is why the fold compares with
    // MessageDifferencer instead of comparing serialized bytes.
    (*meta->mutable_column_to_expr_value())["c1"] = "1";
    (*meta->mutable_column_to_expr_value())["c2"] = "2";
}
} // namespace

TEST(MultiNodeWriteTxnLogTest, merge_keeps_one_copy_of_agreeing_txn_meta) {
    auto dst = make_log("a", 2, false, 10);
    auto src = make_log("b", 3, false, 10);
    set_partial_update_meta(&dst, "v1");
    set_partial_update_meta(&src, "v1");
    // One rewrite segment per segment, which is how delta_writer emits them.
    for (int i = 0; i < 2; i++) {
        dst.mutable_op_write()->add_rewrite_segments_meta()->set_name("a_rw" + std::to_string(i) + ".dat");
    }
    for (int i = 0; i < 3; i++) {
        src.mutable_op_write()->add_rewrite_segments_meta()->set_name("b_rw" + std::to_string(i) + ".dat");
    }

    ASSERT_OK(merge_multi_node_write_txn_log(&dst, &src));

    // txn_meta describes the load, so the folded log carries exactly one -- not a concatenation.
    ASSERT_TRUE(dst.op_write().has_txn_meta());
    EXPECT_EQ("v1", dst.op_write().txn_meta().merge_condition());
    ASSERT_EQ(2, dst.op_write().txn_meta().partial_update_column_ids_size());
    // Rewrite segments ride along with their segments and stay positionally paired.
    ASSERT_EQ(5, dst.op_write().rowset().segment_metas_size());
    ASSERT_EQ(5, dst.op_write().rewrite_segments_meta_size());
    EXPECT_EQ("a_rw0.dat", dst.op_write().rewrite_segments_meta(0).name());
    EXPECT_EQ("b_rw0.dat", dst.op_write().rewrite_segments_meta(2).name());
    EXPECT_EQ("b_rw2.dat", dst.op_write().rewrite_segments_meta(4).name());
}

TEST(MultiNodeWriteTxnLogTest, merge_rejects_disagreeing_txn_meta) {
    auto dst = make_log("a", 1, false, 10);
    auto src = make_log("b", 1, false, 10);
    set_partial_update_meta(&dst, "v1");
    set_partial_update_meta(&src, "v2");
    // Every writer builds txn_meta from the same plan, so a difference is not something to merge --
    // it means the writers were not running the same load.
    EXPECT_FALSE(merge_multi_node_write_txn_log(&dst, &src).ok());
}

TEST(MultiNodeWriteTxnLogTest, merge_rejects_txn_meta_on_only_one_side) {
    auto dst = make_log("a", 1, false, 10);
    auto src = make_log("b", 1, false, 10);
    set_partial_update_meta(&src, "v1");
    EXPECT_FALSE(merge_multi_node_write_txn_log(&dst, &src).ok());
}

TEST(MultiNodeWriteTxnLogTest, merge_rejects_rewrite_segments_that_do_not_cover_segments) {
    auto dst = make_log("a", 2, false, 10);
    auto src = make_log("b", 2, false, 10);
    set_partial_update_meta(&dst, "v1");
    set_partial_update_meta(&src, "v1");
    dst.mutable_op_write()->add_rewrite_segments_meta()->set_name("a_rw0.dat");
    dst.mutable_op_write()->add_rewrite_segments_meta()->set_name("a_rw1.dat");
    // One short: publish resolves these positionally, so a missing entry would shift the rest onto
    // the wrong segments.
    src.mutable_op_write()->add_rewrite_segments_meta()->set_name("b_rw0.dat");
    EXPECT_FALSE(merge_multi_node_write_txn_log(&dst, &src).ok());
}

TEST(MultiNodeWriteTxnLogTest, merge_allows_column_mode_without_rewrite_segments) {
    // Column-mode partial update attaches txn_meta but no rewrite segments at all.
    auto dst = make_log("a", 2, false, 10);
    auto src = make_log("b", 1, false, 10);
    dst.mutable_op_write()->mutable_txn_meta()->set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE);
    src.mutable_op_write()->mutable_txn_meta()->set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE);

    ASSERT_OK(merge_multi_node_write_txn_log(&dst, &src));
    EXPECT_EQ(3, dst.op_write().rowset().segment_metas_size());
    EXPECT_EQ(0, dst.op_write().rewrite_segments_meta_size());
    EXPECT_EQ(PartialUpdateMode::COLUMN_UPDATE_MODE, dst.op_write().txn_meta().partial_update_mode());
}

TEST(MultiNodeWriteTxnLogTest, merge_pads_ssts_for_a_writer_that_built_none) {
    // Whether a segment's sstable is built eagerly is decided per segment against a size threshold,
    // so one writer can come with ssts and another without. That is an ordinary load: the fold pads
    // rather than refusing, and publish reads the array per segment.
    auto dst = make_log("a", 2, true, 10);
    auto src = make_log("b", 3, false, 10);

    ASSERT_OK(merge_multi_node_write_txn_log(&dst, &src));

    const auto& op = dst.op_write();
    ASSERT_EQ(5, op.rowset().segment_metas_size());
    // Segment-aligned, which is what lets publish index it by segment at all.
    ASSERT_EQ(5, op.ssts_size());
    ASSERT_EQ(5, op.sst_ranges_size());
    EXPECT_EQ("a_sst0.sst", op.ssts(0).name());
    EXPECT_EQ("a_sst1.sst", op.ssts(1).name());
    // The contributor that built none leaves empty names -- publish's "no pre-built sst here".
    EXPECT_TRUE(op.ssts(2).name().empty());
    EXPECT_TRUE(op.ssts(3).name().empty());
    EXPECT_TRUE(op.ssts(4).name().empty());
}

TEST(MultiNodeWriteTxnLogTest, merge_pads_ssts_when_the_first_writer_built_none) {
    // The same the other way round: the padding has to reach the segments already in dst, not only
    // the ones being appended.
    auto dst = make_log("a", 2, false, 10);
    auto src = make_log("b", 1, true, 10);

    ASSERT_OK(merge_multi_node_write_txn_log(&dst, &src));

    const auto& op = dst.op_write();
    ASSERT_EQ(3, op.rowset().segment_metas_size());
    ASSERT_EQ(3, op.ssts_size());
    EXPECT_TRUE(op.ssts(0).name().empty());
    EXPECT_TRUE(op.ssts(1).name().empty());
    EXPECT_EQ("b_sst0.sst", op.ssts(2).name());
}

TEST(MultiNodeWriteTxnLogTest, merge_keeps_no_ssts_when_nobody_built_any) {
    auto dst = make_log("a", 2, false, 10);
    auto src = make_log("b", 2, false, 10);

    ASSERT_OK(merge_multi_node_write_txn_log(&dst, &src));
    EXPECT_EQ(0, dst.op_write().ssts_size());
    EXPECT_EQ(0, dst.op_write().sst_ranges_size());
}

TEST(MultiNodeWriteTxnLogTest, merge_concatenates_partial_rowset_footers) {
    // partial_rowset_footers is the per-SEGMENT half of txn_meta: publish reads
    // txn_meta.partial_rowset_footers(segment_id), so each writer's entries belong to its own
    // segments and have to end up behind them. Keeping only one side's copy -- which an equality
    // check on the whole message would have forced -- makes every appended segment read a footer
    // that describes a different file.
    auto dst = make_log("a", 2, false, 10);
    auto src = make_log("b", 3, false, 10);
    for (auto* log : {&dst, &src}) {
        auto* meta = log->mutable_op_write()->mutable_txn_meta();
        meta->set_partial_update_mode(PartialUpdateMode::ROW_MODE);
        meta->add_partial_update_column_ids(0);
    }
    for (int i = 0; i < 2; i++) {
        auto* f = dst.mutable_op_write()->mutable_txn_meta()->add_partial_rowset_footers();
        f->set_position(100 + i);
        f->set_size(10 + i);
    }
    for (int i = 0; i < 3; i++) {
        auto* f = src.mutable_op_write()->mutable_txn_meta()->add_partial_rowset_footers();
        f->set_position(200 + i);
        f->set_size(20 + i);
    }

    ASSERT_OK(merge_multi_node_write_txn_log(&dst, &src));

    const auto& meta = dst.op_write().txn_meta();
    ASSERT_EQ(5, dst.op_write().rowset().segment_metas_size());
    ASSERT_EQ(5, meta.partial_rowset_footers_size());
    EXPECT_EQ(100, meta.partial_rowset_footers(0).position());
    EXPECT_EQ(101, meta.partial_rowset_footers(1).position());
    EXPECT_EQ(200, meta.partial_rowset_footers(2).position());
    EXPECT_EQ(202, meta.partial_rowset_footers(4).position());
    // The load-scope fields still collapse to one copy rather than being concatenated.
    EXPECT_EQ(1, meta.partial_update_column_ids_size());
    EXPECT_EQ(PartialUpdateMode::ROW_MODE, meta.partial_update_mode());
}

TEST(MultiNodeWriteTxnLogTest, merge_ignores_footers_when_comparing_load_scope_fields) {
    // Differing footers are normal and must not read as writers disagreeing about the load.
    auto dst = make_log("a", 1, false, 10);
    auto src = make_log("b", 1, false, 10);
    dst.mutable_op_write()->mutable_txn_meta()->set_merge_condition("v2");
    src.mutable_op_write()->mutable_txn_meta()->set_merge_condition("v2");
    dst.mutable_op_write()->mutable_txn_meta()->add_partial_rowset_footers()->set_position(1);
    src.mutable_op_write()->mutable_txn_meta()->add_partial_rowset_footers()->set_position(999);

    ASSERT_OK(merge_multi_node_write_txn_log(&dst, &src));
    EXPECT_EQ(2, dst.op_write().txn_meta().partial_rowset_footers_size());
}

TEST(MultiNodeWriteTxnLogTest, merge_rejects_footers_that_do_not_cover_segments) {
    auto dst = make_log("a", 2, false, 10);
    auto src = make_log("b", 2, false, 10);
    for (auto* log : {&dst, &src}) {
        log->mutable_op_write()->mutable_txn_meta()->set_partial_update_mode(PartialUpdateMode::ROW_MODE);
    }
    dst.mutable_op_write()->mutable_txn_meta()->add_partial_rowset_footers()->set_position(1);
    dst.mutable_op_write()->mutable_txn_meta()->add_partial_rowset_footers()->set_position(2);
    // One short for its two segments: publish would read the wrong footer for the second.
    src.mutable_op_write()->mutable_txn_meta()->add_partial_rowset_footers()->set_position(3);
    EXPECT_FALSE(merge_multi_node_write_txn_log(&dst, &src).ok());
}

} // namespace starrocks::lake
