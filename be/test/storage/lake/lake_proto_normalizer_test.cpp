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

#include "storage/lake/lake_proto_normalizer.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"

namespace starrocks::lake {

// ---- before_save: structured -> legacy dual-write -----------------------------------------------

TEST(LakeProtoNormalizerTest, before_save_fills_legacy_from_segment_metas) {
    RowsetMetadataPB rs;
    auto* s0 = rs.add_segment_metas();
    s0->set_filename("seg0.dat");
    s0->set_size(100);
    s0->set_encryption_meta("enc0");
    s0->set_shared(true);
    s0->set_bundle_file_offset(0); // 0 is a legitimate value
    auto* s1 = rs.add_segment_metas();
    s1->set_filename("seg1.dat");
    s1->set_size(200);
    s1->set_encryption_meta("enc1");
    s1->set_shared(false);
    s1->set_bundle_file_offset(4096);

    ASSERT_OK(normalize_rowset_before_save(&rs));

    // segment_metas stays the source of truth and is unchanged.
    ASSERT_EQ(2, rs.segment_metas_size());
    // Legacy arrays are filled to match, all-or-nothing per array.
    ASSERT_EQ(2, rs.deprecated_segments_size());
    EXPECT_EQ("seg0.dat", rs.deprecated_segments(0));
    EXPECT_EQ("seg1.dat", rs.deprecated_segments(1));
    ASSERT_EQ(2, rs.deprecated_segment_size_size());
    EXPECT_EQ(100, rs.deprecated_segment_size(0));
    EXPECT_EQ(200, rs.deprecated_segment_size(1));
    ASSERT_EQ(2, rs.deprecated_segment_encryption_metas_size());
    EXPECT_EQ("enc0", rs.deprecated_segment_encryption_metas(0));
    ASSERT_EQ(2, rs.deprecated_shared_segments_size());
    EXPECT_TRUE(rs.deprecated_shared_segments(0));
    EXPECT_FALSE(rs.deprecated_shared_segments(1));
    ASSERT_EQ(2, rs.deprecated_bundle_file_offsets_size());
    EXPECT_EQ(0, rs.deprecated_bundle_file_offsets(0));
    EXPECT_EQ(4096, rs.deprecated_bundle_file_offsets(1));
}

TEST(LakeProtoNormalizerTest, before_save_unencrypted_unbundled_omits_optional_arrays) {
    RowsetMetadataPB rs;
    auto* s0 = rs.add_segment_metas();
    s0->set_filename("seg0.dat");
    s0->set_size(100);
    // no encryption, no shared, no bundle offset

    ASSERT_OK(normalize_rowset_before_save(&rs));

    ASSERT_EQ(1, rs.deprecated_segments_size());
    ASSERT_EQ(1, rs.deprecated_segment_size_size());
    // Optional arrays stay empty when no segment carries the attribute.
    EXPECT_EQ(0, rs.deprecated_segment_encryption_metas_size());
    EXPECT_EQ(0, rs.deprecated_shared_segments_size());
    EXPECT_EQ(0, rs.deprecated_bundle_file_offsets_size());
}

// ---- after_load: legacy -> structured back-fill (old data) --------------------------------------

TEST(LakeProtoNormalizerTest, after_load_backfills_segment_metas_from_legacy_only) {
    // Simulates a rowset written by a pre-feature BE: only the legacy parallel arrays are set.
    RowsetMetadataPB rs;
    rs.add_deprecated_segments("seg0.dat");
    rs.add_deprecated_segments("seg1.dat");
    rs.add_deprecated_segment_size(100);
    rs.add_deprecated_segment_size(200);
    rs.add_deprecated_segment_encryption_metas("enc0");
    rs.add_deprecated_segment_encryption_metas("enc1");
    rs.add_deprecated_shared_segments(true);
    rs.add_deprecated_shared_segments(false);
    rs.add_deprecated_bundle_file_offsets(0);
    rs.add_deprecated_bundle_file_offsets(4096);

    normalize_rowset_after_load(&rs);

    ASSERT_EQ(2, rs.segment_metas_size());
    EXPECT_EQ("seg0.dat", rs.segment_metas(0).filename());
    EXPECT_EQ(100, rs.segment_metas(0).size());
    EXPECT_EQ("enc0", rs.segment_metas(0).encryption_meta());
    EXPECT_TRUE(rs.segment_metas(0).shared());
    EXPECT_TRUE(rs.segment_metas(0).has_bundle_file_offset());
    EXPECT_EQ(0, rs.segment_metas(0).bundle_file_offset());
    EXPECT_EQ("seg1.dat", rs.segment_metas(1).filename());
    EXPECT_EQ(4096, rs.segment_metas(1).bundle_file_offset());
    // Legacy arrays are dropped once back-filled; segment_metas is the sole in-memory source.
    EXPECT_EQ(0, rs.deprecated_segments_size());
    EXPECT_EQ(0, rs.deprecated_segment_size_size());
    EXPECT_EQ(0, rs.deprecated_segment_encryption_metas_size());
    EXPECT_EQ(0, rs.deprecated_shared_segments_size());
    EXPECT_EQ(0, rs.deprecated_bundle_file_offsets_size());
}

TEST(LakeProtoNormalizerTest, after_load_merges_file_attrs_into_existing_sortkey_only_segment_metas) {
    // A rowset from a BE that had segment_metas (sort keys/num_rows) but NOT the new file-attr
    // fields, which still live in the legacy arrays.
    RowsetMetadataPB rs;
    auto* s0 = rs.add_segment_metas();
    s0->set_num_rows(10);
    s0->set_segment_idx(7);
    auto* s1 = rs.add_segment_metas();
    s1->set_num_rows(20);
    s1->set_segment_idx(8);
    rs.add_deprecated_segments("seg0.dat");
    rs.add_deprecated_segments("seg1.dat");
    rs.add_deprecated_segment_size(100);
    rs.add_deprecated_segment_size(200);

    normalize_rowset_after_load(&rs);

    ASSERT_EQ(2, rs.segment_metas_size());
    // Pre-existing fields preserved.
    EXPECT_EQ(10, rs.segment_metas(0).num_rows());
    EXPECT_EQ(7, rs.segment_metas(0).segment_idx());
    // File attrs merged in from legacy.
    EXPECT_EQ("seg0.dat", rs.segment_metas(0).filename());
    EXPECT_EQ(100, rs.segment_metas(0).size());
    EXPECT_EQ("seg1.dat", rs.segment_metas(1).filename());
    EXPECT_EQ(20, rs.segment_metas(1).num_rows());
}

// ---- round-trip + consistency -------------------------------------------------------------------

TEST(LakeProtoNormalizerTest, round_trip_save_then_load_is_consistent) {
    RowsetMetadataPB produced;
    for (int i = 0; i < 3; ++i) {
        auto* s = produced.add_segment_metas();
        s->set_filename("seg" + std::to_string(i) + ".dat");
        s->set_size(100 * (i + 1));
        s->set_num_rows(10 * (i + 1));
        s->set_segment_idx(i);
    }
    ASSERT_OK(normalize_rowset_before_save(&produced));

    // On disk (the produced copy) both segment_metas and the legacy arrays exist, so an old BE can
    // still read it.
    ASSERT_EQ(3, produced.deprecated_segments_size());

    RowsetMetadataPB reloaded = produced;
    normalize_rowset_after_load(&reloaded);

    ASSERT_EQ(3, reloaded.segment_metas_size());
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(produced.segment_metas(i).filename(), reloaded.segment_metas(i).filename());
        EXPECT_EQ(produced.segment_metas(i).size(), reloaded.segment_metas(i).size());
        EXPECT_EQ(produced.segment_metas(i).num_rows(), reloaded.segment_metas(i).num_rows());
    }
    // In memory, after_load keeps only segment_metas.
    EXPECT_EQ(0, reloaded.deprecated_segments_size());
    EXPECT_EQ(0, reloaded.deprecated_segment_size_size());
}

// A segment removed from segment_metas (e.g. partial-compaction trim) must NOT be resurrected from
// the stale legacy arrays at save time: before_save rebuilds legacy from segment_metas.
TEST(LakeProtoNormalizerTest, before_save_does_not_resurrect_removed_segment) {
    RowsetMetadataPB rs;
    for (int i = 0; i < 3; ++i) {
        auto* s = rs.add_segment_metas();
        s->set_filename("seg" + std::to_string(i) + ".dat");
        s->set_size(100);
    }
    ASSERT_OK(normalize_rowset_before_save(&rs));
    ASSERT_EQ(3, rs.deprecated_segments_size());

    // Now drop the middle segment from the canonical list, leaving legacy stale.
    rs.mutable_segment_metas()->erase(rs.mutable_segment_metas()->begin() + 1);
    ASSERT_EQ(2, rs.segment_metas_size());
    ASSERT_EQ(3, rs.deprecated_segments_size()); // legacy still stale here

    ASSERT_OK(normalize_rowset_before_save(&rs));
    // Legacy rebuilt to exactly match segment_metas; the removed segment is gone.
    ASSERT_EQ(2, rs.deprecated_segments_size());
    EXPECT_EQ("seg0.dat", rs.deprecated_segments(0));
    EXPECT_EQ("seg2.dat", rs.deprecated_segments(1));
}

// A rowset must be either fully bundled or fully standalone. before_save rejects a mixed state,
// because the flat legacy bundle_file_offsets array cannot represent "absent" (offset 0 is valid).
TEST(LakeProtoNormalizerTest, before_save_rejects_mixed_bundle_file_offsets) {
    RowsetMetadataPB rowset;
    auto* s0 = rowset.add_segment_metas();
    s0->set_filename("seg0.dat");
    s0->set_bundle_file_offset(0);
    auto* s1 = rowset.add_segment_metas();
    s1->set_filename("seg1.dat"); // no bundle_file_offset -> mixed

    auto status = normalize_rowset_before_save(&rowset);
    EXPECT_TRUE(status.is_corruption()) << status.to_string();
}

// before_save must NOT wipe legacy arrays when segment_metas is empty (e.g. metadata built directly
// from the legacy fields, such as json_to_pb test data). There is no authoritative structured source
// to rebuild from, so the legacy arrays must be left intact.
TEST(LakeProtoNormalizerTest, before_save_preserves_legacy_when_segment_metas_empty) {
    RowsetMetadataPB rowset;
    rowset.add_deprecated_segments("seg0.dat");
    rowset.add_deprecated_segments("seg1.dat");
    rowset.add_deprecated_segment_size(100);
    rowset.add_deprecated_segment_size(200);
    rowset.add_deprecated_bundle_file_offsets(0);
    rowset.add_deprecated_bundle_file_offsets(1000);

    ASSERT_OK(normalize_rowset_before_save(&rowset));

    ASSERT_EQ(0, rowset.segment_metas_size());
    ASSERT_EQ(2, rowset.deprecated_segments_size());
    EXPECT_EQ("seg0.dat", rowset.deprecated_segments(0));
    EXPECT_EQ("seg1.dat", rowset.deprecated_segments(1));
    ASSERT_EQ(2, rowset.deprecated_segment_size_size());
    ASSERT_EQ(2, rowset.deprecated_bundle_file_offsets_size());

    TxnLogPB::OpWrite op_write;
    op_write.add_deprecated_dels("del0");
    op_write.add_deprecated_rewrite_segments("rewrite0");
    ASSERT_OK(normalize_op_write_before_save(&op_write));
    ASSERT_EQ(0, op_write.dels_meta_size());
    ASSERT_EQ(1, op_write.deprecated_dels_size());
    EXPECT_EQ("del0", op_write.deprecated_dels(0));
    ASSERT_EQ(1, op_write.deprecated_rewrite_segments_size());
    EXPECT_EQ("rewrite0", op_write.deprecated_rewrite_segments(0));
}

// ---- OpWrite dels / rewrite_segments ------------------------------------------------------------

TEST(LakeProtoNormalizerTest, opwrite_dels_and_rewrite_segments_round_trip) {
    TxnLogPB::OpWrite op;
    auto* d0 = op.add_dels_meta();
    d0->set_name("del0");
    d0->set_encryption_meta("denc0");
    d0->set_shared(true);
    auto* d1 = op.add_dels_meta();
    d1->set_name("del1");
    d1->set_encryption_meta("denc1");
    d1->set_shared(false);
    op.add_rewrite_segments_meta()->set_name("rw0");

    ASSERT_OK(normalize_op_write_before_save(&op));
    ASSERT_EQ(2, op.deprecated_dels_size());
    EXPECT_EQ("del0", op.deprecated_dels(0));
    ASSERT_EQ(2, op.deprecated_del_encryption_metas_size());
    EXPECT_EQ("denc1", op.deprecated_del_encryption_metas(1));
    ASSERT_EQ(2, op.deprecated_shared_dels_size());
    EXPECT_TRUE(op.deprecated_shared_dels(0));
    ASSERT_EQ(1, op.deprecated_rewrite_segments_size());
    EXPECT_EQ("rw0", op.deprecated_rewrite_segments(0));

    // Legacy-only old log -> back-fill structured fields.
    TxnLogPB::OpWrite old_op;
    old_op.add_deprecated_dels("del0");
    old_op.add_deprecated_dels("del1");
    old_op.add_deprecated_del_encryption_metas("denc0");
    old_op.add_deprecated_del_encryption_metas("denc1");
    old_op.add_deprecated_rewrite_segments("rw0");
    normalize_op_write_after_load(&old_op);
    ASSERT_EQ(2, old_op.dels_meta_size());
    EXPECT_EQ("del0", old_op.dels_meta(0).name());
    EXPECT_EQ("denc1", old_op.dels_meta(1).encryption_meta());
    ASSERT_EQ(1, old_op.rewrite_segments_meta_size());
    EXPECT_EQ("rw0", old_op.rewrite_segments_meta(0).name());
    // Legacy arrays dropped once back-filled.
    EXPECT_EQ(0, old_op.deprecated_dels_size());
    EXPECT_EQ(0, old_op.deprecated_del_encryption_metas_size());
    EXPECT_EQ(0, old_op.deprecated_rewrite_segments_size());
}

} // namespace starrocks::lake
