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

#include "storage/delta_column_group.h"

#include <gtest/gtest.h>

#include "fs/key_cache.h"

namespace starrocks {

TEST(TestDeltaColumnGroup, testLoad) {
    auto ep = KeyCache::instance().create_plain_random_encryption_meta_pair().value();
    DeltaColumnGroup dcg;
    dcg.init(100, {{1, 10, 100}}, {"abc0.cols"}, {ep.encryption_meta});
    std::string pb_str = dcg.save();
    DeltaColumnGroup new_dcg;
    ASSERT_TRUE(new_dcg.load(100, pb_str.data(), pb_str.length()).ok());
    ASSERT_TRUE(dcg.column_files("111") == new_dcg.column_files("111"));
    auto v1 = dcg.column_ids();
    auto v2 = new_dcg.column_ids();
    ASSERT_TRUE(v1.size() == v2.size());
    for (int i = 0; i < v1.size(); ++i) {
        ASSERT_TRUE(v1[i].size() == v2[i].size());
    }
    for (int i = 0; i < v1.size(); i++) {
        for (int j = 0; j < v1[i].size(); ++j) {
            ASSERT_TRUE(v1[i][j] == v2[i][j]);
        }
    }
    DeltaColumnGroup dcg2;
    dcg2.init(100, {{2, 12}}, {"abc1.cols"}, {ep.encryption_meta});
    RowsetId rowset_id;
    rowset_id.init(100, 100, 0, 0);
    dcg.merge_by_version(dcg2, "tmp", rowset_id, 100);
    ASSERT_EQ(2, dcg.encryption_metas().size());
};

TEST(TestDeltaColumnGroup, testGet) {
    auto ep = KeyCache::instance().create_plain_random_encryption_meta_pair().value();
    DeltaColumnGroup dcg;
    dcg.init(100, {{10, 1, 100}}, {"abc.cols"}, {ep.encryption_meta});
    ASSERT_TRUE(0 == dcg.get_column_idx(10).first);
    ASSERT_TRUE(0 == dcg.get_column_idx(10).second);

    ASSERT_TRUE(0 == dcg.get_column_idx(1).first);
    ASSERT_TRUE(1 == dcg.get_column_idx(1).second);

    ASSERT_TRUE(0 == dcg.get_column_idx(100).first);
    ASSERT_TRUE(2 == dcg.get_column_idx(100).second);

    ASSERT_TRUE(-1 == dcg.get_column_idx(2).first);
    ASSERT_TRUE(-1 == dcg.get_column_idx(2).second);

    ASSERT_TRUE(-1 == dcg.get_column_idx(1000).first);
    ASSERT_TRUE(-1 == dcg.get_column_idx(1000).second);
};

TEST(TestDeltaColumnGroup, testGC) {
    auto ep = KeyCache::instance().create_plain_random_encryption_meta_pair().value();
    // test1
    {
        // 1 -> {1, 2, 3}
        // 2 -> {2, 3, 4}
        // 3 -> {3, 4, 5}
        // ...
        std::vector<std::pair<TabletSegmentId, int64_t>> garbage_dcgs;
        DeltaColumnGroupList dcgs;
        for (ColumnUID i = 1; i <= 20; i++) {
            DeltaColumnGroup dcg;
            dcg.init((int64_t)i, {{i, i + 1, i + 2}}, {"abc.cols"}, {ep.encryption_meta});
            dcgs.push_back(std::make_shared<DeltaColumnGroup>(dcg));
        }
        DeltaColumnGroupListPB pb;
        DeltaColumnGroupListSerializer::serialize_delta_column_group_list(dcgs, &pb);
        DeltaColumnGroupList dcgs_deserialized;
        DeltaColumnGroupListSerializer::deserialize_delta_column_group_list(pb, &dcgs_deserialized);
        ASSERT_EQ(dcgs.size(), dcgs_deserialized.size());
        const std::string path = "/asd/";
        std::vector<std::string> clear_files;
        DeltaColumnGroupListHelper::garbage_collection(dcgs, TabletSegmentId(100, 100), 10, path, &garbage_dcgs,
                                                       &clear_files);
        ASSERT_TRUE(dcgs.size() == 20);
        ASSERT_TRUE(clear_files.size() == 0);
        ASSERT_TRUE(garbage_dcgs.size() == 0);
        DeltaColumnGroupListHelper::garbage_collection(dcgs, TabletSegmentId(100, 100), 20, path, &garbage_dcgs,
                                                       &clear_files);
        ASSERT_TRUE(dcgs.size() == 20);
        ASSERT_TRUE(clear_files.size() == 0);
        ASSERT_TRUE(garbage_dcgs.size() == 0);
    };
    // test2
    {
        // 1 -> {1, 2, 3}
        // 2 -> {1, 2, 3}
        // 3 -> {1, 2, 3}
        // ...
        std::vector<std::pair<TabletSegmentId, int64_t>> garbage_dcgs;
        DeltaColumnGroupList dcgs;
        for (uint32_t i = 1; i <= 20; i++) {
            DeltaColumnGroup dcg;
            dcg.init((int64_t)i, {{1, 2, 3}}, {"abc.cols"}, {ep.encryption_meta});
            dcgs.push_back(std::make_shared<DeltaColumnGroup>(dcg));
        }
        const std::string path = "/asd/";
        std::vector<std::string> clear_files;
        DeltaColumnGroupListHelper::garbage_collection(dcgs, TabletSegmentId(100, 100), 10, path, &garbage_dcgs,
                                                       &clear_files);
        ASSERT_TRUE(dcgs.size() == 11);
        ASSERT_TRUE(clear_files.size() == 9);
        ASSERT_TRUE(garbage_dcgs.size() == 9);
        DeltaColumnGroupListHelper::garbage_collection(dcgs, TabletSegmentId(100, 100), 20, path, &garbage_dcgs,
                                                       &clear_files);
        ASSERT_TRUE(dcgs.size() == 1);
        ASSERT_TRUE(clear_files.size() == 19);
        ASSERT_TRUE(garbage_dcgs.size() == 19);
    };
    // test3
    {
        // 1 -> {2, 3, 4}
        // 2 -> {1, 2, 3}
        // 3 -> {2, 3, 4}
        // ...
        std::vector<std::pair<TabletSegmentId, int64_t>> garbage_dcgs;
        DeltaColumnGroupList dcgs;
        for (ColumnUID i = 1; i <= 20; i++) {
            DeltaColumnGroup dcg;
            ColumnUID shift = i % 2;
            dcg.init((int64_t)i, {{1 + shift, 2 + shift, 3 + shift}}, {"abc.cols"}, {ep.encryption_meta});
            dcgs.push_back(std::make_shared<DeltaColumnGroup>(dcg));
        }
        const std::string path = "/asd/";
        std::vector<std::string> clear_files;
        DeltaColumnGroupListHelper::garbage_collection(dcgs, TabletSegmentId(100, 100), 10, path, &garbage_dcgs,
                                                       &clear_files);
        ASSERT_TRUE(dcgs.size() == 12);
        ASSERT_TRUE(garbage_dcgs.size() == 8);
        ASSERT_TRUE(clear_files.size() == 8);
        DeltaColumnGroupListHelper::garbage_collection(dcgs, TabletSegmentId(100, 100), 20, path, &garbage_dcgs,
                                                       &clear_files);
        ASSERT_TRUE(dcgs.size() == 2);
        ASSERT_TRUE(garbage_dcgs.size() == 18);
        ASSERT_TRUE(clear_files.size() == 18);
    };
    // test4
    {
        // 1 -> {2, 3, 4}
        // 2 -> {3, 4, 5}
        // 3 -> {1, 2, 3}
        // 4 -> {2, 3, 4}
        // ...
        std::vector<std::pair<TabletSegmentId, int64_t>> garbage_dcgs;
        DeltaColumnGroupList dcgs;
        for (ColumnUID i = 1; i <= 20; i++) {
            DeltaColumnGroup dcg;
            ColumnUID shift = i % 3;
            dcg.init((int64_t)i, {{1 + shift, 2 + shift, 3 + shift}}, {"abc.cols"}, {ep.encryption_meta});
            dcgs.push_back(std::make_shared<DeltaColumnGroup>(dcg));
        }
        const std::string path = "/asd/";
        std::vector<std::string> clear_files;
        DeltaColumnGroupListHelper::garbage_collection(dcgs, TabletSegmentId(100, 100), 10, path, &garbage_dcgs,
                                                       &clear_files);
        ASSERT_TRUE(dcgs.size() == 13);
        ASSERT_TRUE(garbage_dcgs.size() == 7);
        ASSERT_TRUE(clear_files.size() == 7);
        DeltaColumnGroupListHelper::garbage_collection(dcgs, TabletSegmentId(100, 100), 20, path, &garbage_dcgs,
                                                       &clear_files);
        ASSERT_TRUE(dcgs.size() == 3);
        ASSERT_TRUE(garbage_dcgs.size() == 17);
        ASSERT_TRUE(clear_files.size() == 17);
    };
};

TEST(TestDeltaColumnGroup, testDeltaColumnGroupVerPBLoad) {
    auto ep = KeyCache::instance().create_plain_random_encryption_meta_pair().value();
    // version 10 -> aaa.cols -> <3, 4>
    // version 11 -> bbb.cols -> <5, 6>
    // version 12 -> ccc.cols -> <7, 8>
    DeltaColumnGroupVerPB dcg_ver;
    dcg_ver.add_versions(10);
    dcg_ver.add_versions(11);
    dcg_ver.add_versions(12);
    dcg_ver.add_column_files("aaa.cols");
    dcg_ver.add_column_files("bbb.cols");
    dcg_ver.add_column_files("ccc.cols");
    dcg_ver.add_encryption_metas(ep.encryption_meta);
    dcg_ver.add_encryption_metas(ep.encryption_meta);
    dcg_ver.add_encryption_metas(ep.encryption_meta);
    DeltaColumnGroupColumnIdsPB unique_cids;
    unique_cids.add_column_ids(3);
    unique_cids.add_column_ids(4);
    dcg_ver.add_unique_column_ids()->CopyFrom(unique_cids);
    unique_cids.Clear();
    unique_cids.add_column_ids(5);
    unique_cids.add_column_ids(6);
    dcg_ver.add_unique_column_ids()->CopyFrom(unique_cids);
    unique_cids.Clear();
    unique_cids.add_column_ids(7);
    unique_cids.add_column_ids(8);
    dcg_ver.add_unique_column_ids()->CopyFrom(unique_cids);
    unique_cids.Clear();
    DeltaColumnGroup dcg;
    ASSERT_TRUE(dcg.load(12, dcg_ver).ok());
    auto idx = dcg.get_column_idx(4);
    ASSERT_TRUE(idx.first == 0);
    ASSERT_TRUE(idx.second == 1);
    ASSERT_TRUE("tmp/aaa.cols" == dcg.column_files("tmp")[idx.first]);
    ASSERT_TRUE("tmp/aaa.cols" == dcg.column_file_by_idx("tmp", idx.first).value());
    idx = dcg.get_column_idx(5);
    ASSERT_TRUE(idx.first == 1);
    ASSERT_TRUE(idx.second == 0);
    ASSERT_TRUE("tmp/bbb.cols" == dcg.column_files("tmp")[idx.first]);
    ASSERT_TRUE("tmp/bbb.cols" == dcg.column_file_by_idx("tmp", idx.first).value());
    idx = dcg.get_column_idx(8);
    ASSERT_TRUE(idx.first == 2);
    ASSERT_TRUE(idx.second == 1);
    ASSERT_TRUE("tmp/ccc.cols" == dcg.column_files("tmp")[idx.first]);
    ASSERT_TRUE("tmp/ccc.cols" == dcg.column_file_by_idx("tmp", idx.first).value());

    // overflow
    ASSERT_FALSE(dcg.column_file_by_idx("tmp", 100).ok());
}

// SDCG: a DeltaColumnGroupVerPB that carries NO file_kinds / sparse_row_counts /
// source_segment_num_rows (legacy / dense-only metadata) must read back as all-dense via the
// file_kind() out-of-range hinge, and set_sdcg_meta must never be triggered.
TEST(TestDeltaColumnGroup, testSDCGLegacyHingeAllDense) {
    DeltaColumnGroupVerPB dcg_ver;
    dcg_ver.add_versions(10);
    dcg_ver.add_versions(11);
    dcg_ver.add_column_files("aaa.cols");
    dcg_ver.add_column_files("bbb.cols");
    DeltaColumnGroupColumnIdsPB unique_cids;
    unique_cids.add_column_ids(3);
    dcg_ver.add_unique_column_ids()->CopyFrom(unique_cids);
    unique_cids.Clear();
    unique_cids.add_column_ids(4);
    dcg_ver.add_unique_column_ids()->CopyFrom(unique_cids);

    DeltaColumnGroup dcg;
    ASSERT_TRUE(dcg.load(11, dcg_ver).ok());
    // No SDCG arrays installed: vectors stay empty, file_kind() OOR hinge => DENSE.
    ASSERT_TRUE(dcg.file_kinds().empty());
    ASSERT_TRUE(dcg.sparse_row_counts().empty());
    ASSERT_EQ(0, dcg.source_segment_num_rows());
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(DeltaColumnFileKind::DENSE_COLS, dcg.file_kind(i));
        EXPECT_TRUE(dcg.is_file_dense(i));
        EXPECT_EQ(0, dcg.sparse_row_count(i));
    }
}

// SDCG: a DeltaColumnGroupVerPB that carries file_kinds / sparse_row_counts /
// source_segment_num_rows must be loaded into a strictly-1:1 view, normalizing any absent
// trailing entries to DENSE / 0.
TEST(TestDeltaColumnGroup, testSDCGLoadKindsAndCounts) {
    DeltaColumnGroupVerPB dcg_ver;
    // entry 0: sparse .spcols K=7 ; entry 1: dense .cols
    dcg_ver.add_versions(20);
    dcg_ver.add_versions(21);
    dcg_ver.add_column_files("sp0.spcols");
    dcg_ver.add_column_files("d1.cols");
    DeltaColumnGroupColumnIdsPB unique_cids;
    unique_cids.add_column_ids(3);
    dcg_ver.add_unique_column_ids()->CopyFrom(unique_cids);
    unique_cids.Clear();
    unique_cids.add_column_ids(4);
    dcg_ver.add_unique_column_ids()->CopyFrom(unique_cids);
    dcg_ver.add_file_kinds(SPARSE_PERCOL);
    dcg_ver.add_file_kinds(DENSE_COLS);
    dcg_ver.add_sparse_row_counts(7);
    dcg_ver.add_sparse_row_counts(0);
    dcg_ver.set_source_segment_num_rows(1000);

    DeltaColumnGroup dcg;
    ASSERT_TRUE(dcg.load(21, dcg_ver).ok());
    ASSERT_EQ(2u, dcg.file_kinds().size());
    EXPECT_EQ(DeltaColumnFileKind::SPARSE_PERCOL, dcg.file_kind(0));
    EXPECT_FALSE(dcg.is_file_dense(0));
    EXPECT_EQ(7, dcg.sparse_row_count(0));
    EXPECT_EQ(DeltaColumnFileKind::DENSE_COLS, dcg.file_kind(1));
    EXPECT_TRUE(dcg.is_file_dense(1));
    EXPECT_EQ(0, dcg.sparse_row_count(1));
    EXPECT_EQ(1000, dcg.source_segment_num_rows());
    // Out-of-range index still hinges to DENSE.
    EXPECT_EQ(DeltaColumnFileKind::DENSE_COLS, dcg.file_kind(2));
}

// SDCG: only source_segment_num_rows present (no kinds/counts) still materializes a 1:1 all-DENSE
// view so downstream positional access is uniform.
TEST(TestDeltaColumnGroup, testSDCGLoadOnlySourceRows) {
    DeltaColumnGroupVerPB dcg_ver;
    dcg_ver.add_versions(30);
    dcg_ver.add_column_files("d0.cols");
    DeltaColumnGroupColumnIdsPB unique_cids;
    unique_cids.add_column_ids(3);
    dcg_ver.add_unique_column_ids()->CopyFrom(unique_cids);
    dcg_ver.set_source_segment_num_rows(512);

    DeltaColumnGroup dcg;
    ASSERT_TRUE(dcg.load(30, dcg_ver).ok());
    ASSERT_EQ(1u, dcg.file_kinds().size());
    EXPECT_EQ(DeltaColumnFileKind::DENSE_COLS, dcg.file_kind(0));
    EXPECT_EQ(0, dcg.sparse_row_count(0));
    EXPECT_EQ(512, dcg.source_segment_num_rows());
}

// SDCG: a DeltaColumnGroupVerPB that carries a `presences` array (the per-file [min,max]+K range
// summary) loads back into a 1:1 SparsePresence view: a SPARSE entry with a recorded range is
// `known()`, and a padded/empty SparsePresencePB (DENSE slot) resolves to kSDCGPresenceUnknown so
// the reader never skips it.
TEST(TestDeltaColumnGroup, testSDCGLoadPresences) {
    DeltaColumnGroupVerPB dcg_ver;
    // entry 0: sparse .spcols K=7, presence [min=3, max=998] ; entry 1: dense .cols, empty presence
    dcg_ver.add_versions(40);
    dcg_ver.add_versions(41);
    dcg_ver.add_column_files("sp0.spcols");
    dcg_ver.add_column_files("d1.cols");
    DeltaColumnGroupColumnIdsPB unique_cids;
    unique_cids.add_column_ids(3);
    dcg_ver.add_unique_column_ids()->CopyFrom(unique_cids);
    unique_cids.Clear();
    unique_cids.add_column_ids(4);
    dcg_ver.add_unique_column_ids()->CopyFrom(unique_cids);
    dcg_ver.add_file_kinds(SPARSE_PERCOL);
    dcg_ver.add_file_kinds(DENSE_COLS);
    dcg_ver.add_sparse_row_counts(7);
    dcg_ver.add_sparse_row_counts(0);
    dcg_ver.set_source_segment_num_rows(1000);
    // presences strictly 1:1 with column_files: slot 0 carries the sparse range, slot 1 is empty.
    auto* p0 = dcg_ver.add_presences();
    p0->set_min_source_rowid(3);
    p0->set_max_source_rowid(998);
    p0->set_row_count(7);
    dcg_ver.add_presences(); // slot 1: empty == unknown (dense)

    DeltaColumnGroup dcg;
    ASSERT_TRUE(dcg.load(41, dcg_ver).ok());
    ASSERT_EQ(2u, dcg.presences().size());
    // Slot 0: sparse, fully known.
    EXPECT_EQ(3, dcg.presence_min(0));
    EXPECT_EQ(998, dcg.presence_max(0));
    EXPECT_EQ(7, dcg.presence_row_count(0));
    EXPECT_TRUE(dcg.presence_known(0));
    // Slot 1: dense, padded-empty => unknown, reader must not skip.
    EXPECT_EQ(kSDCGPresenceUnknown, dcg.presence_min(1));
    EXPECT_EQ(kSDCGPresenceUnknown, dcg.presence_max(1));
    EXPECT_EQ(kSDCGPresenceUnknown, dcg.presence_row_count(1));
    EXPECT_FALSE(dcg.presence_known(1));
    // Out-of-range index also resolves to unknown.
    EXPECT_EQ(kSDCGPresenceUnknown, dcg.presence_min(2));
    EXPECT_FALSE(dcg.presence_known(2));
}

// SDCG: legacy / dense-only metadata that carries NO presences array must read back as every-slot
// unknown (no skip), exactly like the file_kinds legacy hinge. A partially-recorded presence (only
// min set, max absent) must also resolve to NOT known() so the reader stays on the safe always-open
// fallback rather than skipping on a half-range.
TEST(TestDeltaColumnGroup, testSDCGLegacyPresencesAbsentIsUnknown) {
    DeltaColumnGroupVerPB dcg_ver;
    dcg_ver.add_versions(50);
    dcg_ver.add_versions(51);
    dcg_ver.add_column_files("sp0.spcols");
    dcg_ver.add_column_files("sp1.spcols");
    DeltaColumnGroupColumnIdsPB unique_cids;
    unique_cids.add_column_ids(3);
    dcg_ver.add_unique_column_ids()->CopyFrom(unique_cids);
    unique_cids.Clear();
    unique_cids.add_column_ids(4);
    dcg_ver.add_unique_column_ids()->CopyFrom(unique_cids);
    dcg_ver.add_file_kinds(SPARSE_PERCOL);
    dcg_ver.add_file_kinds(SPARSE_PERCOL);
    dcg_ver.add_sparse_row_counts(2);
    dcg_ver.add_sparse_row_counts(3);
    dcg_ver.set_source_segment_num_rows(1000);
    // presences array intentionally absent (writer predates the field) for slot 0, and a half-set
    // presence (only min) for slot 1: both must resolve to NOT known().
    auto* p1 = dcg_ver.add_presences(); // index 0 in the array maps to file slot 0
    (void)p1;                           // leave fully empty
    auto* p2 = dcg_ver.add_presences(); // file slot 1
    p2->set_min_source_rowid(5);        // max + row_count absent => half-range

    DeltaColumnGroup dcg;
    ASSERT_TRUE(dcg.load(51, dcg_ver).ok());
    ASSERT_EQ(2u, dcg.presences().size());
    // Slot 0: fully empty presence => unknown.
    EXPECT_EQ(kSDCGPresenceUnknown, dcg.presence_min(0));
    EXPECT_FALSE(dcg.presence_known(0));
    // Slot 1: only min recorded => still NOT known (must not skip on a half-range).
    EXPECT_EQ(5, dcg.presence_min(1));
    EXPECT_EQ(kSDCGPresenceUnknown, dcg.presence_max(1));
    EXPECT_FALSE(dcg.presence_known(1)) << "a presence with min but no max must not be skippable";
}

} // namespace starrocks