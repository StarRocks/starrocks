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

} // namespace starrocks