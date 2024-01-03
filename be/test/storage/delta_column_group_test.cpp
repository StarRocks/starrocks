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

namespace starrocks {

TEST(TestDeltaColumnGroup, testLoad) {
    DeltaColumnGroup dcg;
    dcg.init(100, {{1, 10, 100}}, {"abc0.cols"});
    std::string pb_str = dcg.save();
    DeltaColumnGroup new_dcg;
    ASSERT_TRUE(new_dcg.load(100, pb_str.data(), pb_str.length()).ok());
    ASSERT_TRUE(dcg.column_files("111") == new_dcg.column_files("111"));
    std::vector<std::vector<uint32_t>> v1 = dcg.column_ids();
    std::vector<std::vector<uint32_t>> v2 = new_dcg.column_ids();
    ASSERT_TRUE(v1.size() == v2.size());
    for (int i = 0; i < v1.size(); ++i) {
        ASSERT_TRUE(v1[i].size() == v2[i].size());
    }
    for (int i = 0; i < v1.size(); i++) {
        for (int j = 0; j < v1[i].size(); ++j) {
            ASSERT_TRUE(v1[i][j] == v2[i][j]);
        }
    }
};

TEST(TestDeltaColumnGroup, testGet) {
    DeltaColumnGroup dcg;
    dcg.init(100, {{10, 1, 100}}, {"abc.cols"});
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
    // test1
    {
        // 1 -> {1, 2, 3}
        // 2 -> {2, 3, 4}
        // 3 -> {3, 4, 5}
        // ...
        std::vector<std::pair<TabletSegmentId, int64_t>> garbage_dcgs;
        DeltaColumnGroupList dcgs;
        for (uint32_t i = 1; i <= 20; i++) {
            DeltaColumnGroup dcg;
            dcg.init((int64_t)i, {{i, i + 1, i + 2}}, {"abc.cols"});
            dcgs.push_back(std::make_shared<DeltaColumnGroup>(dcg));
        }
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
            dcg.init((int64_t)i, {{1, 2, 3}}, {"abc.cols"});
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
        for (uint32_t i = 1; i <= 20; i++) {
            DeltaColumnGroup dcg;
            uint32_t shift = i % 2;
            dcg.init((int64_t)i, {{1 + shift, 2 + shift, 3 + shift}}, {"abc.cols"});
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
        for (uint32_t i = 1; i <= 20; i++) {
            DeltaColumnGroup dcg;
            uint32_t shift = i % 3;
            dcg.init((int64_t)i, {{1 + shift, 2 + shift, 3 + shift}}, {"abc.cols"});
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

} // namespace starrocks