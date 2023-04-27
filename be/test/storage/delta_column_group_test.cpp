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
    dcg.init(100, {1, 10, 100}, "abc.cols");
    std::string pb_str = dcg.save();
    DeltaColumnGroup new_dcg;
    ASSERT_TRUE(new_dcg.load(100, pb_str.data(), pb_str.length()).ok());
    ASSERT_TRUE(dcg.column_file("111") == new_dcg.column_file("111"));
    std::vector<uint32_t> v1 = dcg.column_ids();
    std::vector<uint32_t> v2 = new_dcg.column_ids();
    ASSERT_TRUE(v1.size() == v2.size());
    for (int i = 0; i < v1.size(); i++) {
        ASSERT_TRUE(v1[i] == v2[i]);
    }
};

TEST(TestDeltaColumnGroup, testGet) {
    DeltaColumnGroup dcg;
    dcg.init(100, {10, 1, 100}, "abc.cols");
    ASSERT_TRUE(0 == dcg.get_column_idx(10));
    ASSERT_TRUE(1 == dcg.get_column_idx(1));
    ASSERT_TRUE(2 == dcg.get_column_idx(100));
    ASSERT_TRUE(-1 == dcg.get_column_idx(2));
    ASSERT_TRUE(-1 == dcg.get_column_idx(1000));
};

} // namespace starrocks