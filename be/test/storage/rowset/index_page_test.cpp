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

#include "storage/rowset/index_page.h"

#include <gtest/gtest.h>

namespace starrocks {

class IndexPageTest : public testing::Test {};

TEST_F(IndexPageTest, test_seek_at_or_before) {
    // one item
    IndexPageReader reader1;
    reader1._parsed = true;
    reader1._keys = {"111"};
    reader1._num_entries = 1;
    std::vector<Slice> keys1 = {"000", "111", "222"};
    std::vector<int> pos1 = {-1, 0, 0};
    IndexPageIterator iter1(&reader1);

    for (size_t i = 0; i < keys1.size(); i++) {
        if (pos1[i] == -1) {
            ASSERT_TRUE(!iter1.seek_at_or_before(keys1[i]).ok());
        } else {
            ASSERT_TRUE(iter1.seek_at_or_before(keys1[i]).ok());
            ASSERT_EQ(iter1._pos, pos1[i]);
        }
    }

    // two item
    IndexPageReader reader2;
    reader2._parsed = true;
    reader2._keys = {"111", "333"};
    reader2._num_entries = 2;
    std::vector<Slice> keys2 = {"000", "111", "222", "333", "444"};
    std::vector<int> pos2 = {-1, 0, 0, 1, 1};
    IndexPageIterator iter2(&reader2);

    for (size_t i = 0; i < keys2.size(); i++) {
        if (pos2[i] == -1) {
            ASSERT_TRUE(!iter2.seek_at_or_before(keys2[i]).ok());
        } else {
            ASSERT_TRUE(iter2.seek_at_or_before(keys2[i]).ok());
            ASSERT_EQ(iter2._pos, pos2[i]);
        }
    }

    // three item
    IndexPageReader reader3;
    reader3._parsed = true;
    reader3._keys = {"111", "333", "555"};
    reader3._num_entries = 3;
    std::vector<Slice> keys3 = {"000", "111", "222", "333", "444", "555", "666"};
    std::vector<int> pos3 = {-1, 0, 0, 1, 1, 2, 2};
    IndexPageIterator iter3(&reader3);

    for (size_t i = 0; i < keys3.size(); i++) {
        if (pos3[i] == -1) {
            ASSERT_TRUE(!iter3.seek_at_or_before(keys3[i]).ok());
        } else {
            ASSERT_TRUE(iter3.seek_at_or_before(keys3[i]).ok());
            ASSERT_EQ(iter3._pos, pos3[i]);
        }
    }
}

TEST_F(IndexPageTest, test_seek_at_or_after) {
    // one item
    IndexPageReader reader1;
    reader1._parsed = true;
    reader1._keys = {"111"};
    reader1._num_entries = 1;
    std::vector<Slice> keys1 = {"000", "111", "222"};
    std::vector<int> pos1 = {0, 0, 0};
    IndexPageIterator iter1(&reader1);

    for (size_t i = 0; i < keys1.size(); i++) {
        ASSERT_TRUE(iter1.seek_at_or_after(keys1[i]).ok());
        ASSERT_EQ(iter1._pos, pos1[i]);
    }

    // two item
    IndexPageReader reader2;
    reader2._parsed = true;
    reader2._keys = {"111", "333"};
    reader2._num_entries = 2;
    std::vector<Slice> keys2 = {"000", "111", "222", "333", "444"};
    std::vector<int> pos2 = {0, 0, 0, 1, 1};
    IndexPageIterator iter2(&reader2);

    for (size_t i = 0; i < keys2.size(); i++) {
        ASSERT_TRUE(iter2.seek_at_or_after(keys2[i]).ok());
        ASSERT_EQ(iter2._pos, pos2[i]);
    }

    // three item
    IndexPageReader reader3;
    reader3._parsed = true;
    reader3._keys = {"111", "333", "555"};
    reader3._num_entries = 3;
    std::vector<Slice> keys3 = {"000", "111", "222", "333", "444", "555", "666"};
    std::vector<int> pos3 = {0, 0, 0, 1, 1, 2, 2};
    IndexPageIterator iter3(&reader3);

    for (size_t i = 0; i < keys3.size(); i++) {
        ASSERT_TRUE(iter3.seek_at_or_after(keys3[i]).ok());
        ASSERT_EQ(iter3._pos, pos3[i]);
    }
}

} // namespace starrocks
