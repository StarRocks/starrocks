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

#include "storage/rowset/column_iterator_decorator.h"

#include <gtest/gtest.h>

#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "storage/rowset/series_column_iterator.h"
#include "testutil/assert.h"

namespace starrocks {

TEST(ColumnIteratorDecoratorTest, test) {
    auto iter = SeriesColumnIterator<int32_t>{0, 100};
    auto wrapper = ColumnIteratorDecorator{&iter, kDontTakeOwnership};
    EXPECT_EQ(101, wrapper.num_rows());
    EXPECT_EQ(iter.all_page_dict_encoded(), wrapper.all_page_dict_encoded());
    EXPECT_EQ(iter.dict_size(), wrapper.dict_size());
    EXPECT_EQ(iter.dict_lookup(Slice{}), wrapper.dict_lookup(Slice{}));
    EXPECT_EQ(iter.element_ordinal(), wrapper.element_ordinal());
    EXPECT_EQ(iter.seek_to_ordinal_and_calc_element_ordinal(1).code(),
              wrapper.seek_to_ordinal_and_calc_element_ordinal(1).code());
    auto words = std::vector<Slice>{};
    EXPECT_EQ(iter.fetch_all_dict_words(&words).code(), wrapper.fetch_all_dict_words(&words).code());
    auto codes = Int32Column{};
    auto n = size_t{1};
    EXPECT_EQ(iter.next_dict_codes(&n, &codes).code(), wrapper.next_dict_codes(&n, &codes).code());
    auto code = int32_t{0};
    auto words_column = BinaryColumn{};
    EXPECT_EQ(iter.decode_dict_codes(&code, 1, &words_column).code(),
              wrapper.decode_dict_codes(&code, 1, &words_column).code());
    auto codes_column = Int32Column{};
    codes_column.append_numbers(&code, sizeof(code));
    EXPECT_EQ(iter.decode_dict_codes(codes_column, &words_column).code(),
              wrapper.decode_dict_codes(codes_column, &words_column).code());

    auto column = Int32Column{};
    ASSERT_OK(wrapper.init(ColumnIteratorOptions{}));
    ASSERT_OK(wrapper.seek_to_ordinal(2));
    ASSERT_EQ(2, wrapper.get_current_ordinal());
    ASSERT_OK(wrapper.next_batch(&n, &column));
    ASSERT_EQ(1, n);
    ASSERT_EQ(1, column.size());
    ASSERT_EQ(2, column.get(0).get_int32());
    ASSERT_EQ(3, wrapper.get_current_ordinal());

    auto rowid = rowid_t{10};
    column.reset_column();
    ASSERT_OK(wrapper.seek_to_first());
    ASSERT_EQ(0, wrapper.get_current_ordinal());
    ASSERT_OK(wrapper.fetch_values_by_rowid(&rowid, 1, &column));
    ASSERT_EQ(1, column.size());
    ASSERT_EQ(10, column.get(0).get_int32());
    ASSERT_EQ(11, wrapper.get_current_ordinal());

    auto range = SparseRange<>{};
    range.add({1, 3});
    column.reset_column();
    ASSERT_OK(wrapper.next_batch(range, &column));
    ASSERT_EQ(2, column.size());
    ASSERT_EQ(1, column.get(0).get_int32());
    ASSERT_EQ(2, column.get(1).get_int32());
}

} // namespace starrocks