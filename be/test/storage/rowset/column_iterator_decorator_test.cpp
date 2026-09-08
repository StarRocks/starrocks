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

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "storage/rowset/cast_column_iterator.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/series_column_iterator.h"
#include "storage/types.h"

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

// SegmentIterator::_sample_by_page() asks the column iterator for its raw ColumnReader and rejects the
// query when the answer is null, so an iterator that reads no column off disk must answer nullptr rather
// than abort the BE. ColumnIteratorDecorator deliberately does not forward get_column_reader(), so every
// decorator -- CastColumnIterator among them -- lands on the base implementation, and so does
// DefaultValueColumnIterator, which stands in for a column an ADD COLUMN never wrote into an older
// segment. Both are reachable from SAMPLE(method=by_page) over such a column.
TEST(ColumnIteratorDecoratorTest, test_get_column_reader_is_null_when_there_is_no_reader) {
    auto series_iter = SeriesColumnIterator<int32_t>{0, 100};
    EXPECT_EQ(nullptr, series_iter.get_column_reader());

    auto wrapper = ColumnIteratorDecorator{&series_iter, kDontTakeOwnership};
    EXPECT_EQ(nullptr, wrapper.get_column_reader());

    auto source_type = TypeDescriptor::from_logical_type(TYPE_INT);
    auto target_type = TypeDescriptor::from_logical_type(TYPE_BIGINT);
    auto cast_iter = CastColumnIterator{std::make_unique<SeriesColumnIterator<int32_t>>(0, 100), source_type,
                                        target_type, false};
    EXPECT_EQ(nullptr, cast_iter.get_column_reader());

    auto default_value_iter = DefaultValueColumnIterator{true, "7", true, get_type_info(TYPE_INT), 0, 100};
    EXPECT_EQ(nullptr, default_value_iter.get_column_reader());
}

} // namespace starrocks