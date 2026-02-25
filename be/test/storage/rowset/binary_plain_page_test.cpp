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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/rowset/segment_v2/binary_plain_page_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/rowset/binary_plain_page.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <vector>

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate.h"
#include "storage/olap_common.h"
#include "storage/range.h"
#include "storage/rowset/page_decoder.h"
#include "storage/types.h"

namespace starrocks {

class BinaryPlainPageTest : public testing::Test {
public:
    BinaryPlainPageTest() = default;

    ~BinaryPlainPageTest() override = default;

    template <class PageBuilderType, class PageDecoderType, bool vectorize = false>
    void TestBinarySeekByValueSmallPage() {
        std::vector<Slice> slices;
        slices.emplace_back("Hello");
        slices.emplace_back(",");
        slices.emplace_back("StarRocks");

        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        PageBuilderType page_builder(options);
        size_t count = slices.size();

        Slice* ptr = &slices[0];
        count = page_builder.add(reinterpret_cast<const uint8_t*>(ptr), count);

        OwnedSlice owned_slice = page_builder.finish()->build();

        //check first value and last value
        Slice first_value;
        page_builder.get_first_value(&first_value);
        ASSERT_EQ(slices[0], first_value);
        Slice last_value;
        page_builder.get_last_value(&last_value);
        ASSERT_EQ(slices[count - 1], last_value);

        PageDecoderType page_decoder(owned_slice.slice());
        Status status = page_decoder.init();
        ASSERT_TRUE(status.ok());

        //test1

        auto column = BinaryColumn::create();
        column->reserve(1024);
        size_t size = 1024;
        status = page_decoder.next_batch(&size, column.get());
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(3U, size);
        ASSERT_EQ("Hello", column->get_data()[0]);
        ASSERT_EQ(",", column->get_data()[1]);
        ASSERT_EQ("StarRocks", column->get_data()[2]);

        size = 1024;
        auto column1 = BinaryColumn::create();
        ASSERT_OK(page_decoder.seek_to_position_in_page(2));
        status = page_decoder.next_batch(&size, column1.get());
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(1, size);
        ASSERT_EQ("StarRocks", column1->get_data()[0]);

        auto column2 = BinaryColumn::create();
        ASSERT_OK(page_decoder.seek_to_position_in_page(0));
        SparseRange<> read_range;
        read_range.add(Range<>(0, 1));
        read_range.add(Range<>(2, 3));
        status = page_decoder.next_batch(read_range, column2.get());
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(2, column2->size());
        ASSERT_EQ("Hello", column2->get_data()[0]);
        ASSERT_EQ("StarRocks", column2->get_data()[1]);
    }
};

// NOLINTNEXTLINE
TEST_F(BinaryPlainPageTest, test_seek_by_value) {
    TestBinarySeekByValueSmallPage<BinaryPlainPageBuilder, BinaryPlainPageDecoder<TYPE_VARCHAR>, false>();
    TestBinarySeekByValueSmallPage<BinaryPlainPageBuilder, BinaryPlainPageDecoder<TYPE_VARCHAR>, true>();
}

// NOLINTNEXTLINE
TEST_F(BinaryPlainPageTest, test_reserve_head) {
    PageBuilderOptions options;
    options.data_page_size = 256 * 1024;
    BinaryPlainPageBuilder builder(options);
    builder.reserve_head(4);
    Slice slices[] = {
            "first value", "second value", "third value", "fourth value", "fifth value",
    };

    ASSERT_EQ(5, builder.add((const uint8_t*)slices, 5));
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(slices[i], builder.get_value(i));
    }

    OwnedSlice data_with_head = builder.finish()->build();

    Slice slice;
    EXPECT_EQ(5, builder.count());

    ASSERT_TRUE(builder.get_first_value(&slice).ok());
    EXPECT_EQ(slices[0], slice);

    ASSERT_TRUE(builder.get_last_value(&slice).ok());
    EXPECT_EQ(slices[4], slice);

    Slice data_without_head = data_with_head.slice();
    data_without_head.remove_prefix(4);

    BinaryPlainPageDecoder<TYPE_VARCHAR> decoder(data_without_head);
    ASSERT_TRUE(decoder.init().ok());
    ASSERT_EQ(5, decoder.count());
    for (uint32_t i = 0; i < 5; i++) {
        EXPECT_EQ(slices[i], decoder.string_at_index(i));
    }

    auto column = BinaryColumn::create();
    SparseRange<> range(0, 5);
    ASSERT_OK(decoder.next_batch(range, column.get()));
    ASSERT_EQ(column->debug_string(), "['first value', 'second value', 'third value', 'fourth value', 'fifth value']");

    ASSERT_OK(decoder.seek_to_position_in_page(0));
    ASSERT_OK(decoder.next_batch(SparseRange<>(0, 1), column.get()));
    ASSERT_EQ(column->debug_string(),
              "['first value', 'second value', 'third value', 'fourth value', 'fifth value', 'first value']");
}

TEST_F(BinaryPlainPageTest, TestNextBatchWithFilter) {
    PageBuilderOptions options;
    options.data_page_size = 256 * 1024;
    BinaryPlainPageBuilder builder(options);
    Slice slices[] = {
            "a_100", "b_200", "c_300", "d_400", "e_500",
    };

    ASSERT_EQ(5, builder.add((const uint8_t*)slices, 5));
    OwnedSlice data_with_head = builder.finish()->build();
    BinaryPlainPageDecoder<TYPE_VARCHAR> decoder(data_with_head.slice());
    ASSERT_TRUE(decoder.init().ok());

    // Case 1: Without NULLs
    {
        auto column = BinaryColumn::create();

        // Prepare filter: >= "c_300"
        std::unique_ptr<ColumnPredicate> predicate(new_column_ge_predicate(get_type_info(TYPE_VARCHAR), 0, "c_300"));
        std::vector<const ColumnPredicate*> predicates;
        predicates.push_back(predicate.get());

        SparseRange<> range(0, 5);
        std::vector<uint8_t> selection(5);
        std::vector<uint16_t> selected_idx(5);

        // reset selection
        for (int i = 0; i < 5; ++i) selection[i] = 1;

        Status st = decoder.next_batch_with_filter(column.get(), range, predicates, nullptr, selection.data(),
                                                   selected_idx.data());
        ASSERT_TRUE(st.ok()) << st.to_string();

        // Check selection
        ASSERT_EQ(0, selection[0]);
        ASSERT_EQ(0, selection[1]);
        ASSERT_EQ(1, selection[2]);
        ASSERT_EQ(1, selection[3]);
        ASSERT_EQ(1, selection[4]);

        ASSERT_EQ(3, column->size());
        ASSERT_EQ("c_300", column->get_data()[0]);
        ASSERT_EQ("d_400", column->get_data()[1]);
        ASSERT_EQ("e_500", column->get_data()[2]);
    }

    // Reset decoder
    ASSERT_TRUE(decoder.seek_to_position_in_page(0).ok());

    // Case 2: Nullable destination column but the page has no NULL flags (null_data is nullptr).
    {
        auto column = ChunkHelper::column_from_field_type(TYPE_VARCHAR, true);

        std::unique_ptr<ColumnPredicate> predicate(new_column_ge_predicate(get_type_info(TYPE_VARCHAR), 0, "c_300"));
        std::vector<const ColumnPredicate*> predicates;
        predicates.push_back(predicate.get());

        SparseRange<> range(0, 5);
        std::vector<uint8_t> selection(5);
        std::vector<uint16_t> selected_idx(5);

        for (int i = 0; i < 5; ++i) selection[i] = 1;

        Status st = decoder.next_batch_with_filter(column.get(), range, predicates, nullptr, selection.data(),
                                                   selected_idx.data());
        ASSERT_TRUE(st.ok()) << st.to_string();

        ASSERT_EQ(0, selection[0]);
        ASSERT_EQ(0, selection[1]);
        ASSERT_EQ(1, selection[2]);
        ASSERT_EQ(1, selection[3]);
        ASSERT_EQ(1, selection[4]);

        ASSERT_EQ(3, column->size());
        auto nullable_col = down_cast<NullableColumn*>(column.get());
        auto binary_col = down_cast<BinaryColumn*>(nullable_col->data_column_raw_ptr());

        ASSERT_EQ("c_300", binary_col->get_data()[0]);
        ASSERT_EQ("d_400", binary_col->get_data()[1]);
        ASSERT_EQ("e_500", binary_col->get_data()[2]);

        ASSERT_EQ(3, nullable_col->null_column_raw_ptr()->size());
        ASSERT_EQ(0, nullable_col->null_column_data()[0]);
        ASSERT_EQ(0, nullable_col->null_column_data()[1]);
        ASSERT_EQ(0, nullable_col->null_column_data()[2]);
        ASSERT_FALSE(nullable_col->has_null());
    }

    // Reset decoder
    ASSERT_TRUE(decoder.seek_to_position_in_page(0).ok());

    // Case 3: With NULLs
    {
        // Use NullableColumn with ByteColumn as data
        auto column = ChunkHelper::column_from_field_type(TYPE_VARCHAR, true);

        // Prepare filter: >= "c_300"
        std::unique_ptr<ColumnPredicate> predicate(new_column_ge_predicate(get_type_info(TYPE_VARCHAR), 0, "c_300"));
        std::vector<const ColumnPredicate*> predicates;
        predicates.push_back(predicate.get());

        SparseRange<> range(0, 5);
        std::vector<uint8_t> selection(5);
        std::vector<uint16_t> selected_idx(5);

        // reset selection
        for (int i = 0; i < 5; ++i) selection[i] = 1;

        // null_data: 0->not null, 1->null
        // Mark "c_300" (idx 2) as NULL. "e_500" (idx 4) as NULL.
        // "b_200" (idx 1) is not null but filtered out (< "c_300")
        // "a_100" (idx 0) is not null but filtered out
        // "d_400" (idx 3) is not null and kept (>= "c_300")
        uint8_t null_data[] = {0, 0, 1, 0, 1};

        Status st = decoder.next_batch_with_filter(column.get(), range, predicates, null_data, selection.data(),
                                                   selected_idx.data());
        ASSERT_TRUE(st.ok()) << st.to_string();

        // Check selection
        ASSERT_EQ(0, selection[0]);
        ASSERT_EQ(0, selection[1]);
        ASSERT_EQ(0, selection[2]);
        ASSERT_EQ(1, selection[3]);
        ASSERT_EQ(0, selection[4]);

        ASSERT_EQ(1, column->size());
        // For NullableColumn, we need to check the data values.
        auto nullable_col = down_cast<NullableColumn*>(column.get());
        auto binary_col = down_cast<BinaryColumn*>(nullable_col->data_column_raw_ptr());

        ASSERT_EQ("d_400", binary_col->get_data()[0]);
    }
}

TEST_F(BinaryPlainPageTest, TestReadByRowids) {
    PageBuilderOptions options;
    options.data_page_size = 256 * 1024;
    BinaryPlainPageBuilder builder(options);

    std::vector<std::string> strings;
    strings.reserve(10);
    std::vector<Slice> slices;
    slices.reserve(10);
    for (int i = 0; i < 10; ++i) {
        strings.emplace_back("val_" + std::to_string(i));
        slices.emplace_back(strings.back());
    }

    ASSERT_EQ(10, builder.add((const uint8_t*)slices.data(), 10));
    OwnedSlice data_with_head = builder.finish()->build();
    BinaryPlainPageDecoder<TYPE_VARCHAR> decoder(data_with_head.slice());
    ASSERT_TRUE(decoder.init().ok());

    auto column = BinaryColumn::create();

    rowid_t rowids[] = {1, 3, 5, 8};
    size_t num_read = 4;

    Status st = decoder.read_by_rowids(0, rowids, &num_read, column.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(4, num_read);
    ASSERT_EQ(4, column->size());

    ASSERT_EQ("val_1", column->get_data()[0]);
    ASSERT_EQ("val_3", column->get_data()[1]);
    ASSERT_EQ("val_5", column->get_data()[2]);
    ASSERT_EQ("val_8", column->get_data()[3]);
}

TEST_F(BinaryPlainPageTest, TestDictFilterSelectionLargeDictSize) {
    // Ensure dictionary-page predicate evaluation remains correct when dict_size > 65535.
    // This is required by predicate-late-materialization which evaluates predicates on DICTIONARY_PAGE.
    constexpr uint32_t kNumDictValues = 70000;

    PageBuilderOptions options;
    options.data_page_size = 2 * 1024 * 1024; // big enough for 70k small strings + offsets
    BinaryPlainPageBuilder builder(options);

    char buf[16];
    for (uint32_t i = 0; i < kNumDictValues; ++i) {
        // Fixed-width numeric part to keep lexicographic order aligned with numeric order.
        snprintf(buf, sizeof(buf), "k%06u", i);
        ASSERT_TRUE(builder.add_slice(Slice(buf)));
    }

    OwnedSlice dict_page = builder.finish()->build();
    BinaryPlainPageDecoder<TYPE_VARCHAR> decoder(dict_page.slice());
    ASSERT_TRUE(decoder.init().ok());
    ASSERT_EQ(kNumDictValues, decoder.count());

    std::unique_ptr<ColumnPredicate> predicate(new_column_ge_predicate(get_type_info(TYPE_VARCHAR), 0, "k065536"));
    std::vector<const ColumnPredicate*> predicates;
    predicates.push_back(predicate.get());

    const uint8_t* selection = nullptr;
    uint32_t dict_size = 0;
    uint32_t selected_count = 0;
    ASSERT_OK(decoder.get_dict_filter_selection(predicates, &selection, &dict_size, &selected_count));

    ASSERT_EQ(kNumDictValues, dict_size);
    ASSERT_NE(nullptr, selection);
    ASSERT_EQ(kNumDictValues - 65536, selected_count);
    ASSERT_EQ(0, selection[65535]);
    ASSERT_EQ(1, selection[65536]);
    ASSERT_EQ(1, selection[kNumDictValues - 1]);
}

} // namespace starrocks
