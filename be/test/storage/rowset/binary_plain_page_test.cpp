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

#include <vector>

#include "column/binary_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "runtime/mem_pool.h"
#include "storage/olap_common.h"
#include "storage/range.h"
#include "storage/rowset/page_decoder.h"
#include "storage/types.h"
#include "testutil/assert.h"

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

} // namespace starrocks
