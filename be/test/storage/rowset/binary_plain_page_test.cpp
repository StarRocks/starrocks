// This file is made available under Elastic License 2.0.
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
#include "runtime/mem_pool.h"
#include "storage/olap_common.h"
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

        PageDecoderOptions decoder_options;
        PageDecoderType page_decoder(owned_slice.slice(), decoder_options);
        Status status = page_decoder.init();
        ASSERT_TRUE(status.ok());

        //test1

        if (!vectorize) {
            MemPool pool;
            size_t size = 3;
            std::unique_ptr<ColumnVectorBatch> cvb;
            ColumnVectorBatch::create(size, true, get_type_info(OLAP_FIELD_TYPE_VARCHAR), nullptr, &cvb);
            ColumnBlock block(cvb.get(), &pool);
            ColumnBlockView column_block_view(&block);

            status = page_decoder.next_batch(&size, &column_block_view);
            Slice* values = reinterpret_cast<Slice*>(block.data());
            ASSERT_TRUE(status.ok());

            Slice* value = reinterpret_cast<Slice*>(values);
            ASSERT_EQ(3U, size);
            ASSERT_EQ("Hello", value[0].to_string());
            ASSERT_EQ(",", value[1].to_string());
            ASSERT_EQ("StarRocks", value[2].to_string());

            std::unique_ptr<ColumnVectorBatch> cvb2;
            ColumnVectorBatch::create(1, true, get_type_info(OLAP_FIELD_TYPE_VARCHAR), nullptr, &cvb2);
            ColumnBlock block2(cvb2.get(), &pool);
            ColumnBlockView column_block_view2(&block2);

            size_t fetch_num = 1;
            page_decoder.seek_to_position_in_page(2);
            status = page_decoder.next_batch(&fetch_num, &column_block_view2);
            Slice* values2 = reinterpret_cast<Slice*>(block2.data());
            ASSERT_TRUE(status.ok());
            Slice* value2 = reinterpret_cast<Slice*>(values2);
            ASSERT_EQ(1, fetch_num);
            ASSERT_EQ("StarRocks", value2[0].to_string());
        } else {
            auto column = vectorized::BinaryColumn::create();
            column->reserve(1024);
            size_t size = 1024;
            status = page_decoder.next_batch(&size, column.get());
            ASSERT_TRUE(status.ok());
            ASSERT_EQ(3U, size);
            ASSERT_EQ("Hello", column->get_data()[0]);
            ASSERT_EQ(",", column->get_data()[1]);
            ASSERT_EQ("StarRocks", column->get_data()[2]);

            size = 1024;
            auto column1 = vectorized::BinaryColumn::create();
            page_decoder.seek_to_position_in_page(2);
            status = page_decoder.next_batch(&size, column1.get());
            ASSERT_TRUE(status.ok());
            ASSERT_EQ(1, size);
            ASSERT_EQ("StarRocks", column1->get_data()[0]);

            auto column2 = vectorized::BinaryColumn::create();
            page_decoder.seek_to_position_in_page(0);
            vectorized::SparseRange read_range;
            read_range.add(vectorized::Range(0, 1));
            read_range.add(vectorized::Range(2, 3));
            status = page_decoder.next_batch(read_range, column2.get());
            ASSERT_TRUE(status.ok());
            ASSERT_EQ(2, column2->size());
            ASSERT_EQ("Hello", column2->get_data()[0]);
            ASSERT_EQ("StarRocks", column2->get_data()[1]);
        }
    }
};

// NOLINTNEXTLINE
TEST_F(BinaryPlainPageTest, test_seek_by_value) {
    TestBinarySeekByValueSmallPage<BinaryPlainPageBuilder, BinaryPlainPageDecoder<OLAP_FIELD_TYPE_VARCHAR>, false>();
    TestBinarySeekByValueSmallPage<BinaryPlainPageBuilder, BinaryPlainPageDecoder<OLAP_FIELD_TYPE_VARCHAR>, true>();
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

    BinaryPlainPageDecoder<OLAP_FIELD_TYPE_VARCHAR> decoder(data_without_head);
    ASSERT_TRUE(decoder.init().ok());
    ASSERT_EQ(5, decoder.count());
    for (uint32_t i = 0; i < 5; i++) {
        EXPECT_EQ(slices[i], decoder.string_at_index(i));
    }
}

} // namespace starrocks
