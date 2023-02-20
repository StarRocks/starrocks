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
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/rowset/segment_v2/binary_prefix_page_test.cpp

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

#include "storage/rowset/binary_prefix_page.h"

#include <gtest/gtest.h>

#include <iostream>

#include "column/binary_column.h"
#include "common/logging.h"
#include "runtime/mem_pool.h"
#include "storage/olap_common.h"
#include "storage/rowset/page_decoder.h"
#include "storage/types.h"

namespace starrocks {

class BinaryPrefixPageTest : public testing::Test {
public:
    void test_encode_and_decode2() {
        std::vector<std::string> test_data;
        test_data.emplace_back("ab");
        test_data.emplace_back("c");
        std::vector<Slice> slices;
        for (auto& i : test_data) {
            Slice s(i);
            slices.emplace_back(s);
        }
        // encode
        PageBuilderOptions options;
        BinaryPrefixPageBuilder page_builder(options);

        size_t count = slices.size();
        const Slice* ptr = &slices[0];
        count = page_builder.add(reinterpret_cast<const uint8_t*>(ptr), count);

        OwnedSlice dict_slice = page_builder.finish()->build();

        PageDecoderOptions dict_decoder_options;
        auto page_decoder =
                std::make_unique<BinaryPrefixPageDecoder<TYPE_VARCHAR>>(dict_slice.slice(), dict_decoder_options);
        Status ret = page_decoder->init();
        ASSERT_TRUE(ret.ok());

        Slice slice("c");
        bool exact_match;
        ret = page_decoder->seek_at_or_after_value(&slice, &exact_match);
        ASSERT_TRUE(ret.ok());
        ASSERT_TRUE(exact_match);
    }

    void test_vectorized() {
        std::vector<std::string> test_data;
        for (int i = 1000; i < 1038; ++i) {
            test_data.push_back(std::to_string(i));
        }
        std::vector<Slice> slices;
        for (auto& i : test_data) {
            Slice s(i);
            slices.emplace_back(s);
        }
        // encode
        PageBuilderOptions options;
        BinaryPrefixPageBuilder page_builder(options);

        size_t count = slices.size();
        const Slice* ptr = &slices[0];
        count = page_builder.add(reinterpret_cast<const uint8_t*>(ptr), count);

        OwnedSlice dict_slice = page_builder.finish()->build();
        ASSERT_EQ(slices.size(), page_builder.count());
        ASSERT_FALSE(page_builder.is_page_full());

        //check first value and last value
        Slice first_value;
        page_builder.get_first_value(&first_value);
        ASSERT_EQ(slices[0], first_value);
        Slice last_value;
        page_builder.get_last_value(&last_value);
        ASSERT_EQ(slices[count - 1], last_value);

        PageDecoderOptions dict_decoder_options;
        auto page_decoder =
                std::make_unique<BinaryPrefixPageDecoder<TYPE_VARCHAR>>(dict_slice.slice(), dict_decoder_options);
        Status ret = page_decoder->init();
        ASSERT_TRUE(ret.ok());
        // because every slice is unique
        ASSERT_EQ(slices.size(), page_decoder->count());

        //check values
        auto column = BinaryColumn::create();
        size_t size = slices.size();
        ret = page_decoder->next_batch(&size, column.get());
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(slices.size(), size);
        for (int i = 1000; i < 1038; ++i) {
            ASSERT_EQ(std::to_string(i), column->get_slice(i - 1000));
        }

        ret = page_decoder->seek_to_position_in_page(15);
        ASSERT_TRUE(ret.ok());
        auto column2 = BinaryColumn::create();
        ret = page_decoder->next_batch(&size, column2.get());
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(23, size);
        for (int i = 1015; i < 1038; ++i) {
            ASSERT_EQ(std::to_string(i), column2->get_slice(i - 1015));
        }

        ret = page_decoder->seek_to_position_in_page(0);
        ASSERT_TRUE(ret.ok());
        auto column3 = BinaryColumn::create();
        SparseRange read_range;
        read_range.add(Range(0, 10));
        read_range.add(Range(15, 25));
        read_range.add(Range(28, 38));
        ret = page_decoder->next_batch(read_range, column3.get());
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(30, column3->size());
        for (int i = 1000; i < 1010; ++i) {
            ASSERT_EQ(std::to_string(i), column3->get_slice(i - 1000));
        }
        for (int i = 1015; i < 1025; ++i) {
            ASSERT_EQ(std::to_string(i), column3->get_slice(i - 1015 + 10));
        }
        for (int i = 1028; i < 1038; ++i) {
            ASSERT_EQ(std::to_string(i), column3->get_slice(i - 1028 + 20));
        }

        std::string v1_string = std::to_string(1039);
        auto v1 = Slice(v1_string);
        bool exact_match;
        ret = page_decoder->seek_at_or_after_value(&v1, &exact_match);
        ASSERT_TRUE(ret.is_not_found());

        std::string v2_string = std::to_string(1000);
        auto v2 = Slice(v2_string);
        ret = page_decoder->seek_at_or_after_value(&v2, &exact_match);
        ASSERT_TRUE(ret.ok());
        ASSERT_TRUE(exact_match);

        std::string v3_string = std::to_string(1037);
        auto v3 = Slice(v3_string);
        ret = page_decoder->seek_at_or_after_value(&v3, &exact_match);
        ASSERT_TRUE(ret.ok());
        ASSERT_TRUE(exact_match);

        std::string v4_string = std::to_string(100);
        auto v4 = Slice(v4_string);
        ret = page_decoder->seek_at_or_after_value(&v4, &exact_match);
        ASSERT_TRUE(ret.ok());
        ASSERT_TRUE(!exact_match);
    }
};

// NOLINTNEXTLINE
TEST_F(BinaryPrefixPageTest, TestEncodeAndDecode2) {
    test_encode_and_decode2();
}

// NOLINTNEXTLINE
TEST_F(BinaryPrefixPageTest, TestVectorized) {
    test_vectorized();
}

} // namespace starrocks
