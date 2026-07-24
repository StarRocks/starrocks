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
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/short_key_index_test.cpp

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

#include "storage/base/short_key_index.h"

#include <gtest/gtest.h>

namespace starrocks {

class ShortKeyIndexTest : public testing::Test {
public:
    ShortKeyIndexTest() = default;
    ~ShortKeyIndexTest() override = default;
};

TEST_F(ShortKeyIndexTest, buider) {
    ShortKeyIndexBuilder builder(0, 1024);

    int num_items = 0;
    for (int i = 1000; i < 10000; i += 2) {
        builder.add_item(std::to_string(i));
        num_items++;
    }
    std::vector<Slice> slices;
    PageFooterPB footer;
    auto st = builder.finalize(9000 * 1024, &slices, &footer);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(SHORT_KEY_PAGE, footer.type());
    ASSERT_EQ(num_items, footer.short_key_page_footer().num_items());

    std::string buf;
    for (auto& slice : slices) {
        buf.append(slice.data, slice.size);
    }

    ShortKeyIndexDecoder decoder;
    st = decoder.parse(buf, footer.short_key_page_footer());
    ASSERT_TRUE(st.ok());

    // find 1499
    {
        auto iter = decoder.lower_bound("1499");
        ASSERT_TRUE(iter.valid());
        ASSERT_STREQ("1500", (*iter).to_string().c_str());
    }
    // find 1500 lower bound
    {
        auto iter = decoder.lower_bound("1500");
        ASSERT_TRUE(iter.valid());
        ASSERT_STREQ("1500", (*iter).to_string().c_str());
    }
    // find 1500 upper bound
    {
        auto iter = decoder.upper_bound("1500");
        ASSERT_TRUE(iter.valid());
        ASSERT_STREQ("1502", (*iter).to_string().c_str());
    }
    // find prefix "87"
    {
        auto iter = decoder.lower_bound("87");
        ASSERT_TRUE(iter.valid());
        ASSERT_STREQ("8700", (*iter).to_string().c_str());
    }
    // find prefix "87"
    {
        auto iter = decoder.upper_bound("87");
        ASSERT_TRUE(iter.valid());
        ASSERT_STREQ("8700", (*iter).to_string().c_str());
    }

    // find prefix "9999"
    {
        auto iter = decoder.upper_bound("9999");
        ASSERT_FALSE(iter.valid());
    }
}

TEST_F(ShortKeyIndexTest, finalize_with_encoding_metadata) {
    ShortKeyIndexBuilder builder(0, 1024);

    std::vector<std::string> keys;
    for (int i = 1000; i < 2000; i += 2) {
        keys.push_back(std::to_string(i));
        builder.add_item(keys.back());
    }
    std::vector<Slice> slices;
    PageFooterPB footer;
    auto st = builder.finalize(9000 * 1024, &slices, &footer, /*short_key_encoding=*/SHORT_KEY_ENCODING_FULL_SORT_KEY,
                               /*num_sort_key_columns=*/3);
    ASSERT_TRUE(st.ok());

    std::string buf;
    for (auto& slice : slices) {
        buf.append(slice.data, slice.size);
    }

    ShortKeyIndexDecoder decoder;
    st = decoder.parse(buf, footer.short_key_page_footer());
    ASSERT_TRUE(st.ok());

    ASSERT_EQ(SHORT_KEY_ENCODING_FULL_SORT_KEY, decoder.short_key_encoding());
    ASSERT_EQ(3, decoder.num_sort_key_columns());

    // entries still round-trip byte-for-byte
    ASSERT_EQ(keys.size(), decoder.num_items());
    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_EQ(keys[i], decoder.key(i).to_string());
    }
}

TEST_F(ShortKeyIndexTest, finalize_with_default_encoding_metadata) {
    ShortKeyIndexBuilder builder(0, 1024);
    builder.add_item("abc");

    std::vector<Slice> slices;
    PageFooterPB footer;
    auto st = builder.finalize(1024, &slices, &footer);
    ASSERT_TRUE(st.ok());

    ShortKeyIndexDecoder decoder;
    std::string buf;
    for (auto& slice : slices) {
        buf.append(slice.data, slice.size);
    }
    st = decoder.parse(buf, footer.short_key_page_footer());
    ASSERT_TRUE(st.ok());

    ASSERT_EQ(SHORT_KEY_ENCODING_TRUNCATED, decoder.short_key_encoding());
    ASSERT_EQ(0, decoder.num_sort_key_columns());
}

// parse() must runtime-validate the offset table (not just DCHECK) so a checksum-valid page with a
// corrupt/out-of-range offset is rejected cleanly instead of letting key(i) read out of bounds.
TEST_F(ShortKeyIndexTest, parse_rejects_out_of_range_offset) {
    ShortKeyIndexBuilder builder(0, 1024);
    builder.add_item("aaaa");
    builder.add_item("bbbb");
    builder.add_item("cccc");
    std::vector<Slice> slices;
    PageFooterPB footer;
    ASSERT_TRUE(builder.finalize(3000, &slices, &footer).ok());
    const uint32_t key_bytes = footer.short_key_page_footer().key_bytes();

    std::string buf;
    for (auto& slice : slices) {
        buf.append(slice.data, slice.size);
    }
    // A well-formed page parses.
    {
        ShortKeyIndexDecoder ok_decoder;
        ASSERT_TRUE(ok_decoder.parse(buf, footer.short_key_page_footer()).ok());
    }
    // The offset region begins at key_bytes; overwrite the last offset varint with a value that
    // exceeds key_bytes -> parse() must reject with Corruption (single-byte varint, high bit clear).
    ASSERT_GT(buf.size(), static_cast<size_t>(key_bytes));
    buf[buf.size() - 1] = static_cast<char>(0x7F); // 127 > key_bytes
    ShortKeyIndexDecoder bad_decoder;
    Status st = bad_decoder.parse(buf, footer.short_key_page_footer());
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_corruption());
}

} // namespace starrocks
