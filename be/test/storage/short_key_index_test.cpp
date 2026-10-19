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

#include "storage/short_key_index.h"

#include <gtest/gtest.h>

#include <limits>

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

// A footer claiming UINT32_MAX items makes `num_items + 1` wrap to zero, leaving _offsets empty --
// after which the first loop iteration writes through its null data pointer. Every value here comes
// from a persisted page footer, so a corrupt or crafted segment reaches this through the ordinary
// read path and parse() has to reject the shape instead.
TEST_F(ShortKeyIndexTest, parse_rejects_num_items_overflow) {
    ShortKeyFooterPB footer;
    footer.set_num_items(std::numeric_limits<uint32_t>::max());
    footer.set_key_bytes(0);
    footer.set_offset_bytes(1);

    // One 0x00 byte: a valid varint decoding to offset 0, which clears both of the old guards
    // (0 is not below prev_offset 0, and 0 is not above key_bytes 0).
    const std::string body(1, '\0');

    ShortKeyIndexDecoder decoder;
    auto st = decoder.parse(Slice(body), footer);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
}

// key_bytes + offset_bytes was uint32 arithmetic compared against a size_t body size, so this pair
// sums to zero and a zero-byte body passed the check. Slice(body.data, key_bytes) would then claim
// 4 GB and body.data + key_bytes would be pointer arithmetic far past the buffer.
TEST_F(ShortKeyIndexTest, parse_rejects_key_offset_bytes_overflow) {
    ShortKeyFooterPB footer;
    footer.set_num_items(0);
    footer.set_key_bytes(std::numeric_limits<uint32_t>::max());
    footer.set_offset_bytes(1);

    ShortKeyIndexDecoder decoder;
    auto st = decoder.parse(Slice(), footer);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
}

// An offset past key_bytes makes key(i) yield a Slice reaching outside the key region. This was
// guarded only by a DCHECK, which compiles away in a release build, so the page was accepted and
// read out of bounds there.
TEST_F(ShortKeyIndexTest, parse_rejects_out_of_range_offset) {
    ShortKeyFooterPB footer;
    footer.set_num_items(1);
    footer.set_key_bytes(4);
    footer.set_offset_bytes(1);

    std::string body("abcd");
    body.push_back(static_cast<char>(9)); // varint offset 9, past the 4-byte key region

    ShortKeyIndexDecoder decoder;
    auto st = decoder.parse(Slice(body), footer);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
}

} // namespace starrocks
