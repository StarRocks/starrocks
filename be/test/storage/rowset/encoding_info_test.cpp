// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/rowset/segment_v2/encoding_info_test.cpp

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

#include "storage/rowset/encoding_info.h"

#include <gtest/gtest.h>

#include <iostream>

#include "gen_cpp/segment.pb.h"
#include "storage/olap_common.h"
#include "storage/types.h"

namespace starrocks {

class EncodingInfoTest : public testing::Test {
public:
    EncodingInfoTest() = default;
    ~EncodingInfoTest() override = default;
};

TEST_F(EncodingInfoTest, normal) {
    auto type_info = get_type_info(LOGICAL_TYPE_BIGINT);
    const EncodingInfo* encoding_info = nullptr;
    auto status = EncodingInfo::get(LOGICAL_TYPE_BIGINT, PLAIN_ENCODING, &encoding_info);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(nullptr, encoding_info);
}

TEST_F(EncodingInfoTest, no_encoding) {
    auto type_info = get_type_info(LOGICAL_TYPE_BIGINT);
    const EncodingInfo* encoding_info = nullptr;
    auto status = EncodingInfo::get(LOGICAL_TYPE_BIGINT, DICT_ENCODING, &encoding_info);
    ASSERT_FALSE(status.ok());
}

TEST_F(EncodingInfoTest, default_encoding) {
    std::map<LogicalType, EncodingTypePB> default_expected = {
            {LOGICAL_TYPE_TINYINT, BIT_SHUFFLE},
            {LOGICAL_TYPE_SMALLINT, BIT_SHUFFLE},
            {LOGICAL_TYPE_INT, BIT_SHUFFLE},
            {LOGICAL_TYPE_BIGINT, BIT_SHUFFLE},
            {LOGICAL_TYPE_LARGEINT, BIT_SHUFFLE},

            {LOGICAL_TYPE_FLOAT, BIT_SHUFFLE},
            {LOGICAL_TYPE_DOUBLE, BIT_SHUFFLE},

            {LOGICAL_TYPE_CHAR, DICT_ENCODING},
            {LOGICAL_TYPE_VARCHAR, DICT_ENCODING},

            {LOGICAL_TYPE_BOOLEAN, RLE},

            {LOGICAL_TYPE_DATE_V1, BIT_SHUFFLE},
            {LOGICAL_TYPE_DATE, BIT_SHUFFLE},
            {LOGICAL_TYPE_DATETIME_V1, BIT_SHUFFLE},
            {LOGICAL_TYPE_DATETIME, BIT_SHUFFLE},

            {LOGICAL_TYPE_DECIMAL, BIT_SHUFFLE},
            {LOGICAL_TYPE_DECIMALV2, BIT_SHUFFLE},

            {LOGICAL_TYPE_HLL, PLAIN_ENCODING},
            {LOGICAL_TYPE_OBJECT, PLAIN_ENCODING},
            {LOGICAL_TYPE_PERCENTILE, PLAIN_ENCODING},
            {LOGICAL_TYPE_JSON, PLAIN_ENCODING},
    };
    std::map<LogicalType, EncodingTypePB> value_seek_expected = {
            {LOGICAL_TYPE_TINYINT, FOR_ENCODING},     {LOGICAL_TYPE_SMALLINT, FOR_ENCODING},
            {LOGICAL_TYPE_INT, FOR_ENCODING},         {LOGICAL_TYPE_BIGINT, FOR_ENCODING},
            {LOGICAL_TYPE_LARGEINT, FOR_ENCODING},    {LOGICAL_TYPE_CHAR, PREFIX_ENCODING},
            {LOGICAL_TYPE_VARCHAR, PREFIX_ENCODING},  {LOGICAL_TYPE_BOOLEAN, PLAIN_ENCODING},
            {LOGICAL_TYPE_DATE_V1, FOR_ENCODING},     {LOGICAL_TYPE_DATE, FOR_ENCODING},
            {LOGICAL_TYPE_DATETIME_V1, FOR_ENCODING}, {LOGICAL_TYPE_DATETIME, FOR_ENCODING},
            {LOGICAL_TYPE_DECIMALV2, BIT_SHUFFLE},    {LOGICAL_TYPE_DECIMAL, BIT_SHUFFLE},
    };
    for (auto [type, encoding] : default_expected) {
        auto default_encoding = EncodingInfo::get_default_encoding(type, false);
        EXPECT_EQ(default_encoding, encoding);
    }
    for (auto [type, encoding] : value_seek_expected) {
        auto default_encoding = EncodingInfo::get_default_encoding(type, true);
        EXPECT_EQ(default_encoding, encoding);
    }
}

} // namespace starrocks
