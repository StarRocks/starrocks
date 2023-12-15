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
    EncodingInfoTest() {
        _number_types_supports_dict_encoding.emplace(TYPE_TINYINT);
        _number_types_supports_dict_encoding.emplace(TYPE_SMALLINT);
        _number_types_supports_dict_encoding.emplace(TYPE_INT);
        _number_types_supports_dict_encoding.emplace(TYPE_BIGINT);
        _number_types_supports_dict_encoding.emplace(TYPE_LARGEINT);
        _number_types_supports_dict_encoding.emplace(TYPE_FLOAT);
        _number_types_supports_dict_encoding.emplace(TYPE_DOUBLE);
        _number_types_supports_dict_encoding.emplace(TYPE_DATE);
        _number_types_supports_dict_encoding.emplace(TYPE_DATETIME);
        _number_types_supports_dict_encoding.emplace(TYPE_DECIMALV2);
    }

    ~EncodingInfoTest() override = default;
    std::set<LogicalType> _number_types_supports_dict_encoding;
};

TEST_F(EncodingInfoTest, normal) {
    auto type_info = get_type_info(TYPE_BIGINT);
    const EncodingInfo* encoding_info = nullptr;
    auto status = EncodingInfo::get(TYPE_BIGINT, PLAIN_ENCODING, &encoding_info);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(nullptr, encoding_info);
}

TEST_F(EncodingInfoTest, no_encoding) {
    auto type_info = get_type_info(TYPE_BIGINT);
    const EncodingInfo* encoding_info = nullptr;
    auto status = EncodingInfo::get(TYPE_BIGINT, RLE, &encoding_info);
    ASSERT_FALSE(status.ok());
}

TEST_F(EncodingInfoTest, number_types_supports_dict_encoding) {
    for (auto logicType : _number_types_supports_dict_encoding) {
        EXPECT_EQ(true, number_types_supports_dict_encoding(logicType));
        EXPECT_EQ(true, supports_dict_encoding(logicType));
    }
    EXPECT_EQ(false, number_types_supports_dict_encoding(TYPE_CHAR));
    EXPECT_EQ(false, number_types_supports_dict_encoding(TYPE_VARCHAR));
    EXPECT_EQ(true, supports_dict_encoding(TYPE_CHAR));
    EXPECT_EQ(true, supports_dict_encoding(TYPE_VARCHAR));
}

TEST_F(EncodingInfoTest, enable_non_string_column_dict_encoding) {
    EXPECT_EQ(false, enable_non_string_column_dict_encoding());
    config::dictionary_encoding_ratio_for_non_string_column = 0.7;
    EXPECT_EQ(true, enable_non_string_column_dict_encoding());
    config::dictionary_encoding_ratio_for_non_string_column = 1;
    EXPECT_EQ(true, enable_non_string_column_dict_encoding());
    config::dictionary_encoding_ratio_for_non_string_column = 0;
    EXPECT_EQ(false, enable_non_string_column_dict_encoding());
}

TEST_F(EncodingInfoTest, get_default_encoding_number_types) {
    for (auto logicType : _number_types_supports_dict_encoding) {
        EXPECT_EQ(BIT_SHUFFLE, EncodingInfo::get_default_encoding(logicType, false));
        const EncodingInfo* encoding_info;
        auto status = EncodingInfo::get(logicType, DEFAULT_ENCODING, &encoding_info);
        ASSERT_FALSE(status.ok());
        EXPECT_EQ(BIT_SHUFFLE, encoding_info->encoding());

        status = EncodingInfo::get(logicType, DICT_ENCODING, &encoding_info);
        ASSERT_FALSE(status.ok());
        EXPECT_EQ(DICT_ENCODING, encoding_info->encoding());
    }

    config::dictionary_encoding_ratio_for_non_string_column = 0.7;
    for (auto logicType : _number_types_supports_dict_encoding) {
        EXPECT_EQ(DICT_ENCODING, EncodingInfo::get_default_encoding(logicType, false));
        const EncodingInfo* encoding_info;
        auto status = EncodingInfo::get(logicType, DEFAULT_ENCODING, &encoding_info);
        ASSERT_FALSE(status.ok());
        EXPECT_EQ(DICT_ENCODING, encoding_info->encoding());

        status = EncodingInfo::get(logicType, DICT_ENCODING, &encoding_info);
        ASSERT_FALSE(status.ok());
        EXPECT_EQ(DICT_ENCODING, encoding_info->encoding());
    }
}

TEST_F(EncodingInfoTest, default_encoding) {
    std::map<LogicalType, EncodingTypePB> default_expected = {
            {TYPE_TINYINT, BIT_SHUFFLE},  {TYPE_SMALLINT, BIT_SHUFFLE},  {TYPE_INT, BIT_SHUFFLE},
            {TYPE_BIGINT, BIT_SHUFFLE},   {TYPE_LARGEINT, BIT_SHUFFLE},

            {TYPE_FLOAT, BIT_SHUFFLE},    {TYPE_DOUBLE, BIT_SHUFFLE},

            {TYPE_CHAR, DICT_ENCODING},   {TYPE_VARCHAR, DICT_ENCODING},

            {TYPE_BOOLEAN, RLE},

            {TYPE_DATE_V1, BIT_SHUFFLE},  {TYPE_DATE, BIT_SHUFFLE},      {TYPE_DATETIME_V1, BIT_SHUFFLE},
            {TYPE_DATETIME, BIT_SHUFFLE},

            {TYPE_DECIMAL, BIT_SHUFFLE},  {TYPE_DECIMALV2, BIT_SHUFFLE},

            {TYPE_HLL, PLAIN_ENCODING},   {TYPE_OBJECT, PLAIN_ENCODING}, {TYPE_PERCENTILE, PLAIN_ENCODING},
            {TYPE_JSON, PLAIN_ENCODING},
    };
    std::map<LogicalType, EncodingTypePB> value_seek_expected = {
            {TYPE_TINYINT, FOR_ENCODING},    {TYPE_SMALLINT, FOR_ENCODING},    {TYPE_INT, FOR_ENCODING},
            {TYPE_BIGINT, FOR_ENCODING},     {TYPE_LARGEINT, FOR_ENCODING},    {TYPE_CHAR, PREFIX_ENCODING},
            {TYPE_VARCHAR, PREFIX_ENCODING}, {TYPE_BOOLEAN, PLAIN_ENCODING},   {TYPE_DATE_V1, FOR_ENCODING},
            {TYPE_DATE, FOR_ENCODING},       {TYPE_DATETIME_V1, FOR_ENCODING}, {TYPE_DATETIME, FOR_ENCODING},
            {TYPE_DECIMALV2, BIT_SHUFFLE},   {TYPE_DECIMAL, BIT_SHUFFLE},
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
