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

#include "storage/rowset/segment_v2/encoding_info.h"

#include <gtest/gtest.h>

#include <iostream>

#include "common/logging.h"
#include "storage/olap_common.h"
#include "storage/types.h"

namespace starrocks {
namespace segment_v2 {

class EncodingInfoTest : public testing::Test {
public:
    EncodingInfoTest() {}
    virtual ~EncodingInfoTest() {}
};

TEST_F(EncodingInfoTest, normal) {
    auto type_info = get_type_info(OLAP_FIELD_TYPE_BIGINT);
    const EncodingInfo* encoding_info = nullptr;
    auto status = EncodingInfo::get(OLAP_FIELD_TYPE_BIGINT, PLAIN_ENCODING, &encoding_info);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(nullptr, encoding_info);
}

TEST_F(EncodingInfoTest, no_encoding) {
    auto type_info = get_type_info(OLAP_FIELD_TYPE_BIGINT);
    const EncodingInfo* encoding_info = nullptr;
    auto status = EncodingInfo::get(OLAP_FIELD_TYPE_BIGINT, DICT_ENCODING, &encoding_info);
    ASSERT_FALSE(status.ok());
}

} // namespace segment_v2
} // namespace starrocks
