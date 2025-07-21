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

#include "service/service_be/arrow_flight_call_header_utils.h"

#include <gtest/gtest.h>

#include "exec/arrow_flight_batch_reader.h"
#include "runtime/result_buffer_mgr.h"

namespace starrocks {

using arrow::flight::CallHeaders;

class ArrowFlightCallHeaderUtilsTest : public ::testing::Test {
protected:
    CallHeaders headers;
};

TEST_F(ArrowFlightCallHeaderUtilsTest, FindKeyValPrefixInCallHeaders_KeyNotFound) {
    std::string result = FindKeyValPrefixInCallHeaders(headers, "notfound", "Basic ");
    ASSERT_EQ(result, "");
}

TEST_F(ArrowFlightCallHeaderUtilsTest, FindKeyValPrefixInCallHeaders_PrefixNotMatch) {
    headers.insert({kAuthHeader, "Token abcdefg"});
    std::string result = FindKeyValPrefixInCallHeaders(headers, kAuthHeader, kBasicPrefix);
    ASSERT_EQ(result, "");
}

TEST_F(ArrowFlightCallHeaderUtilsTest, FindKeyValPrefixInCallHeaders_PrefixMatch) {
    headers.insert({kAuthHeader, "Basic abcdefg"});
    std::string result = FindKeyValPrefixInCallHeaders(headers, kAuthHeader, kBasicPrefix);
    ASSERT_EQ(result, "abcdefg");
}

TEST_F(ArrowFlightCallHeaderUtilsTest, ParseBasicHeader) {
    std::string encoded = arrow::util::base64_encode("user:pass");
    std::string full_header = std::string(kBasicPrefix) + encoded;

    headers.insert({kAuthHeader, full_header});

    std::string username, password;
    ParseBasicHeader(headers, username, password);

    ASSERT_EQ(username, "user");
    ASSERT_EQ(password, "pass");
}

TEST_F(ArrowFlightCallHeaderUtilsTest, NotExistQueryId) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    auto reader = std::make_shared<ArrowFlightBatchReader>(&buffer_mgr, query_id);
    ASSERT_TRUE(!reader->init().ok());
}

} // namespace starrocks
