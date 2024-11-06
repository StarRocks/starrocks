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

#include "runtime/batch_write/batch_write_util.h"

#include <gtest/gtest.h>

#include "http/http_common.h"
#include "http/http_request.h"

namespace starrocks {

TEST(BatchWriteUtilTest, batch_write_id_hash) {
    BatchWriteId id1{"db1", "table1", {{"param1", "value1"}, {"param2", "value2"}}};
    BatchWriteId id2{"db1", "table1", {{"param1", "value1"}, {"param2", "value2"}}};
    BatchWriteId id3{"db2", "table2", {{"param3", "value3"}}};

    BatchWriteIdHash hash_fn;
    ASSERT_EQ(hash_fn(id1), hash_fn(id2));
    ASSERT_NE(hash_fn(id1), hash_fn(id3));
}

TEST(BatchWriteUtilTest, batch_write_id_equal) {
    BatchWriteId id1{"db1", "table1", {{"param1", "value1"}, {"param2", "value2"}}};
    BatchWriteId id2{"db1", "table1", {{"param1", "value1"}, {"param2", "value2"}}};
    BatchWriteId id3{"db2", "table2", {{"param3", "value3"}}};

    BatchWriteIdEqual equal_fn;
    ASSERT_TRUE(equal_fn(id1, id2));
    ASSERT_FALSE(equal_fn(id1, id3));
}

TEST(BatchWriteUtilTest, get_load_parameters_from_brpc) {
    std::map<std::string, std::string> input_params = {
            {HTTP_FORMAT_KEY, "json"}, {HTTP_COLUMNS, "col1,col2"}, {HTTP_TIMEOUT, "30"}};
    auto load_params = get_load_parameters_from_brpc(input_params);
    ASSERT_EQ(load_params[HTTP_FORMAT_KEY], "json");
    ASSERT_EQ(load_params[HTTP_COLUMNS], "col1,col2");
    ASSERT_EQ(load_params[HTTP_TIMEOUT], "30");
}

TEST(BatchWriteUtilTest, get_load_parameters_from_http) {
    HttpRequest http_req(nullptr);
    http_req._headers.emplace(HTTP_FORMAT_KEY, "json");
    http_req._headers.emplace(HTTP_COLUMNS, "col1,col2");
    http_req._headers.emplace(HTTP_TIMEOUT, "30");

    auto load_params = get_load_parameters_from_http(&http_req);
    ASSERT_EQ(load_params[HTTP_FORMAT_KEY], "json");
    ASSERT_EQ(load_params[HTTP_COLUMNS], "col1,col2");
    ASSERT_EQ(load_params[HTTP_TIMEOUT], "30");
}

} // namespace starrocks
