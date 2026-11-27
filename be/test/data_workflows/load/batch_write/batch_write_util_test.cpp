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

#include "data_workflows/load/batch_write/batch_write_util.h"

#include <gtest/gtest.h>

#include <string>

#include "base/testutil/assert.h"
#include "compute_env/load/http_load_params.h"
#include "platform/http/http_request.h"

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

TEST(BatchWriteUtilTest, check_merge_commit_skip_header) {
    const std::string not_supported = "skip_header is not supported by merge commit";
    // Every CSV format name, in any case, and no format at all, which is CSV.
    for (const char* format : {"", "csv", "CSV", "gzip", "bzip2", "lz4", "Lz4", "deflate", "zstd"}) {
        Status st = check_merge_commit_skip_header(format, "1");
        ASSERT_TRUE(st.is_invalid_argument()) << format << ": " << st;
        ASSERT_NE(std::string::npos, st.message().find(not_supported)) << format << ": " << st;
        ASSERT_OK(check_merge_commit_skip_header(format, "0"));
        ASSERT_OK(check_merge_commit_skip_header(format, ""));
    }
    // The other formats do not use skip_header, and this check does not parse it for them.
    for (const char* format : {"json", "JSON", "arrow"}) {
        ASSERT_OK(check_merge_commit_skip_header(format, "1"));
        ASSERT_OK(check_merge_commit_skip_header(format, "abc"));
    }
    // A CSV value is checked, and reported, as for the classic Stream Load.
    Status negative = check_merge_commit_skip_header("csv", "-1");
    ASSERT_TRUE(negative.is_invalid_argument()) << negative;
    ASSERT_NE(std::string::npos, negative.message().find("skip_header must be equal or greater than 0")) << negative;
    for (const char* value : {"abc", "1x"}) {
        Status invalid = check_merge_commit_skip_header("csv", value);
        ASSERT_TRUE(invalid.is_invalid_argument()) << value << ": " << invalid;
        ASSERT_NE(std::string::npos,
                  invalid.message().find("Invalid parameter skip_header. The value must be an integer"))
                << value << ": " << invalid;
    }
    Status overflow = check_merge_commit_skip_header("csv", "99999999999999999999");
    ASSERT_TRUE(overflow.is_invalid_argument()) << overflow;
    ASSERT_NE(std::string::npos, overflow.message().find("Invalid parameter skip_header. The value must be between"))
            << overflow;
}

} // namespace starrocks
