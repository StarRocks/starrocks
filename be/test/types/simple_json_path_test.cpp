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

#include "types/simple_json_path.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"

namespace starrocks {

class SimpleJsonPathTest : public ::testing::Test {
public:
    Status test_extract_from_object(std::string input, const std::string& jsonpath, std::string* output) {
        input.reserve(input.size() + simdjson::SIMDJSON_PADDING);

        simdjson::ondemand::parser parser;
        simdjson::ondemand::document doc;
        EXPECT_EQ(simdjson::error_code::SUCCESS, parser.iterate(input).get(doc));

        simdjson::ondemand::object obj;
        EXPECT_EQ(simdjson::error_code::SUCCESS, doc.get_object().get(obj));

        std::vector<SimpleJsonPath> path;
        RETURN_IF_ERROR(parse_simple_json_paths(jsonpath, &path));

        simdjson::ondemand::value val;
        RETURN_IF_ERROR(extract_from_object(obj, path, &val));
        std::string_view sv = simdjson::to_json_string(val);

        output->assign(sv.data(), sv.size());
        return Status::OK();
    }
};

TEST_F(SimpleJsonPathTest, extract_from_object) {
    std::string output;

    EXPECT_OK(test_extract_from_object(R"({"data" : 1})", "$.data", &output));
    EXPECT_STREQ(output.data(), "1");

    EXPECT_STATUS(Status::NotFound(""), test_extract_from_object(R"({"data" : 1})", "$.dataa", &output));

    EXPECT_OK(test_extract_from_object(R"({"data": [{"key": 1},{"key": 2}]})", "$.data[1].key", &output));
    EXPECT_STREQ(output.data(), "2");

    EXPECT_STATUS(Status::NotFound(""),
                  test_extract_from_object(R"({"data": [{"key": 1},{"key": 2}]})", "$.data[2].key", &output));

    EXPECT_STATUS(Status::NotFound(""),
                  test_extract_from_object(R"({"data": [{"key": 1},{"key": 2}]})", "$.data[3].key", &output));

    EXPECT_STATUS(Status::DataQualityError(""),
                  test_extract_from_object(R"({"data1 " : 1, "data2":})", "$.data", &output));

    EXPECT_STATUS(Status::NotFound(""), test_extract_from_object(R"({"data": null})", "$.data", &output));
    EXPECT_STATUS(Status::NotFound(""), test_extract_from_object(R"({"data": null})", "$.data.key", &output));

    EXPECT_OK(test_extract_from_object(R"({"data": {}})", "$.data", &output));
    EXPECT_STREQ(output.data(), "{}");
    EXPECT_STATUS(Status::NotFound(""), test_extract_from_object(R"({"data": {}})", "$.data.key", &output));

    EXPECT_STATUS(Status::NotFound(""), test_extract_from_object(R"({"data": 1})", "$.data.key", &output));

    EXPECT_OK(test_extract_from_object(R"({"key1": [1,2]})", "$.key1[1]", &output));
    EXPECT_STREQ(output.data(), "2");

    EXPECT_OK(test_extract_from_object(R"({"key1": [{"key2":3},{"key4": 5}]})", "$.key1[1].key4", &output));
    EXPECT_STREQ(output.data(), "5");

    EXPECT_STATUS(Status::NotFound(""), test_extract_from_object(R"({"key1": null})", "$.key1[1].key4", &output));
}

} // namespace starrocks
