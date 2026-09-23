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

#include "connector/hive/paimon/paimon_global_index_scanner.h"

#include <gtest/gtest.h>

#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {

class PaimonGlobalIndexScannerTestAccessor {
public:
    static Status parse_request(const TPaimonGlobalIndexScanRange& scan_range, rapidjson::Document* request) {
        return PaimonGlobalIndexScanner::_parse_request(scan_range, request);
    }
};

namespace {

TPaimonGlobalIndexScanRange valid_scan_range() {
    TPaimonGlobalIndexScanRange scan_range;
    scan_range.__set_protocol_version(1);
    scan_range.__set_shard_id(0);
    scan_range.__set_range_from(0);
    scan_range.__set_range_to(9);
    scan_range.__set_query_json(
            R"({"version":1,"snapshotId":7,"predicate":{"o":"isn","ng":false,"c":[{"o":"cr","t":"int","n":"k"}]},"indexes":{"k":"range"}})");
    scan_range.__set_table_path("s3://warehouse/db/table");
    scan_range.__set_snapshot_id(7);
    return scan_range;
}

Status parse_request(const TPaimonGlobalIndexScanRange& scan_range) {
    rapidjson::Document request;
    return PaimonGlobalIndexScannerTestAccessor::parse_request(scan_range, &request);
}

TEST(PaimonGlobalIndexScannerTest, AcceptsConsistentVersionedRequest) {
    auto scan_range = valid_scan_range();
    rapidjson::Document request;

    Status status = PaimonGlobalIndexScannerTestAccessor::parse_request(scan_range, &request);

    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(request.IsObject());
    EXPECT_EQ(7, request["snapshotId"].GetInt64());
}

TEST(PaimonGlobalIndexScannerTest, RejectsIncompleteAndUnsupportedProtocol) {
    TPaimonGlobalIndexScanRange incomplete;
    incomplete.__set_protocol_version(1);
    EXPECT_TRUE(parse_request(incomplete).is_invalid_argument());

    auto unsupported = valid_scan_range();
    unsupported.__set_protocol_version(2);
    Status status = parse_request(unsupported);
    EXPECT_TRUE(status.is_invalid_argument());
    EXPECT_NE(std::string::npos, status.message().find("protocol version 2"));
}

TEST(PaimonGlobalIndexScannerTest, RejectsInvalidShardAndJson) {
    auto invalid_range = valid_scan_range();
    invalid_range.__set_range_from(10);
    invalid_range.__set_range_to(9);
    EXPECT_TRUE(parse_request(invalid_range).is_invalid_argument());

    auto invalid_json = valid_scan_range();
    invalid_json.__set_query_json("not-json");
    EXPECT_TRUE(parse_request(invalid_json).is_invalid_argument());
}

TEST(PaimonGlobalIndexScannerTest, RejectsInconsistentEnvelope) {
    auto snapshot_mismatch = valid_scan_range();
    snapshot_mismatch.__set_snapshot_id(8);
    EXPECT_TRUE(parse_request(snapshot_mismatch).is_invalid_argument());

    auto missing_indexes = valid_scan_range();
    missing_indexes.__set_query_json(R"({"version":1,"snapshotId":7,"predicate":{"o":"isn","ng":false,"c":[]}})");
    EXPECT_TRUE(parse_request(missing_indexes).is_invalid_argument());

    auto missing_predicate = valid_scan_range();
    missing_predicate.__set_query_json(R"({"version":1,"snapshotId":7,"indexes":{"k":"range"}})");
    EXPECT_TRUE(parse_request(missing_predicate).is_invalid_argument());
}

} // namespace
} // namespace starrocks
