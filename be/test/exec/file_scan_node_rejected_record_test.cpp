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

// Tests for check_rejected_record_format_support() extracted from
// FileScanNode::_scanner_scan (file_scan_node.cpp lines 240-245).
//
// Covered lines:
//   242  format_type != FORMAT_JSON
//   243  format_type != FORMAT_PARQUET
//   244  format_type != FORMAT_ORC
//   245  return Status::InternalError(...)

#include <gtest/gtest.h>

#include "common/status.h"
#include "exec/file_scan_node.h"
#include "gen_cpp/InternalService_types.h"

namespace starrocks {

// ===========================================================================
// check_rejected_record_format_support:
//   When logging is disabled the check is always OK regardless of format.
// ===========================================================================

TEST(CheckRejectedRecordFormatSupportTest, LoggingDisabledAlwaysOk) {
    // logging_enabled = false -> skip check entirely.
    EXPECT_TRUE(check_rejected_record_format_support(false, TFileFormatType::FORMAT_AVRO).ok());
    EXPECT_TRUE(check_rejected_record_format_support(false, TFileFormatType::FORMAT_CSV_PLAIN).ok());
    EXPECT_TRUE(check_rejected_record_format_support(false, TFileFormatType::FORMAT_JSON).ok());
    EXPECT_TRUE(check_rejected_record_format_support(false, TFileFormatType::FORMAT_PARQUET).ok());
    EXPECT_TRUE(check_rejected_record_format_support(false, TFileFormatType::FORMAT_ORC).ok());
}

// ===========================================================================
// check_rejected_record_format_support:
//   When logging is enabled, supported formats (CSV/JSON/PARQUET/ORC) return OK.
// ===========================================================================

TEST(CheckRejectedRecordFormatSupportTest, SupportedFormatsReturnOkWhenLoggingEnabled) {
    EXPECT_TRUE(check_rejected_record_format_support(true, TFileFormatType::FORMAT_CSV_PLAIN).ok());
    EXPECT_TRUE(check_rejected_record_format_support(true, TFileFormatType::FORMAT_JSON).ok());
    EXPECT_TRUE(check_rejected_record_format_support(true, TFileFormatType::FORMAT_PARQUET).ok());
    EXPECT_TRUE(check_rejected_record_format_support(true, TFileFormatType::FORMAT_ORC).ok());
}

// ===========================================================================
// check_rejected_record_format_support:
//   Unsupported format with logging enabled returns InternalError (covers
//   the branch at file_scan_node.cpp lines 240-245).
// ===========================================================================

TEST(CheckRejectedRecordFormatSupportTest, UnsupportedFormatWithLoggingEnabledReturnsError) {
    // FORMAT_AVRO is not in the whitelist.
    auto st = check_rejected_record_format_support(true, TFileFormatType::FORMAT_AVRO);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(std::string::npos, st.message().find("only support csv/json/parquet/orc"));
}

// ===========================================================================
// check_rejected_record_format_support:
//   Check each whitelisted format individually to ensure none triggers the
//   error branch.  This guards against off-by-one edits in the condition.
// ===========================================================================

TEST(CheckRejectedRecordFormatSupportTest, EachWhitelistedFormatIndividuallyOk) {
    const std::vector<TFileFormatType::type> ok_formats = {
            TFileFormatType::FORMAT_CSV_PLAIN,
            TFileFormatType::FORMAT_JSON,
            TFileFormatType::FORMAT_PARQUET,
            TFileFormatType::FORMAT_ORC,
    };
    for (auto fmt : ok_formats) {
        auto st = check_rejected_record_format_support(true, fmt);
        EXPECT_TRUE(st.ok()) << "Expected OK for format " << static_cast<int>(fmt) << " but got: " << st.message();
    }
}

// ===========================================================================
// check_rejected_record_format_support:
//   Error message is exactly the canonical string used in both call sites.
// ===========================================================================

TEST(CheckRejectedRecordFormatSupportTest, ErrorMessageMatchesCanonicalString) {
    auto st = check_rejected_record_format_support(true, TFileFormatType::FORMAT_AVRO);
    EXPECT_FALSE(st.ok());
    // Both file_scan_node.cpp and file_connector.cpp use the same message.
    EXPECT_STREQ("only support csv/json/parquet/orc format to log rejected record", st.message().c_str());
}

} // namespace starrocks
