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

#include "connector/hive/paimon/paimon_scanner.h"

#include <gtest/gtest.h>

#include <limits>

namespace starrocks {

TEST(PaimonScannerTest, validate_read_batch_size_accepts_positive_int32) {
    ASSERT_TRUE(PaimonScanner::validate_read_batch_size(1).ok());
    ASSERT_TRUE(PaimonScanner::validate_read_batch_size(10000).ok());
    ASSERT_TRUE(PaimonScanner::validate_read_batch_size(std::numeric_limits<int32_t>::max()).ok());
}

TEST(PaimonScannerTest, validate_read_batch_size_rejects_non_positive) {
    auto st = PaimonScanner::validate_read_batch_size(0);
    ASSERT_TRUE(st.is_invalid_argument()) << st;
    ASSERT_TRUE(PaimonScanner::validate_read_batch_size(-1).is_invalid_argument());
}

TEST(PaimonScannerTest, validate_read_batch_size_rejects_int32_overflow) {
    int64_t too_large = static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1;
    ASSERT_TRUE(PaimonScanner::validate_read_batch_size(too_large).is_invalid_argument());
}

} // namespace starrocks
