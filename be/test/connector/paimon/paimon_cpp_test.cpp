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

#include <gtest/gtest.h>
#include <paimon/catalog/catalog.h>
#include <paimon/defs.h>
#include <paimon/status.h>

#include <filesystem>
#include <map>
#include <string>
#include <utility>

namespace starrocks {

TEST(PaimonCppTest, test_status_basic) {
    ASSERT_TRUE(paimon::Status::OK().ok());
    paimon::Status invalid = paimon::Status::Invalid("invalid for ut");
    ASSERT_FALSE(invalid.ok());
    ASSERT_NE(invalid.ToString().find("invalid for ut"), std::string::npos);
}

} // namespace starrocks
