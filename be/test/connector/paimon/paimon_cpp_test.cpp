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

#include <filesystem>
#include <map>
#include <string>
#include <utility>

#include "paimon/catalog/catalog.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace starrocks {

TEST(PaimonCppTest, test_status_basic) {
    ASSERT_TRUE(paimon::Status::OK().ok());
    paimon::Status invalid = paimon::Status::Invalid("invalid for ut");
    ASSERT_FALSE(invalid.ok());
    ASSERT_NE(invalid.ToString().find("invalid for ut"), std::string::npos);
}

TEST(PaimonCppTest, test_create_local_catalog) {
    auto root = std::filesystem::temp_directory_path() / "paimon_cpp_ut_warehouse";
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);

    std::map<std::string, std::string> options;
    options[paimon::Options::FILE_SYSTEM] = "local";
    auto catalog_res = paimon::Catalog::Create(root.string(), options);
    ASSERT_TRUE(catalog_res.ok()) << catalog_res.status().ToString();
    auto catalog = std::move(catalog_res).value();

    auto st = catalog->CreateDatabase("paimon_cpp_ut_db", {}, /*ignore_if_exists=*/true);
    ASSERT_TRUE(st.ok()) << st.ToString();

    auto exists = catalog->DatabaseExists("paimon_cpp_ut_db");
    ASSERT_TRUE(exists.ok()) << exists.status().ToString();
    ASSERT_TRUE(exists.value());

    std::filesystem::remove_all(root);
}

} // namespace starrocks
