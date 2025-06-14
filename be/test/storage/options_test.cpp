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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/options_test.cpp

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

#include "storage/options.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

namespace starrocks {

static void set_up() {
    [[maybe_unused]] auto res = system("rm -rf ./test_run && mkdir -p ./test_run");
    res = system("mkdir -p ./test_run/data && mkdir -p ./test_run/data.ssd");
}

static void tear_down() {
    [[maybe_unused]] auto res = system("rm -rf ./test_run");
}

class OptionsTest : public testing::Test {
public:
    OptionsTest() = default;
    ~OptionsTest() override = default;

    static void SetUpTestSuite() { set_up(); }
    static void TearDownTestSuite() { tear_down(); }
};

TEST_F(OptionsTest, parse_root_path) {
    std::string path_prefix = std::filesystem::absolute("./test_run").string();
    std::string path1 = path_prefix + "/data";
    std::string path2 = path_prefix + "/data.ssd";

    std::string root_path;
    StorePath path;

    // /path<.extension>
    {
        root_path = path1;
        ASSERT_TRUE(parse_root_path(root_path, &path).ok());
        ASSERT_STREQ(path1.c_str(), path.path.c_str());
        ASSERT_EQ(TStorageMedium::HDD, path.storage_medium);
    }
    {
        root_path = path2;
        ASSERT_TRUE(parse_root_path(root_path, &path).ok());
        ASSERT_STREQ(path2.c_str(), path.path.c_str());
        ASSERT_EQ(TStorageMedium::SSD, path.storage_medium);
    }

    // /path, <property>:<value>,...
    {
        root_path = path1 + ", medium: ssd";
        ASSERT_TRUE(parse_root_path(root_path, &path).ok());
        ASSERT_STREQ(path1.c_str(), path.path.c_str());
        ASSERT_EQ(TStorageMedium::SSD, path.storage_medium);
    }
    {
        root_path = path1 + ", medium: hdd";
        ASSERT_TRUE(parse_root_path(root_path, &path).ok());
        ASSERT_STREQ(path1.c_str(), path.path.c_str());
        ASSERT_EQ(TStorageMedium::HDD, path.storage_medium);
    }
    {
        root_path = path1 + ", medium: ssd, medium: hdd";
        ASSERT_TRUE(parse_root_path(root_path, &path).ok());
        ASSERT_STREQ(path1.c_str(), path.path.c_str());
        ASSERT_EQ(TStorageMedium::HDD, path.storage_medium);
    }
}

} // namespace starrocks
