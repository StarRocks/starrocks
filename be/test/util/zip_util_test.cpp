// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/util/zip_util_test.cpp

// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/util/zip_util_test.cpp

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

#include "util/zip_util.h"

#include <gtest/gtest.h>
#include <libgen.h>

#include <iostream>
#include <string>

#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gutil/strings/util.h"
#include "testutil/assert.h"
#include "util/logging.h"

namespace starrocks {

using namespace strings;

TEST(ZipUtilTest, basic) {
    const std::string path = "./be/test/util";

    fs::remove_all(path + "/test_data/target");

    ZipFile zf = ZipFile(path + "/test_data/zip_normal.zip");
    auto st = zf.extract(path + "/test_data", "target");
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(fs::path_exist(path + "/test_data/target/zip_normal_data"));
    ASSERT_FALSE(fs::is_directory(path + "/test_data/target/zip_normal_data").value());

    auto file = *FileSystem::Default()->new_random_access_file(path + "/test_data/target/zip_normal_data");

    char f[11];
    Slice slice(f, 11);
    ASSIGN_OR_ABORT(slice.size, file->read_at(0, slice.data, slice.size));

    ASSERT_EQ("hello world", slice.to_string());

    fs::remove_all(path + "/test_data/target");
}

TEST(ZipUtilTest, dir) {
    const std::string path = "./be/test/util";

    fs::remove_all(path + "/test_data/target");

    ZipFile zipFile = ZipFile(path + "/test_data/zip_dir.zip");
    ASSERT_TRUE(zipFile.extract(path + "/test_data", "target").ok());

    ASSERT_TRUE(fs::path_exist(path + "/test_data/target/zip_test/one"));
    ASSERT_TRUE(fs::is_directory(path + "/test_data/target/zip_test/one").value());

    ASSERT_TRUE(fs::path_exist(path + "/test_data/target/zip_test/one/data"));
    ASSERT_FALSE(fs::is_directory(path + "/test_data/target/zip_test/one/data").value());

    ASSERT_TRUE(fs::path_exist(path + "/test_data/target/zip_test/two"));
    ASSERT_TRUE(fs::is_directory(path + "/test_data/target/zip_test/two").value());

    auto file = *FileSystem::Default()->new_random_access_file(path + "/test_data/target/zip_test/one/data");

    char f[4];
    Slice slice(f, 4);
    file->read_at_fully(0, slice.data, slice.size);

    ASSERT_EQ("test", slice.to_string());

    fs::remove_all(path + "/test_data/target");
}

TEST(ZipUtilTest, targetAlready) {
    const std::string path = "./be/test/util";

    ZipFile f(path + "/test_data/zip_normal.zip");

    Status st = f.extract(path + "/test_data", "zip_test");
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(HasPrefixString(st.to_string(), "Already exist"));
}

TEST(ZipUtilTest, notzip) {
    const std::string path = "./be/test/util";

    ZipFile f(path + "/test_data/zip_normal_data");
    Status st = f.extract("test", "test");
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(HasPrefixString(st.to_string(), "Invalid argument"));
}

} // namespace starrocks
