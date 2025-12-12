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

#include "storage/lake/filenames.h"

#include <gtest/gtest.h>

#include "gutil/strings/util.h"
#include "util/string_parser.hpp"

namespace starrocks::lake {

class FilenamesTest : public testing::Test {
public:
    FilenamesTest() = default;
    ~FilenamesTest() override = default;
};

TEST_F(FilenamesTest, extract_uuid_from) {
    // Test valid segment file names
    {
        std::string file_name = "0000000000000003_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.dat";
        std::string uuid = extract_uuid_from(file_name);
        ASSERT_EQ("6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b", uuid);
    }

    // Test valid del file names
    {
        std::string file_name = "0000000000000003_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.del";
        std::string uuid = extract_uuid_from(file_name);
        ASSERT_EQ("6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b", uuid);
    }

    // Test valid sst file names
    {
        std::string file_name = "6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.sst";
        std::string uuid = extract_uuid_from(file_name);
        ASSERT_EQ("6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b", uuid);
    }

    // Test valid delvec file names
    {
        std::string file_name = "0000000000000003_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.delvec";
        std::string uuid = extract_uuid_from(file_name);
        ASSERT_EQ("6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b", uuid);
    }

    // Test valid cols file names
    {
        std::string file_name = "0000000000000003_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.cols";
        std::string uuid = extract_uuid_from(file_name);
        ASSERT_EQ("6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b", uuid);
    }

    // Test invalid file names - wrong extension
    {
        std::string file_name = "0000000000000003_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.txt";
        std::string uuid = extract_uuid_from(file_name);
        ASSERT_TRUE(uuid.empty());
    }

    // Test invalid file names - wrong position of underscore
    {
        std::string file_name = "00000001_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.dat";
        std::string uuid = extract_uuid_from(file_name);
        ASSERT_TRUE(uuid.empty());
    }

    // Test invalid file names - too short
    {
        std::string file_name = "6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.dat";
        std::string uuid = extract_uuid_from(file_name);
        ASSERT_TRUE(uuid.empty());
    }

    // Test invalid file names - empty string
    {
        std::string file_name = "";
        std::string uuid = extract_uuid_from(file_name);
        ASSERT_TRUE(uuid.empty());
    }
}

TEST_F(FilenamesTest, gen_segment_filename_from) {
    int64_t new_txn_id = 4;

    // Test valid segment file generation
    {
        std::string old_file_name = "0000000000000003_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.dat";
        std::string new_file_name = gen_filename_from(new_txn_id, old_file_name);
        ASSERT_EQ("0000000000000004_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.dat", new_file_name);
    }

    // Test valid del file input
    {
        std::string old_file_name = "0000000000000003_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.del";
        std::string new_file_name = gen_filename_from(new_txn_id, old_file_name);
        ASSERT_EQ("0000000000000004_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.del", new_file_name);
    }

    // Test valid sst file input (sst file only has uuid as name, no txn id)
    {
        std::string old_file_name = "6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.sst";
        std::string new_file_name = gen_filename_from(new_txn_id, old_file_name);
        // file name never changed
        ASSERT_EQ(old_file_name, new_file_name);
    }

    // Test valid delvec file input
    {
        std::string old_file_name = "0000000000000003_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.delvec";
        std::string new_file_name = gen_filename_from(new_txn_id, old_file_name);
        ASSERT_EQ("0000000000000004_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.delvec", new_file_name);
    }

    // Test valid cols file input
    {
        std::string old_file_name = "0000000000000003_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.cols";
        std::string new_file_name = gen_filename_from(new_txn_id, old_file_name);
        ASSERT_EQ("0000000000000004_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.cols", new_file_name);
    }

    // Test invalid old file name
    {
        std::string old_file_name = "invalid_filename.txt";
        std::string new_file_name = gen_filename_from(new_txn_id, old_file_name);
        ASSERT_TRUE(new_file_name.empty());
    }

    // Test empty old file name
    {
        std::string old_file_name = "";
        std::string new_file_name = gen_filename_from(new_txn_id, old_file_name);
        ASSERT_TRUE(new_file_name.empty());
    }

    // Test file name with correct UUID but wrong extension
    {
        std::string old_file_name = "0000000000000123_abcdef1234567890.log";
        std::string new_file_name = gen_filename_from(new_txn_id, old_file_name);
        ASSERT_TRUE(new_file_name.empty());
    }
}
} // namespace starrocks::lake
