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

#include "compute_env/load_path/load_path_mgr.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>

#include "base/testutil/assert.h"
#include "compute_env/load_path/dummy_load_path_mgr.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

namespace {

class TempDir {
public:
    TempDir() {
        static std::atomic<int> next_id{0};
        std::error_code ec;
        _path = std::filesystem::temp_directory_path(ec) / ("starrocks_load_path_mgr_test_" + std::to_string(getpid()) +
                                                            "_" + std::to_string(next_id.fetch_add(1)));
        std::filesystem::create_directories(_path, ec);
    }

    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(_path, ec);
    }

    std::string path() const { return _path.string(); }

private:
    std::filesystem::path _path;
};

} // namespace

TEST(LoadPathMgrTest, RealManagerUsesStoreRootsForLoadErrorAndRejectedRecordPaths) {
    TempDir root1;
    TempDir root2;
    LoadPathMgr mgr({root1.path(), root2.path()});

    ASSERT_OK(mgr.init());

    std::vector<std::string> load_paths;
    mgr.get_load_data_path(&load_paths);
    ASSERT_EQ(2, load_paths.size());
    EXPECT_EQ(root1.path() + "/mini_download", load_paths[0]);
    EXPECT_EQ(root2.path() + "/mini_download", load_paths[1]);
    EXPECT_TRUE(std::filesystem::exists(root1.path() + "/error_log"));

    std::string first_prefix;
    ASSERT_OK(mgr.allocate_dir("db1", "label1", &first_prefix));
    EXPECT_EQ(root1.path() + "/mini_download/db1/__shard_0/label1", first_prefix);
    EXPECT_TRUE(std::filesystem::exists(first_prefix));

    std::string second_prefix;
    ASSERT_OK(mgr.allocate_dir("db2", "label2", &second_prefix));
    EXPECT_EQ(root2.path() + "/mini_download/db2/__shard_1/label2", second_prefix);
    EXPECT_TRUE(std::filesystem::exists(second_prefix));

    TUniqueId fragment_instance_id;
    fragment_instance_id.hi = 0xabc;
    fragment_instance_id.lo = 0xdef;

    std::string error_file;
    ASSERT_OK(mgr.get_load_error_file_name(fragment_instance_id, &error_file));
    EXPECT_EQ("error_log_abc_def", error_file);
    EXPECT_EQ(root1.path() + "/error_log/" + error_file, mgr.get_load_error_absolute_path(error_file));

    EXPECT_EQ(root1.path() + "/rejected_record/db1/label1/42/abc_def",
              mgr.get_load_rejected_record_absolute_path("", "db1", "label1", 42, fragment_instance_id));
    EXPECT_EQ(
            "/custom/rejected/db1/label1/42/abc_def",
            mgr.get_load_rejected_record_absolute_path("/custom/rejected", "db1", "label1", 42, fragment_instance_id));
}

TEST(LoadPathMgrTest, DummyManagerKeepsEmptyNoStorePathBehavior) {
    DummyLoadPathMgr mgr;
    ASSERT_OK(mgr.init());

    std::vector<std::string> load_paths;
    mgr.get_load_data_path(&load_paths);
    EXPECT_TRUE(load_paths.empty());

    std::string prefix;
    EXPECT_FALSE(mgr.allocate_dir("db", "label", &prefix).ok());

    TUniqueId fragment_instance_id;
    fragment_instance_id.hi = 0xabc;
    fragment_instance_id.lo = 0xdef;

    std::string error_file = "unchanged";
    ASSERT_OK(mgr.get_load_error_file_name(fragment_instance_id, &error_file));
    EXPECT_TRUE(error_file.empty());
    EXPECT_TRUE(mgr.get_load_error_absolute_path("error_log").empty());
    EXPECT_TRUE(mgr.get_load_rejected_record_absolute_path("", "db", "label", 1, fragment_instance_id).empty());
}

} // namespace starrocks
