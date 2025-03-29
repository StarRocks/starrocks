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

#include "cache/block_cache/datacache_utils.h"

#include <gtest/gtest.h>

#include <filesystem>

#include "fs/fs_util.h"
#include "gen_cpp/DataCache_types.h"

namespace starrocks {
class DataCacheUtilsTest : public ::testing::Test {};

TEST_F(DataCacheUtilsTest, test_set_metrics_from_thrift) {
    TDataCacheMetrics t_metrics{};
    DataCacheMetrics metrics{};
    metrics.status = DataCacheStatus::NORMAL;
    DataCacheUtils::set_metrics_from_thrift(t_metrics, metrics);
    ASSERT_EQ(t_metrics.status, TDataCacheStatus::NORMAL);

    metrics.status = DataCacheStatus::UPDATING;
    DataCacheUtils::set_metrics_from_thrift(t_metrics, metrics);
    ASSERT_EQ(t_metrics.status, TDataCacheStatus::UPDATING);

    metrics.status = DataCacheStatus::LOADING;
    DataCacheUtils::set_metrics_from_thrift(t_metrics, metrics);
    ASSERT_EQ(t_metrics.status, TDataCacheStatus::LOADING);

    metrics.status = DataCacheStatus::ABNORMAL;
    DataCacheUtils::set_metrics_from_thrift(t_metrics, metrics);
    ASSERT_EQ(t_metrics.status, TDataCacheStatus::ABNORMAL);
}

TEST_F(DataCacheUtilsTest, test_mem_size_invalid_parse) {
    size_t parsed_mem_size = 0;
    Status st = DataCacheUtils::parse_conf_datacache_mem_size("12g", 1024, &parsed_mem_size);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(1024, parsed_mem_size);
    st = DataCacheUtils::parse_conf_datacache_mem_size("12gb", 1024, &parsed_mem_size);
    ASSERT_FALSE(st.ok());
}

TEST_F(DataCacheUtilsTest, parse_cache_space_size_str) {
    const std::string cache_dir = "./block_disk_cache1";
    ASSERT_TRUE(fs::create_directories(cache_dir).ok());

    size_t parsed_size = 0;
    uint64_t mem_size = 10;
    ASSERT_TRUE(DataCacheUtils::parse_conf_datacache_mem_size("10", 0, &parsed_size).ok());
    ASSERT_EQ(mem_size, parsed_size);
    mem_size *= 1024;
    ASSERT_TRUE(DataCacheUtils::parse_conf_datacache_mem_size("10K", 0, &parsed_size).ok());
    ASSERT_EQ(mem_size, parsed_size);
    mem_size *= 1024;
    ASSERT_TRUE(DataCacheUtils::parse_conf_datacache_mem_size("10M", 0, &parsed_size).ok());
    ASSERT_EQ(mem_size, parsed_size);
    mem_size *= 1024;
    ASSERT_TRUE(DataCacheUtils::parse_conf_datacache_mem_size("10G", 0, &parsed_size).ok());
    ASSERT_EQ(mem_size, parsed_size);
    mem_size *= 1024;
    ASSERT_TRUE(DataCacheUtils::parse_conf_datacache_mem_size("10T", 0, &parsed_size).ok());
    ASSERT_EQ(mem_size, parsed_size);
    ASSERT_TRUE(DataCacheUtils::parse_conf_datacache_mem_size("10%", 10 * 1024, &parsed_size).ok());
    ASSERT_EQ(1024, parsed_size);

    std::string disk_path = cache_dir;
    const int64_t kMaxLimit = 20L * 1024 * 1024 * 1024 * 1024; // 20T
    int64_t disk_size = 10;
    ASSERT_EQ(DataCacheUtils::parse_conf_datacache_disk_size(disk_path, "10", kMaxLimit), disk_size);
    disk_size *= 1024;
    ASSERT_EQ(DataCacheUtils::parse_conf_datacache_disk_size(disk_path, "10K", kMaxLimit), disk_size);
    disk_size *= 1024;
    ASSERT_EQ(DataCacheUtils::parse_conf_datacache_disk_size(disk_path, "10M", kMaxLimit), disk_size);
    disk_size *= 1024;
    ASSERT_EQ(DataCacheUtils::parse_conf_datacache_disk_size(disk_path, "10G", kMaxLimit), disk_size);
    disk_size *= 1024;
    ASSERT_EQ(DataCacheUtils::parse_conf_datacache_disk_size(disk_path, "10T", kMaxLimit), disk_size);

    // The disk size exceed disk limit
    ASSERT_EQ(DataCacheUtils::parse_conf_datacache_disk_size(disk_path, "10T", 1024), 1024);

    disk_size = DataCacheUtils::parse_conf_datacache_disk_size(disk_path, "10%", kMaxLimit);
    ASSERT_EQ(disk_size, int64_t(10.0 / 100.0 * kMaxLimit));

    fs::remove_all(cache_dir).ok();
}

TEST_F(DataCacheUtilsTest, parse_cache_space_paths) {
    const std::string cache_dir = "./block_disk_cache2";
    ASSERT_TRUE(fs::create_directories(cache_dir).ok());

    const std::string cwd = std::filesystem::current_path().string();
    const std::string s_normal_path = fmt::format("{}/block_disk_cache2/cache1;{}/block_disk_cache2/cache2", cwd, cwd);
    std::vector<std::string> paths;
    ASSERT_TRUE(DataCacheUtils::parse_conf_datacache_disk_paths(s_normal_path, &paths, true).ok());
    ASSERT_EQ(paths.size(), 2);

    paths.clear();
    const std::string s_space_path =
            fmt::format(" {}/block_disk_cache2/cache3 ; {}/block_disk_cache2/cache4 ", cwd, cwd);
    ASSERT_TRUE(DataCacheUtils::parse_conf_datacache_disk_paths(s_space_path, &paths, true).ok());
    ASSERT_EQ(paths.size(), 2);

    paths.clear();
    const std::string s_empty_path = fmt::format("//;{}/block_disk_cache2/cache4 ", cwd);
    ASSERT_FALSE(DataCacheUtils::parse_conf_datacache_disk_paths(s_empty_path, &paths, true).ok());
    ASSERT_EQ(paths.size(), 1);

    paths.clear();
    const std::string s_invalid_path = fmt::format(" /block_disk_cache2/cache5;{}/+/cache6", cwd);
    ASSERT_FALSE(DataCacheUtils::parse_conf_datacache_disk_paths(s_invalid_path, &paths, true).ok());
    ASSERT_EQ(paths.size(), 0);

    paths.clear();
    const std::string s_duplicated_path =
            fmt::format(" {}/block_disk_cache2/cache7 ; {}/block_disk_cache2/cache7 ", cwd, cwd);
    ASSERT_TRUE(DataCacheUtils::parse_conf_datacache_disk_paths(s_duplicated_path, &paths, true).ok());
    ASSERT_EQ(paths.size(), 1);

    fs::remove_all(cache_dir).ok();
}

TEST_F(DataCacheUtilsTest, change_cache_path_suc) {
    const std::string old_dir = "./old_disk_cache_path";
    const std::string new_dir = "./new_disk_cache_path";
    ASSERT_TRUE(fs::create_directories(old_dir).ok());
    ASSERT_TRUE(fs::create_directories(new_dir).ok());

    ASSERT_TRUE(DataCacheUtils::change_disk_path(old_dir, new_dir).ok());

    fs::remove_all(old_dir);
    fs::remove_all(new_dir);
}

TEST_F(DataCacheUtilsTest, change_cache_path_fail) {
    const std::string old_dir = "./old_disk_cache_path2";
    const std::string new_dir = "./old_disk_cache_path2/subdir";
    ASSERT_TRUE(fs::create_directories(old_dir).ok());
    ASSERT_TRUE(fs::create_directories(new_dir).ok());

    ASSERT_FALSE(DataCacheUtils::change_disk_path(old_dir, new_dir).ok());

    fs::remove_all(old_dir);
}

} // namespace starrocks
