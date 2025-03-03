// Copyright 2020-present StarRocks, Inc. All rights reserved.
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

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>

#include "cache/block_cache/block_cache.h"
#include "cache/block_cache/test_cache_utils.h"
#include "common/logging.h"
#include "common/statusor.h"
#include "fs/fs_util.h"
#include "testutil/scoped_updater.h"

namespace starrocks {

class MockFileSystem : public DiskSpace::FileSystemWrapper {
public:
    struct DiskSpaceInfo {
        dev_t device_id;
        SpaceInfo space_info;
    };

    StatusOr<SpaceInfo> space(const std::string& path) override {
        auto info = _disk_space_info(path);
        if (!info) {
            return Status::IOError("io error");
        }
        return info->space_info;
    }

    StatusOr<size_t> directory_size(const std::string& dir) override { return _dir_capacity; }

    dev_t device_id(const std::string& path) override {
        auto info = _disk_space_info(path);
        if (!info) {
            return -1;
        }
        return info->device_id;
    }

    void set_space(dev_t device_id, const std::string& disk_prefix, const SpaceInfo& space) {
        DiskSpaceInfo info;
        info.device_id = device_id;
        info.space_info = space;
        _space_infos[disk_prefix] = info;
    }

    void set_global_directory_capacity(size_t capacity) { _dir_capacity = capacity; }

private:
    DiskSpaceInfo* _disk_space_info(const std::string& path) {
        std::string disk_prefix = path;
        auto pos = disk_prefix.find("/");
        if (pos != std::string::npos) {
            disk_prefix = disk_prefix.substr(0, pos);
        }
        auto it = _space_infos.find(disk_prefix);
        if (it != _space_infos.end()) {
            return &(it->second);
        }
        return nullptr;
    }

    std::unordered_map<std::string, DiskSpaceInfo> _space_infos;
    size_t _dir_capacity = 0;
};

class DiskSpaceMonitorTest : public ::testing::Test {
public:
    static const size_t kBlockSize;

    static void SetUpTestCase() { ASSERT_TRUE(fs::create_directories("./block_disk_cache").ok()); }

    static void TearDownTestCase() { ASSERT_TRUE(fs::remove_all("./block_disk_cache").ok()); }

    void SetUp() override {}
    void TearDown() override {
        if (_cache) {
            _cache->shutdown();
        }
    }

private:
    BlockCache* _cache = nullptr;
};

const size_t DiskSpaceMonitorTest::kBlockSize = 256 * KB;

#ifdef WITH_STARCACHE

TEST_F(DiskSpaceMonitorTest, adjust_for_empty_cache_dir) {
    SCOPED_UPDATE(bool, config::datacache_auto_adjust_enable, true);
    SCOPED_UPDATE(int64_t, config::datacache_disk_safe_level, 70);
    SCOPED_UPDATE(int64_t, config::datacache_min_disk_quota_for_adjustment, 0);

    auto space_monitor = std::make_unique<DiskSpaceMonitor>(nullptr);
    MockFileSystem* mock_fs = new MockFileSystem;
    space_monitor->_fs.reset(mock_fs);

    SpaceInfo space_info = {.capacity = 1000 * GB, .free = 800 * GB, .available = 500 * GB};
    mock_fs->set_space(1, "disk1", space_info);
    mock_fs->set_space(2, "disk2", space_info);

    std::vector<DirSpace> dir_spaces = {
            {.path = "disk1/dir1", .size = 0}, {.path = "disk1/dir2", .size = 0}, {.path = "disk2/dir2", .size = 0}};

    // Set a default quota based on the `datacache_disk_safe_level`.
    // * disk usage: 1000G - 500G = 500G
    // * avail cache quota:  1000G * 70% - 500G = 200G
    // * dir_spaces in disk1: 100G / 2 = 100G
    // * dir_spaces in disk2: 200G
    ASSERT_TRUE(space_monitor->init(&dir_spaces).ok());
    ASSERT_EQ(dir_spaces[0].size, 100 * GB);
    ASSERT_EQ(dir_spaces[1].size, 100 * GB);
    ASSERT_EQ(dir_spaces[2].size, 200 * GB);
}

TEST_F(DiskSpaceMonitorTest, adjust_for_dirty_cache_dir) {
    SCOPED_UPDATE(bool, config::datacache_auto_adjust_enable, true);
    SCOPED_UPDATE(int64_t, config::datacache_min_disk_quota_for_adjustment, 0);
    SCOPED_UPDATE(int64_t, config::datacache_disk_high_level, 80);
    SCOPED_UPDATE(int64_t, config::datacache_disk_safe_level, 70);
    SCOPED_UPDATE(int64_t, config::datacache_disk_low_level, 60);

    auto space_monitor = std::make_unique<DiskSpaceMonitor>(nullptr);
    MockFileSystem* mock_fs = new MockFileSystem;
    space_monitor->_fs.reset(mock_fs);

    SpaceInfo space_info = {.capacity = 1000 * GB, .free = 800 * GB, .available = 200 * GB};
    mock_fs->set_space(1, "disk1", space_info);
    mock_fs->set_space(2, "disk2", space_info);
    mock_fs->set_global_directory_capacity(300 * GB);

    std::vector<DirSpace> dir_spaces = {
            {.path = "disk1/dir1", .size = 0}, {.path = "disk1/dir2", .size = 0}, {.path = "disk2/dir2", .size = 0}};

    // * disk1 usage except cache: 1000G - 200G - 300G * 2 = 200G
    // * disk2 usage except cache: 1000G - 200G - 300G = 500G
    // * disk1 avail cache quota:  1000G * 70% - 200G = 500G
    // * disk2 avail cache quota:  1000G * 70% - 500G = 200G
    // * dir_spaces in disk1: 500G / 2 = 250G
    // * dir_spaces in disk2: 200G
    ASSERT_TRUE(space_monitor->init(&dir_spaces).ok());
    ASSERT_EQ(dir_spaces[0].size, 250 * GB);
    ASSERT_EQ(dir_spaces[1].size, 250 * GB);
    ASSERT_EQ(dir_spaces[2].size, 200 * GB);
}

TEST_F(DiskSpaceMonitorTest, auto_increase_cache_quota) {
    SCOPED_UPDATE(bool, config::datacache_enable, true);
    SCOPED_UPDATE(bool, config::datacache_auto_adjust_enable, false);
    SCOPED_UPDATE(int64_t, config::datacache_min_disk_quota_for_adjustment, 0);
    SCOPED_UPDATE(int64_t, config::datacache_disk_adjust_interval_seconds, 1);
    SCOPED_UPDATE(int64_t, config::datacache_disk_idle_seconds_for_expansion, 300);
    SCOPED_UPDATE(int64_t, config::datacache_disk_high_level, 80);
    SCOPED_UPDATE(int64_t, config::datacache_disk_safe_level, 70);
    SCOPED_UPDATE(int64_t, config::datacache_disk_low_level, 60);

    auto options = create_simple_options(kBlockSize, 0, 20 * MB);
    auto cache = create_cache(options);

    MockFileSystem* mock_fs = new MockFileSystem;
    SpaceInfo space_info = {.capacity = 500 * MB, .free = 400 * MB, .available = 300 * MB};
    mock_fs->set_space(1, ".", space_info);

    auto& space_monitor = cache->_disk_space_monitor;
    space_monitor->_fs.reset(mock_fs);
    space_monitor->init(&options.disk_spaces);

    // Fill cache data
    {
        size_t batch_size = MB;
        const std::string cache_key = "test_file";
        for (size_t i = 0; i < 20; ++i) {
            char ch = 'a' + i % 26;
            std::string value(batch_size, ch);
            Status st = cache->write_buffer(cache_key + std::to_string(i), 0, batch_size, value.c_str());
            ASSERT_TRUE(st.ok()) << st.message();
        }
        auto metrics = cache->cache_metrics();
        int64_t used_rate = metrics.disk_used_bytes * 100 / metrics.disk_quota_bytes;
        ASSERT_GT(used_rate, DiskSpace::kAutoIncreaseThreshold);
    }

    {
        auto metrics = cache->cache_metrics();
        ASSERT_EQ(metrics.disk_quota_bytes, 20 * MB);
    }

    {
        config::datacache_auto_adjust_enable = true;
        sleep(3);
        auto metrics = cache->cache_metrics();
        ASSERT_EQ(metrics.disk_quota_bytes, 20 * MB);
    }

    {
        config::datacache_disk_idle_seconds_for_expansion = 1;
        sleep(3);
        auto metrics = cache->cache_metrics();
        // other: 500M - 300M - 20M = 180M
        // new quota: 500 * 0.7 - other = 170M
        ASSERT_EQ(metrics.disk_quota_bytes, 170 * MB);
    }

    cache->shutdown();
}

TEST_F(DiskSpaceMonitorTest, auto_decrease_cache_quota) {
    SCOPED_UPDATE(bool, config::datacache_enable, true);
    SCOPED_UPDATE(bool, config::datacache_auto_adjust_enable, false);
    SCOPED_UPDATE(int64_t, config::datacache_min_disk_quota_for_adjustment, 0);
    SCOPED_UPDATE(int64_t, config::datacache_disk_adjust_interval_seconds, 3);
    SCOPED_UPDATE(int64_t, config::datacache_disk_idle_seconds_for_expansion, 300);
    SCOPED_UPDATE(int64_t, config::datacache_disk_high_level, 80);
    SCOPED_UPDATE(int64_t, config::datacache_disk_safe_level, 70);
    SCOPED_UPDATE(int64_t, config::datacache_disk_low_level, 60);

    auto options = create_simple_options(kBlockSize, 0, 50 * MB);
    auto cache = create_cache(options);

    MockFileSystem* mock_fs = new MockFileSystem;
    SpaceInfo space_info = {.capacity = 100 * MB, .free = 20 * MB, .available = 10 * MB};
    mock_fs->set_space(1, ".", space_info);

    auto& space_monitor = cache->_disk_space_monitor;
    space_monitor->_fs.reset(mock_fs);
    space_monitor->init(&options.disk_spaces);

    // Fill cache data
    {
        size_t batch_size = MB;
        const std::string cache_key = "test_file";
        for (size_t i = 0; i < 50; ++i) {
            char ch = 'a' + i % 26;
            std::string value(batch_size, ch);
            Status st = cache->write_buffer(cache_key + std::to_string(i), 0, batch_size, value.c_str());
            ASSERT_TRUE(st.ok());
        }
        auto metrics = cache->cache_metrics();
        int64_t used_rate = metrics.disk_used_bytes * 100 / metrics.disk_quota_bytes;
        ASSERT_GT(used_rate, DiskSpace::kAutoIncreaseThreshold);
    }

    {
        auto metrics = cache->cache_metrics();
        ASSERT_EQ(metrics.disk_quota_bytes, 50 * MB);
    }

    {
        config::datacache_auto_adjust_enable = true;
        size_t new_quota = 0;
        for (int i = 0; i < 6; ++i) {
            auto metrics = cache->cache_metrics();
            if (metrics.disk_quota_bytes > 0 && metrics.disk_quota_bytes != 50 * MB) {
                config::datacache_auto_adjust_enable = false;
                new_quota = metrics.disk_quota_bytes;
                break;
            }
            sleep(1);
        }
        // other: 100M - 10M - 50M = 40M
        // new quota: 100 * 0.7 - other = 30M
        ASSERT_EQ(new_quota, 30 * MB);
    }

    cache->shutdown();
}

TEST_F(DiskSpaceMonitorTest, auto_decrease_cache_quota_to_zero) {
    SCOPED_UPDATE(bool, config::datacache_enable, true);
    SCOPED_UPDATE(bool, config::datacache_auto_adjust_enable, false);
    SCOPED_UPDATE(int64_t, config::datacache_min_disk_quota_for_adjustment, 40 * MB);
    SCOPED_UPDATE(int64_t, config::datacache_disk_adjust_interval_seconds, 2);
    SCOPED_UPDATE(int64_t, config::datacache_disk_idle_seconds_for_expansion, 300);
    SCOPED_UPDATE(int64_t, config::datacache_disk_high_level, 80);
    SCOPED_UPDATE(int64_t, config::datacache_disk_safe_level, 70);
    SCOPED_UPDATE(int64_t, config::datacache_disk_low_level, 60);

    auto options = create_simple_options(kBlockSize, 0, 50 * MB);
    auto cache = create_cache(options);

    MockFileSystem* mock_fs = new MockFileSystem;
    SpaceInfo space_info = {.capacity = 100 * MB, .free = 20 * MB, .available = 10 * MB};
    mock_fs->set_space(1, ".", space_info);

    auto& space_monitor = cache->_disk_space_monitor;
    space_monitor->_fs.reset(mock_fs);
    space_monitor->init(&options.disk_spaces);

    // Fill cache data
    {
        size_t batch_size = MB;
        const std::string cache_key = "test_file";
        for (size_t i = 0; i < 50; ++i) {
            char ch = 'a' + i % 26;
            std::string value(batch_size, ch);
            Status st = cache->write_buffer(cache_key + std::to_string(i), 0, batch_size, value.c_str());
            ASSERT_TRUE(st.ok());
        }
        auto metrics = cache->cache_metrics();
        int64_t used_rate = metrics.disk_used_bytes * 100 / metrics.disk_quota_bytes;
        ASSERT_GT(used_rate, DiskSpace::kAutoIncreaseThreshold);
    }

    {
        auto metrics = cache->cache_metrics();
        ASSERT_EQ(metrics.disk_quota_bytes, 50 * MB);
    }

    {
        config::datacache_auto_adjust_enable = true;
        size_t new_quota = 0;
        for (int i = 0; i < 6; ++i) {
            auto metrics = cache->cache_metrics();
            if (metrics.disk_quota_bytes > 0 && metrics.disk_quota_bytes != 50 * MB) {
                config::datacache_auto_adjust_enable = false;
                new_quota = metrics.disk_quota_bytes;
                break;
            }
            sleep(1);
        }
        // other: 100M - 10M - 50M = 40M
        // new quota: 100 * 0.7 - other = 30M < 40M = 0
        ASSERT_EQ(new_quota, 0);
        sleep(3);
        // Adjust to zero again
        ASSERT_EQ(new_quota, 0);
    }

    cache->shutdown();
}

TEST_F(DiskSpaceMonitorTest, get_directory_capacity) {
    SCOPED_UPDATE(bool, config::datacache_enable, true);
    SCOPED_UPDATE(bool, config::datacache_auto_adjust_enable, false);

    auto options = create_simple_options(kBlockSize, 0, 20 * MB);
    auto cache = create_cache(options);

    // Fill cache data
    {
        size_t batch_size = MB;
        const std::string cache_key = "test_file";
        for (size_t i = 0; i < 20; ++i) {
            char ch = 'a' + i % 26;
            std::string value(batch_size, ch);
            Status st = cache->write_buffer(cache_key + std::to_string(i), 0, batch_size, value.c_str());
            ASSERT_TRUE(st.ok());
        }

        auto& disk_spaces = options.disk_spaces;
        auto& space_monitor = cache->_disk_space_monitor;
        size_t capacity = 0;
        for (auto& space : disk_spaces) {
            auto ret = space_monitor->_fs->directory_size(space.path);
            ASSERT_TRUE(ret.ok());
            capacity += ret.value();
        }
        ASSERT_EQ(capacity, 20 * MB);
    }
}

#endif

} // namespace starrocks
