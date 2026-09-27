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
#include <sys/stat.h>
#ifdef __linux__
#include <sys/sysmacros.h>
#endif

#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <set>
#include <vector>

#include "common/logging.h"
#include "common/system/disk_info.h"

namespace starrocks {

extern const char* k_ut_partitions_path;
extern const char* k_ut_mounts_path;

// Paths to the per-test-suite temporary mock files.
static std::string s_tmp_partitions_path;
static std::string s_tmp_mounts_path;

// Writes a /proc/partitions-format file that registers "dev_abcd" with the
// major:minor matching stat("/tmp").  Returns false on failure.
static bool write_mock_partitions(const std::string& path) {
    struct stat st {};
    if (::stat("/tmp", &st) != 0) {
        return false;
    }
#ifdef __linux__
    unsigned int maj = gnu_dev_major(st.st_dev);
    unsigned int min = gnu_dev_minor(st.st_dev);
#else
    // macOS: high 12 bits = major, low 20 bits = minor (same as the macro above)
    unsigned int maj = static_cast<unsigned int>(st.st_dev >> 20) & 0xFFFU;
    unsigned int min = static_cast<unsigned int>(st.st_dev) & 0xFFFFFU;
#endif
    std::ofstream f(path);
    if (!f.is_open()) {
        return false;
    }
    // header line expected by get_device_names (skipped because "name" != 4 fields)
    f << "major minor  #blocks  name\n\n";
    f << maj << " " << min << " 1048576 dev_abcd\n";
    return true;
}

class DiskInfoTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        s_tmp_partitions_path = "/tmp/ut_partitions_" + std::to_string(::getpid());
        s_tmp_mounts_path = "/tmp/ut_mounts_" + std::to_string(::getpid());

        ASSERT_TRUE(write_mock_partitions(s_tmp_partitions_path)) << "Failed to write mock partitions file";

        // Re-initialise DiskInfo using the mock partitions so that _s_disks
        // contains "dev_abcd" mapped to the real dev_t of /tmp.
        k_ut_partitions_path = s_tmp_partitions_path.c_str();
        DiskInfo::init();
    }

    static void TearDownTestSuite() {
        ::remove(s_tmp_mounts_path.c_str());
        ::remove(s_tmp_partitions_path.c_str());
        k_ut_partitions_path = nullptr;
    }

    void TearDown() override { k_ut_mounts_path = nullptr; }
};

// ── Test 1 ────────────────────────────────────────────────────────────────
// Empty input: no paths given; devices should remain empty and status OK.
TEST_F(DiskInfoTest, GetDiskDevices_EmptyPaths) {
    std::vector<std::string> paths;
    std::set<std::string> devices;

    auto st = DiskInfo::get_disk_devices(paths, &devices);

    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(devices.empty());
}

// ── Test 2 ────────────────────────────────────────────────────────────────
// Verify a path that is covered by the mock mounts resolves to a non-empty device set.
TEST_F(DiskInfoTest, GetDiskDevices_MockedPath) {
    std::ofstream f(s_tmp_mounts_path);
    ASSERT_TRUE(f.is_open());
    f << "/dev/dev_abcd /tmp ext4 rw,relatime 0 0\n";
    f.close();
    k_ut_mounts_path = s_tmp_mounts_path.c_str();
    std::vector<std::string> paths = {"/tmp"};
    std::set<std::string> devices;

    auto st = DiskInfo::get_disk_devices(paths, &devices);

    ASSERT_TRUE(st.ok()) << st.message();
    ASSERT_FALSE(devices.empty()) << "/tmp should map to 'dev_abcd' as normal device";
    ASSERT_EQ("dev_abcd", *devices.begin());
}

// ── Test 3 ────────────────────────────────────────────────────────────────
// LVM fallback path: the mock mounts file maps /dev/mapper/fake-vg-fake-lv
// to /tmp.  dev "fake-vg-fake-lv" is not in _s_disk_name_to_disk_id,
// so the else-fallback fires: stat("/tmp") returns the real `dev_t` whose major and minor were
// pre-registered as "dev_abcd" in the mock partitions file.
TEST_F(DiskInfoTest, GetDiskDevices_LvmFallback) {
    // Writes a /proc/mounts-format file that maps /dev/mapper/fake-vg-fake-lv to
    // /tmp (LVM-style, that is not in _s_disk_name_to_disk_id)
    std::ofstream f(s_tmp_mounts_path);
    ASSERT_TRUE(f.is_open());
    f << "/dev/mapper/fake-vg-fake-lv /tmp ext4 rw,relatime 0 0\n";
    f.close();
    k_ut_mounts_path = s_tmp_mounts_path.c_str();

    std::vector<std::string> paths = {"/tmp"};
    std::set<std::string> devices;

    auto st = DiskInfo::get_disk_devices(paths, &devices);

    ASSERT_TRUE(st.ok()) << st.message();
    ASSERT_FALSE(devices.empty()) << "LVM fallback should resolve /tmp to 'dev_abcd'";
    ASSERT_EQ("dev_abcd", *devices.begin());
}

// ── Test 4 ────────────────────────────────────────────────────────────────
// fopen failure to open mount path, get_disk_devices must return InternalError.
TEST_F(DiskInfoTest, GetDiskDevices_MountsOpenFailed) {
    k_ut_mounts_path = "/tmp/this_file_does_not_exist_ut_disk_info";

    std::vector<std::string> paths = {"/tmp"};
    std::set<std::string> devices;

    auto st = DiskInfo::get_disk_devices(paths, &devices);

    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_internal_error()) << "expected InternalError, got: " << st.message();
    ASSERT_TRUE(devices.empty());
}

} // namespace starrocks
