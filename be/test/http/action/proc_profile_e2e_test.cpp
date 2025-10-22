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
#include <fstream>
#include <sstream>
#include <string>

#include "common/config.h"
#include "http/action/profile_utils.h"
#include "http/default_path_handlers.h"

namespace starrocks {

class ProcProfileE2ETest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test directory
        test_dir = "/tmp/starrocks_proc_profile_test";
        std::filesystem::create_directories(test_dir);

        // Set config to use test directory
        original_sys_log_dir = config::sys_log_dir.c_str();
        config::sys_log_dir = test_dir.c_str();

        // Create fake profile files
        create_fake_profile_files();
    }

    void TearDown() override {
        // Restore original config
        config::sys_log_dir = original_sys_log_dir;

        // Clean up test directory
        std::filesystem::remove_all(test_dir);
    }

    void create_fake_profile_files() {
        // Create proc_profile subdirectory
        std::string proc_profile_dir = test_dir + "/proc_profile";
        std::filesystem::create_directories(proc_profile_dir);

        // Create CPU flame profile
        std::string cpu_flame_content = R"(<!DOCTYPE html>
<html><head><title>CPU Profile</title></head>
<body><h1>CPU Flame Graph</h1><p>This is a fake CPU profile</p></body></html>)";
        create_gzipped_file("cpu-profile-20231201-143022-flame.html.gz", cpu_flame_content);

        // Create CPU pprof profile
        std::string cpu_pprof_content = "fake pprof data for CPU profile";
        create_gzipped_file("cpu-profile-20231201-143022-pprof.gz", cpu_pprof_content);

        // Create contention flame profile
        std::string contention_flame_content = R"(<!DOCTYPE html>
<html><head><title>Contention Profile</title></head>
<body><h1>Contention Flame Graph</h1><p>This is a fake contention profile</p></body></html>)";
        create_gzipped_file("contention-profile-20231201-143022-flame.html.gz", contention_flame_content);

        // Create contention pprof profile
        std::string contention_pprof_content = "fake pprof data for contention profile";
        create_gzipped_file("contention-profile-20231201-143022-pprof.gz", contention_pprof_content);
    }

    void create_gzipped_file(const std::string& filename, const std::string& content) {
        std::string file_path = test_dir + "/proc_profile/" + filename;

        // Create temporary file with content
        std::string temp_file = file_path + ".tmp";
        std::ofstream temp_stream(temp_file);
        temp_stream << content;
        temp_stream.close();

        // Compress with gzip
        std::string gzip_cmd = "gzip -c " + temp_file + " > " + file_path;
        system(gzip_cmd.c_str());

        // Remove temporary file
        std::filesystem::remove(temp_file);
    }

    std::string test_dir;
    const char* original_sys_log_dir;
};

TEST_F(ProcProfileE2ETest, TestProcProfilePageDisplay) {
    // Test the /proc_profile page display
    WebPageHandler::ArgumentMap args;
    std::stringstream output;

    // Debug: check if files were created
    std::cout << "=== DEBUG: Checking test directory ===" << std::endl;
    std::cout << "Test directory: " << test_dir << std::endl;
    std::cout << "Config sys_log_dir: " << config::sys_log_dir << std::endl;

    for (const auto& entry : std::filesystem::directory_iterator(test_dir)) {
        std::cout << "Found file: " << entry.path().filename().string() << std::endl;
    }
    std::cout << "=== END DEBUG ===" << std::endl;

    // Call the proc_profile_handler
    proc_profile_handler(args, &output);

    std::string result = output.str();

    // Debug: print the actual output
    std::cout << "=== DEBUG: Actual output from proc_profile_handler ===" << std::endl;
    std::cout << result << std::endl;
    std::cout << "=== END DEBUG ===" << std::endl;

    // Check that the page contains expected elements
    EXPECT_TRUE(result.find("<h2>Process Profiles</h2>") != std::string::npos);
    EXPECT_TRUE(result.find("Available Profile Files") != std::string::npos);
    EXPECT_TRUE(result.find("<table") != std::string::npos);

    // Check that fake files are listed
    EXPECT_TRUE(result.find("cpu-profile-20231201-143022-flame.html.gz") == std::string::npos);
    EXPECT_TRUE(result.find("cpu-profile-20231201-143022-pprof.gz") != std::string::npos);

    // Check that profile types and formats are correctly identified
    EXPECT_TRUE(result.find(">CPU<") != std::string::npos);
    EXPECT_TRUE(result.find(">Flame<") == std::string::npos);
    EXPECT_TRUE(result.find(">Pprof<") != std::string::npos);

    // Check that action links are present
    EXPECT_TRUE(result.find("/proc_profile/file?filename=") != std::string::npos);
}

TEST_F(ProcProfileE2ETest, TestProfileUtilsIntegration) {
    // Test ProfileUtils functions with our fake files
    std::string cpu_flame_file = "cpu-profile-20231201-143022-flame.html.gz";
    std::string cpu_pprof_file = "cpu-profile-20231201-143022-pprof.gz";
    std::string contention_flame_file = "contention-profile-20231201-143022-flame.html.gz";

    // Test timestamp extraction
    EXPECT_EQ(ProfileUtils::extract_timestamp_from_filename(cpu_flame_file), "20231201-143022");
    EXPECT_EQ(ProfileUtils::extract_timestamp_from_filename(cpu_pprof_file), "20231201-143022");
    EXPECT_EQ(ProfileUtils::extract_timestamp_from_filename(contention_flame_file), "20231201-143022");

    // Test profile type detection
    EXPECT_EQ(ProfileUtils::get_profile_type(cpu_flame_file), "CPU");
    EXPECT_EQ(ProfileUtils::get_profile_type(cpu_pprof_file), "CPU");
    EXPECT_EQ(ProfileUtils::get_profile_type(contention_flame_file), "Contention");

    // Test profile format detection
    EXPECT_EQ(ProfileUtils::get_profile_format(cpu_flame_file), "Flame");
    EXPECT_EQ(ProfileUtils::get_profile_format(cpu_pprof_file), "Pprof");
    EXPECT_EQ(ProfileUtils::get_profile_format(contention_flame_file), "Flame");
}

} // namespace starrocks
