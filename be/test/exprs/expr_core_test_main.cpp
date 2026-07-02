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
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

#include "common/configbase.h"
#include "common/system/cpu_info.h"
#include "types/time_types.h"

namespace {

bool init_config() {
    const auto test_dir =
            std::filesystem::temp_directory_path() / ("starrocks_expr_core_test_" + std::to_string(::getpid()));
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    if (std::getenv("STARROCKS_HOME") == nullptr) {
        ::setenv("STARROCKS_HOME", test_dir.c_str(), 1);
    }
    if (std::getenv("UDF_RUNTIME_DIR") == nullptr) {
        const auto udf_runtime_dir = test_dir / "udf_runtime";
        ::setenv("UDF_RUNTIME_DIR", udf_runtime_dir.c_str(), 1);
    }

    const auto conf_path = test_dir / "be_test.conf";
    std::ofstream conf(conf_path);
    if (!conf.is_open()) {
        return false;
    }

    conf << "local_library_dir = " << (test_dir / "local_library_dir").string() << "\n"
         << "storage_root_path = " << (test_dir / "storage").string() << "\n"
         << "plugin_path = " << (test_dir / "plugin").string() << "\n"
         << "query_scratch_dirs = " << (test_dir / "query_scratch").string() << "\n"
         << "pprof_profile_dir = " << (test_dir / "log").string() << "\n"
         << "flamegraph_tool_dir = " << (test_dir / "bin" / "flamegraph").string() << "\n"
         << "small_file_dir = " << (test_dir / "small_file").string() << "\n"
         << "pull_load_task_dir = " << (test_dir / "pull_load").string() << "\n"
         << "sys_log_dir = " << (test_dir / "log").string() << "\n"
         << "pipeline_gcore_output_dir = " << (test_dir / "log").string() << "\n"
         << "spill_local_storage_dir = " << (test_dir / "spill").string() << "\n"
         << "user_function_dir = " << (test_dir / "udf").string() << "\n"
         << "enable_system_metrics = false\n"
         << "enable_metric_calculator = false\n"
         << "mem_limit = 5G\n";
#ifdef __x86_64__
    conf << "sys_minidump_dir = " << (test_dir / "minidump").string() << "\n";
#endif
    conf.close();

    return starrocks::config::init(conf_path.c_str());
}

} // namespace

int main(int argc, char** argv) {
    if (!init_config()) {
        return 1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    starrocks::CpuInfo::init();
    starrocks::date::init_date_cache();
    return RUN_ALL_TESTS();
}
