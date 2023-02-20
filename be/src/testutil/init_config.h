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

#include <filesystem>
#include <fstream>

#include "common/config.h"
#include "common/status.h"

namespace starrocks {

[[nodiscard]] static bool init_config_for_test() {
    std::ofstream conf("test.conf");
    // following configs are based on environment variables and if those environment not setted
    // config::init would failed.
    conf << "local_library_dir = test_dir/local_library_dir\n"
         << "storage_root_path = test_dir/benchmark_store\n"
         << "plugin_path = test_dir/plugin_path\n"
         << "query_scratch_dirs = test_dir/query_scratch_dirs\n"
         << "pprof_profile_dir = test_dir/pprof_profile_dir\n"
         << "small_file_dir = test_dir/small_file_dir\n"
         << "pull_load_task_dir = test_dir/pull_load_task_dir\n"
         << "sys_log_dir = test_dir/log\n"
         << "user_function_dir = test_dir/user_function_dir\n"
         << "enable_system_metrics = false\n"
         << "enable_metric_calculator = false\n"
         << "mem_limit = 5G\n";
    conf.close();
    RETURN_IF(!starrocks::config::init("test.conf", false), false);
    std::filesystem::remove_all("test_dir");
    RETURN_IF(!std::filesystem::create_directories(config::local_library_dir), false);
    RETURN_IF(!std::filesystem::create_directories(config::storage_root_path), false);
    RETURN_IF(!std::filesystem::create_directories(config::plugin_path), false);
    RETURN_IF(!std::filesystem::create_directories(config::query_scratch_dirs), false);
    RETURN_IF(!std::filesystem::create_directories(config::pprof_profile_dir), false);
    RETURN_IF(!std::filesystem::create_directories(config::small_file_dir), false);
    RETURN_IF(!std::filesystem::create_directories(config::pull_load_task_dir), false);
    RETURN_IF(!std::filesystem::create_directories(config::sys_log_dir), false);
    RETURN_IF(!std::filesystem::create_directories(config::user_function_dir), false);
    return true;
}

} // namespace starrocks
