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

#include <cstdlib>
#include <iostream>
#include <string>

#include "common/config_exec_flow_fwd.h"
#include "common/config_thrift_server_fwd.h"
#include "common/configbase.h"
#include "common/system/cpu_info.h"
#include "common/system/disk_info.h"
#include "common/system/mem_info.h"
#include "runtime/env/global_env.h"

namespace {

bool init_config() {
#ifdef USE_STAROS
    const char* starrocks_home = std::getenv("STARROCKS_HOME");
    if (starrocks_home != nullptr) {
        const std::string conffile = std::string(starrocks_home) + "/conf/be_test.conf";
        if (!starrocks::config::init(conffile.c_str())) {
            std::cerr << "failed to initialize config from " << conffile << std::endl;
            return false;
        }
        return true;
    }
#endif

    if (!starrocks::config::init(nullptr)) {
        std::cerr << "failed to initialize config defaults" << std::endl;
        return false;
    }
    return true;
}

} // namespace

int main(int argc, char** argv) {
    if (!init_config()) {
        return 1;
    }

    starrocks::config::max_memory_sink_batch_count = 20;
    starrocks::config::thrift_max_message_size = 1073741824;
    starrocks::config::thrift_max_frame_size = 16384000;
    starrocks::config::thrift_max_recursion_depth = 64;

    starrocks::CpuInfo::init();
    starrocks::DiskInfo::init();
    starrocks::MemInfo::init();

    auto global_env_status = starrocks::GlobalEnv::GetInstance()->init(nullptr);
    if (!global_env_status.ok()) {
        std::cerr << "failed to initialize GlobalEnv: " << global_env_status << std::endl;
        return 1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    const int result = RUN_ALL_TESTS();
    starrocks::GlobalEnv::GetInstance()->stop();
    return result;
}
